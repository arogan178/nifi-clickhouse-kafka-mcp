import fnmatch
import functools
import getpass
import inspect
import json
import logging
import time
from importlib.metadata import PackageNotFoundError, version
from typing import Literal

import anyio
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response

from .auth import OIDCVerifier
from .config import Config
from .masking import mask_text

audit = logging.getLogger("dataplane_mcp.audit")
READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)
CALLS = Counter("dataplane_mcp_tool_calls_total", "Tool calls", ["tool", "env", "outcome"])
DURATION = Histogram("dataplane_mcp_tool_duration_seconds", "Tool call duration", ["tool"])

INSTRUCTIONS = """Read-only access to NiFi, ClickHouse and Kafka. Nothing here can change state.

- Every tool takes an environment; call environments to see them and what each one has.
- Every result is a JSON object. An empty list means nothing matched; a failure is always a tool error.
- List tools page with offset/limit and return next_offset (None on the last page). Filter before paging.
- Secrets and configured fields are masked everywhere. Never try to recover a masked value."""

BACKEND_INSTRUCTIONS = {
    "nifi": """NiFi: start with nifi_health for problems, nifi_search to find components by name/id/type/property, then
nifi_process_group or nifi_component. Parameters come from nifi_parameters/nifi_parameter, not nifi_component.
nifi_queue lists FlowFiles waiting in a connection; nifi_provenance shows what happened to them.""",
    "clickhouse": """ClickHouse: clickhouse_query runs one read statement on one node; results stop at a row/character cap and
report truncated. Find tables with clickhouse_tables or clickhouse_search_columns and check them with clickhouse_describe_table
before querying. Prefer aggregates and LIMIT. Run EXPLAIN ESTIMATE first on queries over big tables. Use params for values.""",
    "kafka": """Kafka: nothing joins a consumer group or commits offsets. kafka_sample reads one partition from an offset, a
timestamp or the end. kafka_search scans every partition for a key, substring or JSON field; if stopped is "time" or
"max_scan", repeat it with resume set to the returned resume. For long windows, search 1-2 hour slices newest first.""",
}


def package_version() -> str:
    try:
        return version("dataplane-mcp")
    except PackageNotFoundError:
        return "0.0.0"


class Toolbox:
    """Registers read-only tools with auditing, a result size cap and metrics."""

    def __init__(self, server: MCPServer, cfg: Config):
        self.server = server
        self.cfg = cfg
        self.registered: list[str] = []

    def user(self, ctx: Context | None) -> str:
        token = get_access_token()
        if token:
            return token.subject
        if self.cfg.trusted_user_header:
            request = ctx.request_context.request if ctx else None
            name = request.headers.get(self.cfg.trusted_user_header, "").strip() if request is not None else ""
            if not name:
                raise ToolError(f"missing {self.cfg.trusted_user_header} header")
            return name[:200]
        return f"local:{getpass.getuser()}"

    def enabled(self, name: str) -> bool:
        return not any(fnmatch.fnmatch(name, pattern) for pattern in self.cfg.disabled_tools)

    def tool(self, fn):
        if not self.enabled(fn.__name__):
            return fn
        limit = self.cfg.max_result_chars

        @functools.wraps(fn)
        async def wrapper(ctx: Context, **kwargs):
            user = self.user(ctx)
            started = time.monotonic()
            outcome, size = "ok", None
            try:
                result = await anyio.to_thread.run_sync(functools.partial(fn, user, **kwargs))
                size = len(json.dumps(result, default=str))
                if size > limit:
                    raise ValueError(
                        f"result is {size} characters, over the {limit} limit; "
                        "use offset/limit, a smaller limit or a narrower filter"
                    )
            except Exception as e:
                outcome = "error"
                # Backend errors can quote the request or the data, so they are masked too.
                message = mask_text(str(e), self.cfg.masked_fields)
                audit_error = message[:500]
                raise ToolError(message) from e
            else:
                audit_error = None
            finally:
                elapsed = time.monotonic() - started
                CALLS.labels(fn.__name__, str(kwargs.get("env", "")), outcome).inc()
                DURATION.labels(fn.__name__).observe(elapsed)
                record = {
                    "user": user,
                    "tool": fn.__name__,
                    "args": kwargs,
                    "outcome": outcome,
                    "ms": round(elapsed * 1000),
                }
                if size is not None:
                    record["chars"] = size
                if audit_error:
                    record["error"] = audit_error
                audit.info(json.dumps(record, default=str))
            return result

        # The caller's name comes from the token or proxy header, so it must not appear in the input schema;
        # ctx is injected by the SDK and left out of the schema too.
        signature = inspect.signature(fn)
        ctx_param = inspect.Parameter("ctx", inspect.Parameter.KEYWORD_ONLY, annotation=Context)
        wrapper.__signature__ = signature.replace(parameters=list(signature.parameters.values())[1:] + [ctx_param])
        wrapper.__annotations__ = {**{k: v for k, v in fn.__annotations__.items() if k != "user"}, "ctx": Context}
        del wrapper.__wrapped__
        self.registered.append(fn.__name__)
        # Claude Code otherwise cuts tool output at 25k tokens and spills results over 50k chars to a file.
        return self.server.tool(annotations=READ_ONLY, meta={"anthropic/maxResultSizeChars": limit})(wrapper)


def env_type(names: list[str]):
    return Literal[tuple(names)]


def build_server(cfg: Config) -> MCPServer:
    from .tools import clickhouse, kafka, nifi

    backends = {
        name: [env for env, e in cfg.environments.items() if getattr(e, name) is not None]
        for name in ("nifi", "clickhouse", "kafka")
    }
    instructions = "\n\n".join([INSTRUCTIONS] + [BACKEND_INSTRUCTIONS[b] for b, envs in backends.items() if envs])
    kwargs = {}
    if cfg.auth:
        resource = f"{cfg.public_url.rstrip('/')}/mcp"
        kwargs["token_verifier"] = OIDCVerifier(cfg.auth, resource)
        kwargs["auth"] = AuthSettings(
            issuer_url=cfg.auth.issuer, resource_server_url=resource, validate_token_resource=False
        )
    server = MCPServer(
        name="dataplane-mcp",
        version=package_version(),
        instructions=instructions,
        website_url="https://github.com/arogan178/dataplane-mcp",
        **kwargs,
    )
    box = Toolbox(server, cfg)

    @box.tool
    def environments(user: str) -> dict:
        """The environments this server can read, with their description and which systems each one has."""
        return {
            "environments": [
                {
                    "name": name,
                    "description": e.description,
                    "systems": [b for b in ("nifi", "clickhouse", "kafka") if getattr(e, b) is not None],
                }
                for name, e in cfg.environments.items()
            ]
        }

    if backends["nifi"]:
        nifi.register(box, {n: cfg.environments[n].nifi for n in backends["nifi"]})
    if backends["clickhouse"]:
        clickhouse.register(box, {n: cfg.environments[n].clickhouse for n in backends["clickhouse"]})
    if backends["kafka"]:
        kafka.register(box, {n: cfg.environments[n].kafka for n in backends["kafka"]})

    @server.custom_route("/healthz", methods=["GET"])
    async def healthz(_: Request) -> PlainTextResponse:
        return PlainTextResponse("ok")

    @server.custom_route("/metrics", methods=["GET"])
    async def metrics(_: Request) -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    server.toolbox = box
    return server
