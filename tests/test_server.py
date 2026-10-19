import json
import re
from pathlib import Path

import anyio
import pytest

from dataplane_mcp import cli, config
from dataplane_mcp.server import build_server

SRC = Path(__file__).parent.parent / "src" / "dataplane_mcp"


def cfg(**overrides):
    raw = {
        "masked_fields": ["Email"],
        "environments": {
            "prod": {
                "nifi": {"url": "https://nifi.example:8443", "auth": {"type": "none"}},
                "clickhouse": {"url": "https://ch.example:8443", "user": "reader", "password": "x"},
                "kafka": {"config": {"bootstrap.servers": "kafka.example:9092"}},
            },
            "dev": {"clickhouse": {"url": "http://localhost:8123"}},
        },
    }
    raw.update(overrides)
    return config.Config.model_validate(raw)


def tools(c):
    return {t.name: t for t in anyio.run(build_server(c).list_tools)}


def test_every_tool_is_read_only_and_never_asks_for_the_user():
    listed = tools(cfg())
    assert len(listed) > 20
    for tool in listed.values():
        assert tool.annotations.read_only_hint is True
        assert tool.annotations.destructive_hint is False
        assert "user" not in tool.input_schema.get("properties", {})


def test_environment_choices_follow_configured_backends():
    listed = tools(cfg())
    assert listed["clickhouse_query"].input_schema["properties"]["env"]["enum"] == ["prod", "dev"]
    only = listed["kafka_topics"].input_schema["properties"]["env"]
    assert only.get("enum", [only.get("const")]) == ["prod"]


def test_only_configured_backends_register_tools():
    names = set(tools(cfg(environments={"dev": {"clickhouse": {"url": "http://localhost:8123"}}})))
    assert {"environments", "clickhouse_query"} <= names
    assert not {n for n in names if n.startswith(("nifi_", "kafka_"))}


def test_disabled_tools_take_patterns():
    names = set(tools(cfg(disabled_tools=["kafka_search", "nifi_flow*"])))
    assert "kafka_search" not in names and "nifi_flowfile" not in names and "nifi_flow_tree" not in names
    assert "kafka_sample" in names


def test_array_and_object_schemas_are_complete():
    # Some clients reject array schemas without items.
    for tool in tools(cfg()).values():
        for prop in json.dumps(tool.input_schema) and _walk(tool.input_schema):
            if prop.get("type") == "array":
                assert "items" in prop, tool.name


def _walk(schema):
    if isinstance(schema, dict):
        yield schema
        for v in schema.values():
            yield from _walk(v)
    elif isinstance(schema, list):
        for v in schema:
            yield from _walk(v)


def test_config_expands_environment_variables(tmp_path, monkeypatch):
    monkeypatch.setenv("CH_PASSWORD", "s3cret")
    path = tmp_path / "c.yaml"
    path.write_text(
        "environments:\n  dev:\n    clickhouse:\n      url: ${CH_URL:-http://localhost:8123}\n      password: ${CH_PASSWORD}\n"
    )
    loaded = config.load(path)
    assert loaded.environments["dev"].clickhouse.password == "s3cret"
    assert loaded.environments["dev"].clickhouse.url == "http://localhost:8123"


def test_config_rejects_missing_variables_and_commit_settings(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text(
        "environments:\n  dev:\n    clickhouse:\n      url: http://x\n      password: ${NOT_SET_ANYWHERE}\n"
    )
    with pytest.raises(ValueError, match="NOT_SET_ANYWHERE"):
        config.load(path)
    with pytest.raises(ValueError, match="never commits"):
        config.KafkaConfig.model_validate({"config": {"bootstrap.servers": "k:9092", "group.id": "g"}})


def test_refuses_unauthenticated_http_off_loopback(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text("environments:\n  dev:\n    clickhouse:\n      url: http://x\n")
    with pytest.raises(SystemExit, match="refusing"):
        cli.main(["--config", str(path), "--transport", "http", "--host", "0.0.0.0"])


def test_source_has_no_write_paths():
    kafka = (SRC / "kafka.py").read_text()
    assert not re.search(r"\bProducer\b|\.commit\(|\.subscribe\(|\.produce\(|create_topics|delete_|alter_", kafka)
    nifi = (SRC / "nifi.py").read_text()
    # Every non-GET NiFi call goes through request(), which checks the allowlist; login is the one exception.
    assert re.findall(r"\.session\.(post|put|delete|patch)\(", nifi) == ["post"]
    assert set(re.findall(r'_send\("(\w+)"', nifi)) == {"GET"}
    assert "/access/token" in nifi


def test_trusted_user_header_and_auth_are_exclusive():
    with pytest.raises(ValueError, match="not both"):
        cfg(public_url="http://x", trusted_user_header="X-User", auth={"issuer": "http://i", "audience": "a"})


def test_user_comes_from_the_trusted_header():
    from types import SimpleNamespace

    from mcp.server.mcpserver.exceptions import ToolError

    box = build_server(cfg(trusted_user_header="X-Auth-Request-Email")).toolbox

    def ctx(headers):
        return SimpleNamespace(request_context=SimpleNamespace(request=SimpleNamespace(headers=headers)))

    assert box.user(ctx({"X-Auth-Request-Email": "alice@example.com"})) == "alice@example.com"
    with pytest.raises(ToolError, match="missing X-Auth-Request-Email"):
        box.user(ctx({}))
    assert build_server(cfg()).toolbox.user(None).startswith("local:")


def test_proxy_guard_rejects_mcp_requests_without_the_header():
    sent = []

    async def app(scope, receive, send):
        sent.append("app")

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request"}

    guard = cli.RequireHeader(app, "X-Auth-Request-Email")

    async def call(path, headers):
        await guard({"type": "http", "path": path, "headers": headers, "method": "POST"}, receive, send)

    anyio.run(call, "/mcp", [])
    assert sent[0]["status"] == 401
    sent.clear()
    anyio.run(call, "/mcp", [(b"x-auth-request-email", b"alice@example.com")])
    assert sent == ["app"]
    sent.clear()
    anyio.run(call, "/healthz", [])
    assert sent == ["app"]


def test_metrics_are_opt_in():
    def routes(c):
        return {r.path for r in build_server(c)._custom_starlette_routes}

    assert "/metrics" not in routes(cfg())
    assert "/metrics" in routes(cfg(metrics=True))
    assert "/healthz" in routes(cfg())
