import base64
import errno
import os
import re
import ssl
import threading
import time
from datetime import UTC, datetime

import requests
from requests.adapters import HTTPAdapter

from .config import NiFiConfig
from .masking import MASK, SECRET_KEY, decode_text, mask, mask_document, mask_text


class _TLSAdapter(HTTPAdapter):
    def __init__(self, context: ssl.SSLContext):
        self.context = context
        super().__init__()

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self.context
        return super().init_poolmanager(*args, **kwargs)


CACHE_SECONDS = 60
POLL_SECONDS = 0.5
POLL_TIMEOUT_SECONDS = 60
MAX_CONTENT_BYTES = 5 * 1024 * 1024
_ID = "[0-9a-f-]+"
# Besides logging in, the only non-GET calls: creating and deleting temporary queue listings and provenance queries.
ALLOWED_REQUESTS = [
    ("POST", re.compile(f"/flowfile-queues/{_ID}/listing-requests")),
    ("DELETE", re.compile(f"/flowfile-queues/{_ID}/listing-requests/{_ID}")),
    ("POST", re.compile("/provenance")),
    ("DELETE", re.compile(f"/provenance/{_ID}")),
]


# Ids end up in paths. Every segment must be letters, digits, "-" or "_", so no "..", "..;", encoding, query
# string or extra segment can reach another endpoint.
SAFE_SEGMENT = re.compile(r"[A-Za-z0-9_-]+")


def check_path(path: str) -> None:
    if not path.startswith("/") or not all(SAFE_SEGMENT.fullmatch(seg) for seg in path[1:].split("/")):
        raise ValueError(f"invalid id in NiFi path {path!r}")


def check_request(method: str, path: str) -> None:
    if not any(method == m and pattern.fullmatch(path) for m, pattern in ALLOWED_REQUESTS):
        raise ValueError(f"NiFi {method} {path} is not allowed")


class NiFi:
    """Read-only NiFi REST client; give its identity view-only policies in NiFi as the second lock.

    Besides GET, request() allows only logging in and POST/DELETE of queue listing requests and
    provenance queries. NiFi needs those to read queues and provenance; nothing else is allowed.
    """

    def __init__(self, cfg: NiFiConfig):
        self.base_url = cfg.url.rstrip("/")
        if not self.base_url.endswith("/nifi-api"):
            self.base_url += "/nifi-api"
        self.cfg = cfg
        self.timeout = cfg.timeout_seconds
        for path in filter(None, (cfg.ca_bundle, getattr(cfg.auth, "cert", None), getattr(cfg.auth, "key", None))):
            if not os.path.isfile(path):
                raise FileNotFoundError(errno.ENOENT, "NiFi certificate file not found", path)
        self.session = requests.Session()
        if not cfg.verify_tls:
            self.session.verify = False
        elif cfg.ca_bundle:
            self.session.verify = cfg.ca_bundle
        if self.base_url.startswith("https://") and (cfg.verify_tls or cfg.auth.type == "mtls"):
            context = ssl.create_default_context(cafile=cfg.ca_bundle)
            # The NiFi TLS toolkit CA does not mark basicConstraints critical, which Python's strict mode rejects.
            context.verify_flags &= ~ssl.VERIFY_X509_STRICT
            if not cfg.verify_tls:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            if cfg.auth.type == "mtls":
                context.load_cert_chain(cfg.auth.cert, cfg.auth.key)
            self.session.mount("https://", _TLSAdapter(context))
        if cfg.auth.type == "token":
            self.session.headers["Authorization"] = f"Bearer {cfg.auth.token}"
        self._login_lock = threading.Lock()
        self._cache: dict = {}

    def _login(self, stale: str | None = None) -> None:
        with self._login_lock:
            if self.session.headers.get("Authorization") != stale:
                return  # another thread already logged in again
            response = self.session.post(
                f"{self.base_url}/access/token",
                data={"username": self.cfg.auth.username, "password": self.cfg.auth.password},
                timeout=self.timeout,
            )
            if not response.ok:
                raise RuntimeError(f"NiFi login failed with {response.status_code}: {response.text[:300]}")
            self.session.headers["Authorization"] = f"Bearer {response.text.strip()}"

    def _send(self, method: str, path: str, **kwargs) -> requests.Response:
        check_path(path)
        kwargs.setdefault("timeout", self.timeout)
        password_auth = self.cfg.auth.type == "password"
        if password_auth and "Authorization" not in self.session.headers:
            self._login()
        used = self.session.headers.get("Authorization")
        response = self.session.request(method, f"{self.base_url}{path}", **kwargs)
        if response.status_code == 401 and password_auth:
            response.close()
            self._login(stale=used)
            response = self.session.request(method, f"{self.base_url}{path}", **kwargs)
        if not response.ok:
            message = f"NiFi {response.status_code}: {response.text[:1000]}"
            response.close()
            raise RuntimeError(message)
        return response

    def get(self, path: str, params: dict | None = None, timeout: float | None = None) -> dict:
        return self._send("GET", path, params=params, timeout=timeout or self.timeout).json()

    def get_bytes(self, path: str, params: dict | None, max_bytes: int) -> tuple[bytes, bool]:
        """Up to max_bytes of a raw response, and whether that is all of it."""
        data = b""
        with self._send("GET", path, params=params, stream=True) as response:
            for chunk in response.iter_content(65536):
                data += chunk
                if len(data) >= max_bytes:
                    return data[:max_bytes], False
        return data, True

    def request(self, method: str, path: str, body: dict | None = None) -> dict:
        check_request(method, path)
        response = self._send(method, path, json=body)
        return response.json() if response.content else {}

    def temporary_request(self, path: str, key: str, body: dict | None = None) -> dict:
        """Create a listing request or provenance query at path, wait until it finishes, then delete it."""
        request_id = self.request("POST", path, body)[key]["id"]
        try:
            deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
            while True:
                result = self.get(f"{path}/{request_id}")[key]
                if result.get("failureReason"):
                    raise RuntimeError(f"NiFi {key} failed: {result['failureReason']}")
                if result.get("finished"):
                    return result
                if time.monotonic() > deadline:
                    raise TimeoutError(f"NiFi {key} did not finish within {POLL_TIMEOUT_SECONDS} seconds")
                time.sleep(POLL_SECONDS)
        finally:
            self.request("DELETE", f"{path}/{request_id}")

    def cached(self, path: str, params: dict | None = None) -> dict:
        """A slow, large GET kept for CACHE_SECONDS, so paging through it doesn't refetch it."""
        key = (path, tuple(sorted((params or {}).items())))
        fetched, value = self._cache.get(key, (0.0, None))
        if value is None or time.monotonic() - fetched > CACHE_SECONDS:
            value = self.get(path, params, timeout=max(self.timeout, 120))
            self._cache[key] = (time.monotonic(), value)
        return value

    def parameter_contexts(self) -> dict:
        # The full listing can be tens of MB.
        return self.cached("/flow/parameter-contexts")

    def status(self, process_group_id: str) -> dict:
        return self.cached(f"/flow/process-groups/{process_group_id}/status", {"recursive": "true"})


def strip_descriptors(component: dict) -> dict:
    """Drop property descriptors (most of the payload) and mask secret-looking properties and values."""
    component = {k: v for k, v in component.items() if k != "descriptors"}
    if "config" in component:
        component["config"] = {k: v for k, v in component["config"].items() if k != "descriptors"}
    return mask(component, frozenset())


def flatten_search_results(results: dict, query: str, masked_fields: frozenset[str]) -> list[dict]:
    """Search hits with their matches masked; a hit whose matches only matched inside masked text is dropped,
    so searching can't be used to confirm a masked value."""
    items = []
    needle = query.lower()
    for group, hits in results.items():
        if not isinstance(hits, list):
            continue
        # processorResults -> processor, controllerServiceNodeResults -> controller-service
        kind = re.sub(r"(?<!^)(?=[A-Z])", "-", group.removesuffix("Results").removesuffix("Node")).lower()
        for r in hits:
            secret_name = SECRET_KEY.search(r.get("name") or "")
            matches = [
                f"{m.split(':', 1)[0]}: {MASK}"
                if secret_name and m.lower().startswith("value")
                else mask_text(m, masked_fields)
                for m in r.get("matches", [])
            ]
            matches = [m for m in matches if needle in m.lower()]
            if not matches:
                continue
            items.append(
                {
                    "kind": kind,
                    "id": r["id"],
                    "name": r.get("name"),
                    "process_group_id": r.get("groupId"),
                    "process_group": (r.get("parentGroup") or {}).get("name"),
                    "matches": [m[:1000] for m in matches[:5]],
                }
            )
    return items


def parameter_value(parameter: dict) -> str | None:
    value = parameter.get("value")
    if value is None:
        return None
    return "***" if SECRET_KEY.search(parameter["name"]) else mask_text(value)


def flatten_parameters(
    contexts: dict,
    context: str | None = None,
    name_contains: str | None = None,
    value_contains: str | None = None,
    max_value_chars: int = 2000,
) -> tuple[list[dict], int]:
    """Parameters of all readable contexts, sorted by context and name, plus the count of unreadable contexts."""
    items, unreadable = [], 0
    for entry in contexts["parameterContexts"]:
        if "component" not in entry:
            unreadable += 1
            continue
        c = entry["component"]
        if context is not None and context not in (c["name"], c["id"]):
            continue
        for p in (e["parameter"] for e in c.get("parameters", [])):
            value = parameter_value(p)
            if name_contains is not None and name_contains not in p["name"]:
                continue
            if value_contains is not None and (value is None or value_contains not in value):
                continue
            items.append(
                {
                    "context": c["name"],
                    "context_id": c["id"],
                    "name": p["name"],
                    "sensitive": p.get("sensitive", False),
                    "provided": p.get("provided", False),
                    "inherited": p.get("inherited", False),
                    "value": value[:max_value_chars] if value is not None else None,
                    "value_length": len(value) if value is not None else 0,
                }
            )
    return sorted(items, key=lambda i: (i["context"], i["name"])), unreadable


def parameter_slice(contexts: dict, context: str, name: str, offset: int, length: int) -> dict:
    offset, length = max(0, offset), max(1, length)
    for c in (e["component"] for e in contexts["parameterContexts"] if "component" in e):
        if context not in (c["name"], c["id"]):
            continue
        for p in (e["parameter"] for e in c.get("parameters", [])):
            if p["name"] != name:
                continue
            value = parameter_value(p) or ""
            end = offset + length
            return {
                "context": c["name"],
                "context_id": c["id"],
                "name": name,
                "sensitive": p.get("sensitive", False),
                "value_length": len(value),
                "offset": offset,
                "value": value[offset:end],
                "next_offset": end if end < len(value) else None,
            }
    raise ValueError(f"no readable parameter {name!r} in context {context!r}; use nifi_parameters to find it")


def summarize_process_group(flow: dict) -> dict:
    pg = flow["processGroupFlow"]
    contents = pg["flow"]
    return {
        "id": pg["id"],
        "name": pg["breadcrumb"]["breadcrumb"]["name"],
        "parent_id": pg.get("parentGroupId"),
        "process_groups": [
            {
                "id": g["id"],
                "name": g["component"]["name"],
                "running": g.get("runningCount"),
                "stopped": g.get("stoppedCount"),
                "invalid": g.get("invalidCount"),
                "disabled": g.get("disabledCount"),
                "queued": g["status"]["aggregateSnapshot"]["queued"],
            }
            for g in contents["processGroups"]
        ],
        "processors": [
            {
                "id": p["id"],
                "name": p["component"]["name"],
                "type": p["component"]["type"].rsplit(".", 1)[-1],
                "state": p["component"]["state"],
                "validation_errors": p["component"].get("validationErrors", []),
            }
            for p in contents["processors"]
        ],
        "connections": [
            {
                "id": c["id"],
                "source": c["component"]["source"]["name"],
                "destination": c["component"]["destination"]["name"],
                "queued": c["status"]["aggregateSnapshot"]["queued"],
            }
            for c in contents["connections"]
        ],
        "input_ports": [{"id": p["id"], "name": p["component"]["name"]} for p in contents["inputPorts"]],
        "output_ports": [{"id": p["id"], "name": p["component"]["name"]} for p in contents["outputPorts"]],
    }


def nifi_date(millis: int) -> str:
    return datetime.fromtimestamp(millis / 1000, UTC).strftime("%m/%d/%Y %H:%M:%S UTC")


def looks_like_json(raw: bytes) -> bool:
    return raw.lstrip()[:1] in (b"{", b"[")


def binary(piece: bytes, total: int, show: bool) -> dict:
    if show:
        return {"base64": base64.b64encode(piece).decode(), "bytes": len(piece)}
    return {"binary": True, "bytes": total, "note": "binary data can't be masked, so it is not shown"}


def content_slice(
    raw: bytes, complete: bool, offset: int, length: int, masked_fields: frozenset[str], show_binary: bool = False
) -> dict:
    """content[offset:offset+length] of FlowFile content: characters of text, JSON masked and re-serialized, else bytes.

    raw may be only the first part of the content (complete False).
    """
    end = offset + length
    text = decode_text(raw, complete)
    if text is None:
        if not show_binary:
            return {"content": binary(b"", len(raw), False), "next_offset": None}
        return {
            "content": binary(raw[offset:end], len(raw), True),
            "next_offset": end if end < len(raw) or not complete else None,
        }
    if looks_like_json(raw) and not complete:
        raise ValueError(f"JSON content over {MAX_CONTENT_BYTES} bytes cannot be masked, so it is not shown")
    text = mask_document(text, masked_fields)
    return {"content": text[offset:end], "next_offset": end if end < len(text) or not complete else None}


def summarize_listing(listing: dict, limit: int) -> dict:
    summaries = listing.get("flowFileSummaries") or []
    return {
        "queue_size": {
            "flowfiles": listing["queueSize"]["objectCount"],
            "bytes": listing["queueSize"]["byteCount"],
        },
        "listed": len(summaries),
        "items": [
            {
                "uuid": s["uuid"],
                "filename": s.get("filename"),
                "size": s.get("size"),
                "position": s.get("position"),
                "queued_duration_ms": s.get("queuedDuration"),
                "lineage_duration_ms": s.get("lineageDuration"),
                "penalized": s.get("penalized"),
                "mime_type": s.get("mimeType"),
                "cluster_node_id": s.get("clusterNodeId"),
                "cluster_node_address": s.get("clusterNodeAddress"),
            }
            for s in summaries[:limit]
        ],
    }


def summarize_flowfile(flowfile: dict, masked_fields: frozenset[str]) -> dict:
    return {
        "uuid": flowfile.get("uuid"),
        "filename": flowfile.get("filename"),
        "size": flowfile.get("size"),
        "mime_type": flowfile.get("mimeType"),
        "queued_duration_ms": flowfile.get("queuedDuration"),
        "lineage_duration_ms": flowfile.get("lineageDuration"),
        "penalized": flowfile.get("penalized"),
        "cluster_node_id": flowfile.get("clusterNodeId"),
        "attributes": mask(flowfile.get("attributes") or {}, masked_fields),
    }


def summarize_event(event: dict) -> dict:
    return {
        "event_id": event["eventId"],
        "event_time": event.get("eventTime"),
        "event_type": event.get("eventType"),
        "component_id": event.get("componentId"),
        "component_name": event.get("componentName"),
        "component_type": event.get("componentType"),
        "flowfile_uuid": event.get("flowFileUuid"),
        "file_size": event.get("fileSize"),
        "details": mask_text(event["details"]) if event.get("details") else None,
        "cluster_node_id": event.get("clusterNodeId"),
        "parent_uuids": event.get("parentUuids", []),
        "child_uuids": event.get("childUuids", []),
    }


def event_details(event: dict, masked_fields: frozenset[str]) -> dict:
    attributes = event.get("attributes") or []
    return {
        **summarize_event(event),
        "attributes": mask({a["name"]: a.get("value") for a in attributes}, masked_fields),
        "previous_attributes": mask(
            {a["name"]: a.get("previousValue") for a in attributes if a.get("previousValue") != a.get("value")},
            masked_fields,
        ),
        "input_content_available": event.get("inputContentAvailable", False),
        "output_content_available": event.get("outputContentAvailable", False),
    }


def walk_status(snapshot: dict, path: str = ""):
    """Yield (group snapshot, path) for a recursive process group status snapshot and all its descendants."""
    path = f"{path}/{snapshot.get('name', '')}" if path else snapshot.get("name", "")
    yield snapshot, path
    for child in snapshot.get("processGroupStatusSnapshots") or []:
        yield from walk_status(child["processGroupStatusSnapshot"], path)


def health_report(status: dict, bulletins: list[dict], services: list[dict], threshold: int, limit: int) -> dict:
    """Problems in a flow, worst first: back pressure, invalid and stopped processors, services, error bulletins."""
    pressured, invalid, stopped, disabled = [], [], [], []
    for group, path in walk_status(status["processGroupStatus"]["aggregateSnapshot"]):
        for c in (e["connectionStatusSnapshot"] for e in group.get("connectionStatusSnapshots") or []):
            use = max(c.get("percentUseCount") or 0, c.get("percentUseBytes") or 0)
            if use >= threshold:
                pressured.append(
                    {
                        "id": c["id"],
                        "name": c.get("name") or f"{c.get('sourceName')} -> {c.get('destinationName')}",
                        "process_group": path,
                        "percent_full": use,
                        "queued": c.get("queued"),
                    }
                )
        for p in (e["processorStatusSnapshot"] for e in group.get("processorStatusSnapshots") or []):
            item = {"id": p["id"], "name": p.get("name"), "type": p.get("type"), "process_group": path}
            state = p.get("runStatus")
            if state == "Invalid":
                invalid.append(item)
            elif state == "Stopped":
                stopped.append(item)
            elif state == "Disabled":
                disabled.append(item)
    pressured.sort(key=lambda c: -c["percent_full"])
    bad_services = [
        {
            "id": s["id"],
            "name": s["component"]["name"],
            "state": s["component"]["state"],
            "validation": s["component"].get("validationStatus"),
        }
        for s in services
        if s["component"]["state"] != "ENABLED" or s["component"].get("validationStatus") == "INVALID"
    ]
    errors = [b for b in bulletins if b.get("level") == "ERROR"]

    def capped(items: list) -> dict:
        return {"total": len(items), "items": items[:limit]}

    return {
        "back_pressure": capped(pressured),
        "invalid_processors": capped(invalid),
        "controller_services_not_enabled_or_invalid": capped(bad_services),
        "error_bulletins": capped(errors),
        "stopped_processors": capped(stopped),
        "disabled_processors": {"total": len(disabled)},
    }


def flow_tree(snapshot: dict, depth: int) -> dict:
    """Process group names, ids, queued data and processor counts by state, down to depth levels."""
    states: dict[str, int] = {}
    for p in (e["processorStatusSnapshot"] for e in snapshot.get("processorStatusSnapshots") or []):
        states[p.get("runStatus", "Unknown")] = states.get(p.get("runStatus", "Unknown"), 0) + 1
    children = [e["processGroupStatusSnapshot"] for e in snapshot.get("processGroupStatusSnapshots") or []]
    node = {
        "id": snapshot["id"],
        "name": snapshot.get("name"),
        "queued": snapshot.get("queued"),
        "processors": states,
    }
    if depth > 0:
        node["groups"] = [flow_tree(c, depth - 1) for c in children]
    elif children:
        node["child_groups"] = len(children)
    return node


def component_state(state: dict, masked_fields: frozenset[str]) -> dict:
    """Local and cluster state of a processor as key/value maps, masked."""
    result = {}
    for scope in ("localState", "clusterState"):
        entries = (state.get(scope) or {}).get("state") or []
        values: dict = {}
        for e in entries:
            # Local state is per node on a cluster, with the same keys on every node.
            target = values.setdefault(e["clusterNodeAddress"], {}) if e.get("clusterNodeAddress") else values
            target[e["key"]] = e.get("value")
        if entries:
            result[scope.removesuffix("State")] = mask(values, masked_fields)
    return result
