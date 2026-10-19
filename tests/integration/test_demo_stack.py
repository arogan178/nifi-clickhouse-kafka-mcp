"""End to end against examples/demo: the real server over stdio, real NiFi 2, ClickHouse and Kafka.

Run with: docker compose -f examples/demo/compose.yaml up -d, seed (see examples/demo/README.md), then
DATAPLANE_MCP_DEMO=1 uv run pytest tests/integration
"""

import json
import os
import shutil
from pathlib import Path

import anyio
import pytest
import requests
from mcp import Client, StdioServerParameters

pytestmark = pytest.mark.skipif(os.environ.get("DATAPLANE_MCP_DEMO") != "1", reason="needs the demo stack")
CONFIG = Path(__file__).parents[2] / "examples" / "demo" / "config.yaml"


def call_all(calls: list[tuple[str, dict]]) -> list:
    async def run():
        server = StdioServerParameters(command=shutil.which("dataplane-mcp"), args=["--config", str(CONFIG)])
        results = []
        async with Client(server) as client:
            for name, args in calls:
                result = await client.call_tool(name, {"env": "demo", **args} if name != "environments" else args)
                text = result.content[0].text if result.content else ""
                results.append(("error", text) if result.is_error else json.loads(text))
        return results

    return anyio.run(run)


def call(name: str, **args):
    result = call_all([(name, args)])[0]
    if isinstance(result, tuple):
        raise AssertionError(result[1])
    return result


def error(name: str, **args) -> str:
    result = call_all([(name, args)])[0]
    assert isinstance(result, tuple), f"{name} should have failed: {result}"
    return result[1]


def test_environments():
    assert call("environments")["environments"][0]["systems"] == ["nifi", "clickhouse", "kafka"]


def test_nifi_health_finds_the_backed_up_queue_and_stopped_sink():
    assert call("nifi_status")["version"].startswith("2.")
    health = call("nifi_health")
    assert health["back_pressure"]["items"][0]["name"] == "orders"
    assert health["back_pressure"]["items"][0]["process_group"].endswith("/Ingest")
    assert "Load to warehouse" in [p["name"] for p in health["stopped_processors"]["items"]]
    tree = call("nifi_flow_tree")
    assert [g["name"] for g in tree["groups"]] == ["Ingest"]


def test_nifi_navigation_and_parameters_are_masked():
    found = call("nifi_search", query="warehouse")["items"]
    sink = next(i for i in found if i["kind"] == "processor")
    assert call("nifi_component", kind="processor", component_id=sink["id"])["name"] == "Load to warehouse"
    params = {p["name"]: p["value"] for p in call("nifi_parameters", context="ingest-params")["items"]}
    assert params["db.password"] == "***"
    assert "hunter2" not in params["db.url"] and "password=***" in params["db.url"]
    assert params["batch.size"] == "500"


def test_nifi_queue_flowfile_and_provenance_mask_content():
    connection = next(i for i in call("nifi_search", query="orders")["items"] if i["kind"] == "connection")
    queue = call("nifi_queue", connection_id=connection["id"], limit=5)
    assert queue["queue_size"]["flowfiles"] >= 100 and len(queue["items"]) == 5
    item = queue["items"][0]
    flowfile = call(
        "nifi_flowfile",
        connection_id=connection["id"],
        flowfile_uuid=item["uuid"],
        cluster_node_id=item["cluster_node_id"],
        include_content=True,
    )
    content = json.loads(flowfile["content"])
    assert content == {"id": 1, "customer_email": "***", "token": "***"}
    generator = next(i for i in call("nifi_search", query="Generate orders")["items"] if i["kind"] == "processor")
    events = call("nifi_provenance", component_id=generator["id"], start="-1h", limit=5)["events"]
    assert events
    event = call("nifi_provenance_event", event_id=events[0]["event_id"], content_side="output")
    assert "a@example.com" not in json.dumps(event)
    assert call("nifi_processor_state", processor_id=generator["id"])["processor_id"] == generator["id"]
    assert "services" in call("nifi_controller_services")
    assert "bulletins" in call("nifi_bulletins")


def test_clickhouse_reads_caps_and_refuses_writes():
    total = call("clickhouse_query", sql="SELECT count() AS n FROM shop.orders")
    assert total["rows"] == [[5000]] and total["stats"]["read_rows"]
    capped = call("clickhouse_query", sql="SELECT id FROM shop.orders")
    assert capped["row_count"] == 500 and capped["truncated"] == "more than 500 rows"
    bound = call("clickhouse_query", sql="SELECT id FROM shop.orders WHERE id = {id:UInt64}", params={"id": 7})
    assert bound["rows"] == [[7]]
    assert "statements are allowed" in error(
        "clickhouse_query", sql="INSERT INTO shop.orders VALUES (1, 'x', 1, now())"
    )
    assert "is not allowed" in error("clickhouse_query", sql="SELECT * FROM url('http://example.com', CSV)")
    # The second lock: even past the gateway, the user itself cannot write.
    direct = requests.post(
        "http://localhost:8123/", auth=("mcp", "mcp-password"), data="INSERT INTO shop.orders VALUES (1, 'x', 1, now())"
    )
    assert direct.status_code != 200 and ("ACCESS_DENIED" in direct.text or "readonly" in direct.text)


def test_clickhouse_schema_discovery_and_health():
    assert "shop" in [d["name"] for d in call("clickhouse_databases")["databases"]]
    tables = call("clickhouse_tables", database="shop")["items"]
    assert tables[0]["name"] == "orders" and tables[0]["comment"] == "Customer orders"
    described = call("clickhouse_describe_table", database="shop", table="orders")
    assert described["sorting_key"] == "id" and [c["name"] for c in described["columns"]][0] == "id"
    assert call("clickhouse_search_columns", contains="email")["items"][0]["name"] == "customer_email"
    health = call("clickhouse_health")
    assert health["server"]["version"] and isinstance(health["disks"], list)


def test_kafka_reads_without_joining_a_group():
    before = {g["group"] for g in call("kafka_consumer_groups")["items"]}
    assert call("kafka_cluster")["brokers"][0]["id"] == 1
    assert "orders" in [t["topic"] for t in call("kafka_topics")["items"]]
    assert sum(p["messages"] for p in call("kafka_describe_topic", topic="orders")["partitions"]) == 300
    lag = call("kafka_consumer_lag", group="billing")
    assert lag["total_lag"] == 180
    sample = call("kafka_sample", topic="orders", partition=0, start="earliest", limit=3)["messages"]
    assert sample[0]["value"]["customer"]["email"] == "***" and sample[0]["value"]["api_key"] == "***"
    gold = call("kafka_search", topic="orders", field="customer.tier", value="gold", limit=1000)
    assert len(gold["matches"]) == 150 and gold["stopped"] == "end"
    by_key = call("kafka_search", topic="orders", key="order-42")["matches"]
    assert [m["value"]["id"] for m in by_key] == [42]
    assert "user1@example.com" not in json.dumps(call("kafka_search", topic="orders", contains="user1@example.com"))
    first = call("kafka_search", topic="orders", contains="no such text", max_scan=100)
    assert first["stopped"] == "max_scan" and first["resume"]
    after = {g["group"] for g in call("kafka_consumer_groups")["items"]}
    assert after == before
    resumed = call("kafka_search", topic="orders", contains="no such text", max_scan=1000, resume=first["resume"])
    assert resumed["scanned"] == 200 and resumed["resume"] is None


def test_review_bypasses_stay_closed():
    contexts = call("nifi_parameters", context="ingest-params")["items"]
    context_id = contexts[0]["context_id"]
    assert "invalid id" in error(
        "nifi_component", kind="connection", component_id=f"../parameter-contexts/{context_id}"
    )
    assert "invalid id" in error("nifi_processor_state", processor_id="..%2Fparameter-contexts")
    assert call("nifi_search", query="hunter2")["items"] == []
    jdbc = [m for i in call("nifi_search", query="jdbc")["items"] for m in i["matches"]]
    assert jdbc and all("hunter2" not in m for m in jdbc)
    for sql in [
        "SELECT * FROM numbers(1) # '\n, url('http://example.com', CSV) -- '",
        "SELECT $$ ' $$, * FROM url('http://example.com', CSV) -- '",
        "SELECT * FROM `url`('http://example.com', CSV)",
        "SELECT * FROM \"file\"('/etc/passwd', 'LineAsString')",
        "SELECT * FROM icebergS3('http://example.com/x')",
        "DESCRIBE TABLE url('http://example.com', CSV)",
        "KILL QUERY WHERE 1",
    ]:
        message = error("clickhouse_query", sql=sql)
        assert "not allowed" in message or "only SELECT" in message, (sql, message)
    arrays = call("clickhouse_query", sql="SELECT has({ids:Array(String)}, 'b') AS hit", params={"ids": ["a", "b"]})
    assert arrays["rows"] == [[1]]
    path = call("clickhouse_query", sql="SELECT {p:String} AS p", params={"p": "C:\\new\\table"})
    assert path["rows"] == [["C:\\new\\table"]]
    assert call("clickhouse_query", sql="SELECT count() FROM numbers(10)")["rows"] == [[10]]
