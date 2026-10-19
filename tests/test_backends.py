import io
import json

import fastavro

from dataplane_mcp.clickhouse import ClickHouse
from dataplane_mcp.config import ClickHouseConfig, SchemaRegistryConfig
from dataplane_mcp.kafka import Decoder, message_matches
from dataplane_mcp.nifi import component_state, flow_tree, health_report

SCHEMA = {
    "type": "record",
    "name": "Bet",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "email", "type": "string"},
        {"name": "raw", "type": "bytes"},
    ],
}


class FakeResponse:
    def __init__(self, body=None, lines=None, headers=None, ok=True, text=""):
        self.body, self.lines, self.headers, self.ok, self.text = body, lines or [], headers or {}, ok, text

    def json(self):
        return self.body

    def iter_lines(self, decode_unicode=True):
        return iter(self.lines)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class FakeSession:
    def __init__(self, response, readonly="2"):
        self.response, self.calls, self.readonly = response, [], readonly

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self.response

    def post(self, url, **kwargs):
        data = kwargs.get("data", b"")
        if data.startswith(b"EXPLAIN AST"):
            return FakeResponse(text="SelectWithUnionQuery (children 1)")
        if data.startswith(b"SELECT lower(name) FROM system.table_functions"):
            return FakeResponse(text="url\nnumbers\n")
        if data.startswith(b"SELECT getSetting('readonly')"):
            return FakeResponse(text=self.readonly)
        self.calls.append(("POST", url, kwargs))
        return self.response


def avro_message(record: dict, schema_id: int = 7) -> bytes:
    out = io.BytesIO()
    fastavro.schemaless_writer(out, fastavro.parse_schema(SCHEMA), record)
    return b"\x00" + schema_id.to_bytes(4, "big") + out.getvalue()


def registry_decoder():
    decoder = Decoder(frozenset({"email"}), SchemaRegistryConfig(url="http://registry"))
    decoder.session = FakeSession(FakeResponse({"schema": json.dumps(SCHEMA)}))
    return decoder


def test_decodes_avro_through_the_registry_and_masks():
    decoder = registry_decoder()
    raw = avro_message({"id": 42, "email": "a@b.c", "raw": b"\x01\x02"})
    assert decoder(raw) == {"id": 42, "email": "***", "raw": "AQI="}
    decoder(raw)
    assert len(decoder.session.calls) == 1, "schemas are cached by id"


def test_searches_decoded_avro_without_revealing_masked_fields():
    decoder = registry_decoder()
    raw = avro_message({"id": 42, "email": "a@b.c", "raw": b""})
    assert message_matches(None, raw, decoder, field="id", value="42")
    assert not message_matches(None, raw, decoder, contains="a@b.c")


def test_registry_failure_is_reported_not_raised():
    decoder = Decoder(frozenset(), SchemaRegistryConfig(url="http://registry"))
    decoder.session = FakeSession(FakeResponse(ok=False, text="not found"))
    result = decoder(b"\x00\x00\x00\x00\x09xyz")
    assert result["schema_id"] == 9 and "decode_error" in result


def clickhouse(lines, headers=None, masked=frozenset(), **cfg):
    client = ClickHouse(ClickHouseConfig(url="http://ch:8123", **cfg), masked)
    client.session = FakeSession(FakeResponse(lines=lines, headers=headers))
    return client


def test_clickhouse_streams_rows_and_reports_stats():
    client = clickhouse(
        ['["n","s"]', '["UInt64","String"]', '["1","a"]', '["2","b"]'],
        {"X-ClickHouse-Summary": '{"read_rows":"2","read_bytes":"16","elapsed_ns":"5"}'},
        settings={"max_execution_time": 30},
    )
    result = client.query("SELECT n, s FROM t WHERE n > {min:UInt64}", "ann", params={"min": 0})
    assert result["columns"] == [{"name": "n", "type": "UInt64"}, {"name": "s", "type": "String"}]
    assert result["rows"] == [["1", "a"], ["2", "b"]]
    assert result["truncated"] is None
    assert result["stats"] == {"read_rows": "2", "read_bytes": "16", "elapsed_ns": "5"}
    sent = client.session.calls[0][2]["params"]
    assert sent["param_min"] == "0" and sent["max_execution_time"] == "30"
    assert sent["log_comment"] == "dataplane-mcp user=ann"


def test_clickhouse_cuts_at_rows_and_characters():
    lines = ['["n"]', '["UInt64"]'] + [f'["{i}"]' for i in range(10)]
    big = clickhouse(['["n"]', '["UInt64"]', "[18446744073709551615]", "[7]"]).query("SELECT n FROM t", "u")
    assert big["rows"] == [["18446744073709551615"], [7]]
    by_rows = clickhouse(lines, max_rows=3).query("SELECT n FROM t", "u")
    assert len(by_rows["rows"]) == 3 and by_rows["truncated"] == "more than 3 rows"
    by_chars = clickhouse(lines).query("SELECT n FROM t", "u", max_chars=20)
    assert by_chars["truncated"] == "more than 20 characters" and len(by_chars["rows"]) < 10


def test_clickhouse_max_rows_cannot_exceed_the_server_setting():
    lines = ['["n"]', '["UInt64"]'] + [f'["{i}"]' for i in range(10)]
    assert len(clickhouse(lines, max_rows=2).query("SELECT n FROM t", "u", max_rows=1000)["rows"]) == 2


STATUS = {
    "processGroupStatus": {
        "aggregateSnapshot": {
            "id": "root",
            "name": "Root",
            "queued": "5 / 1 KB",
            "connectionStatusSnapshots": [
                {
                    "connectionStatusSnapshot": {
                        "id": "c1",
                        "name": "",
                        "sourceName": "A",
                        "destinationName": "B",
                        "percentUseCount": 95,
                        "percentUseBytes": 10,
                        "queued": "9,500",
                    }
                },
                {"connectionStatusSnapshot": {"id": "c2", "name": "ok", "percentUseCount": 5, "percentUseBytes": 1}},
            ],
            "processorStatusSnapshots": [
                {"processorStatusSnapshot": {"id": "p1", "name": "A", "type": "ConsumeKafka", "runStatus": "Running"}},
                {"processorStatusSnapshot": {"id": "p2", "name": "B", "type": "PutSQL", "runStatus": "Invalid"}},
            ],
            "processGroupStatusSnapshots": [
                {
                    "processGroupStatusSnapshot": {
                        "id": "g1",
                        "name": "Ingest",
                        "queued": "0",
                        "connectionStatusSnapshots": [
                            {
                                "connectionStatusSnapshot": {
                                    "id": "c3",
                                    "name": "full",
                                    "percentUseCount": 40,
                                    "percentUseBytes": 100,
                                    "queued": "1 GB",
                                }
                            },
                        ],
                        "processorStatusSnapshots": [
                            {
                                "processorStatusSnapshot": {
                                    "id": "p3",
                                    "name": "C",
                                    "type": "PutFile",
                                    "runStatus": "Stopped",
                                }
                            },
                        ],
                        "processGroupStatusSnapshots": [
                            {
                                "processGroupStatusSnapshot": {
                                    "id": "g2",
                                    "name": "Deep",
                                    "processorStatusSnapshots": [],
                                }
                            },
                        ],
                    }
                },
            ],
        }
    }
}


def test_health_report_lists_worst_first_with_group_paths():
    services = [
        {"id": "s1", "component": {"name": "pool", "state": "DISABLED"}},
        {"id": "s2", "component": {"name": "fine", "state": "ENABLED", "validationStatus": "VALID"}},
    ]
    bulletins = [{"level": "ERROR", "message": "boom"}, {"level": "WARNING", "message": "meh"}]
    report = health_report(STATUS, bulletins, services, threshold=80, limit=10)
    assert [c["id"] for c in report["back_pressure"]["items"]] == ["c3", "c1"]
    assert report["back_pressure"]["items"][0]["process_group"] == "Root/Ingest"
    assert report["back_pressure"]["items"][1]["name"] == "A -> B"
    assert [p["id"] for p in report["invalid_processors"]["items"]] == ["p2"]
    assert [p["id"] for p in report["stopped_processors"]["items"]] == ["p3"]
    assert [s["id"] for s in report["controller_services_not_enabled_or_invalid"]["items"]] == ["s1"]
    assert report["error_bulletins"]["total"] == 1


def test_health_report_caps_each_section():
    report = health_report(STATUS, [], [], threshold=0, limit=1)
    assert report["back_pressure"]["total"] == 3 and len(report["back_pressure"]["items"]) == 1


def test_flow_tree_stops_at_depth():
    snapshot = STATUS["processGroupStatus"]["aggregateSnapshot"]
    tree = flow_tree(snapshot, depth=1)
    assert tree["processors"] == {"Running": 1, "Invalid": 1}
    assert tree["groups"][0]["name"] == "Ingest"
    assert tree["groups"][0]["child_groups"] == 1 and "groups" not in tree["groups"][0]


def test_component_state_is_split_per_node_and_masked():
    state = {
        "localState": {
            "state": [
                {"key": "last.ts", "value": "1", "clusterNodeAddress": "n1:8443"},
                {"key": "last.ts", "value": "2", "clusterNodeAddress": "n2:8443"},
            ]
        },
        "clusterState": {"state": [{"key": "db.password", "value": "x"}, {"key": "max.id", "value": "9"}]},
    }
    assert component_state(state, frozenset()) == {
        "local": {"n1:8443": {"last.ts": "1"}, "n2:8443": {"last.ts": "2"}},
        "cluster": {"db.password": "***", "max.id": "9"},
    }


def test_clickhouse_masks_configured_column_names():
    lines = ['["id","Email"]', '["UInt64","String"]', '[1,"a@b.c"]']
    assert clickhouse(lines, masked=frozenset({"email"})).query("SELECT id, Email FROM t", "u")["rows"] == [[1, "***"]]


def test_clickhouse_leaves_out_log_comment_for_readonly_1_users():
    client = clickhouse(['["n"]', '["UInt8"]', "[1]"])
    client.session.readonly = "1"
    assert client.query("SELECT 1", "u")["rows"] == [[1]]
    assert "log_comment" not in client.session.calls[0][2]["params"]
    tagged = clickhouse(['["n"]', '["UInt8"]', "[1]"])
    tagged.query("SELECT 1", "u")
    assert tagged.session.calls[0][2]["params"]["log_comment"] == "dataplane-mcp user=u"
