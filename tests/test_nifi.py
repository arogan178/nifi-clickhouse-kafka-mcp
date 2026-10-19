import pytest

from dataplane_mcp.nifi import (
    check_path,
    check_request,
    content_slice,
    event_details,
    flatten_parameters,
    flatten_search_results,
    nifi_date,
    parameter_slice,
    strip_descriptors,
    summarize_event,
    summarize_flowfile,
    summarize_listing,
)

CONTEXTS = {
    "parameterContexts": [
        {"id": "c-hidden", "permissions": {"canRead": False}},
        {
            "id": "c2",
            "component": {
                "id": "c2",
                "name": "kafka",
                "parameters": [
                    {"parameter": {"name": "password", "sensitive": True, "value": "********", "provided": False}},
                    {"parameter": {"name": "bootstrap", "sensitive": False, "value": "broker:9092", "inherited": True}},
                ],
            },
        },
        {
            "id": "c1",
            "component": {
                "id": "c1",
                "name": "clickhouse",
                "parameters": [{"parameter": {"name": "query", "sensitive": False, "value": "SELECT 1234567890"}}],
            },
        },
    ]
}


def test_flattens_and_sorts_parameters_and_counts_unreadable_contexts():
    items, unreadable = flatten_parameters(CONTEXTS, max_value_chars=8)
    assert unreadable == 1
    assert items == [
        {
            "context": "clickhouse",
            "context_id": "c1",
            "name": "query",
            "sensitive": False,
            "provided": False,
            "inherited": False,
            "value": "SELECT 1",
            "value_length": 17,
        },
        {
            "context": "kafka",
            "context_id": "c2",
            "name": "bootstrap",
            "sensitive": False,
            "provided": False,
            "inherited": True,
            "value": "broker:9",
            "value_length": 11,
        },
        {
            "context": "kafka",
            "context_id": "c2",
            "name": "password",
            "sensitive": True,
            "provided": False,
            "inherited": False,
            "value": "***",
            "value_length": 3,
        },
    ]


def test_filters_parameters_by_context_name_and_value():
    def names(**filters):
        return [(i["context"], i["name"]) for i in flatten_parameters(CONTEXTS, **filters)[0]]

    assert names(context="kafka") == [("kafka", "bootstrap"), ("kafka", "password")]
    assert names(context="c1") == [("clickhouse", "query")]
    assert names(name_contains="pass") == [("kafka", "password")]
    assert names(value_contains="9092") == [("kafka", "bootstrap")]
    assert names(context="kafka", value_contains="SELECT") == []


def test_slices_one_parameter_value():
    assert parameter_slice(CONTEXTS, "clickhouse", "query", 7, 5) == {
        "context": "clickhouse",
        "context_id": "c1",
        "name": "query",
        "sensitive": False,
        "value_length": 17,
        "offset": 7,
        "value": "12345",
        "next_offset": 12,
    }
    assert parameter_slice(CONTEXTS, "c1", "query", 12, 5)["next_offset"] is None
    with pytest.raises(ValueError):
        parameter_slice(CONTEXTS, "kafka", "query", 0, 5)


def test_strips_descriptors_from_processor_and_controller_service():
    processor = {"id": "p", "config": {"properties": {"a": "1"}, "descriptors": {"a": {}}, "schedulingPeriod": "1s"}}
    assert strip_descriptors(processor) == {"id": "p", "config": {"properties": {"a": "1"}, "schedulingPeriod": "1s"}}
    service = {"id": "s", "properties": {"b": "2"}, "descriptors": {"b": {}}}
    assert strip_descriptors(service) == {"id": "s", "properties": {"b": "2"}}


def test_flattens_search_results_with_kind():
    results = {
        "processorResults": [
            {
                "id": "p1",
                "name": "Consume",
                "groupId": "g1",
                "parentGroup": {"id": "g1", "name": "Ingest"},
                "matches": [f"Name: m{i}" for i in range(7)] + ["x" * 1500],
            }
        ],
        "controllerServiceNodeResults": [{"id": "s1", "name": "Pool m", "groupId": "g2", "matches": ["Name: Pool m"]}],
        "parameterResults": [],
    }
    assert flatten_search_results(results, "m", MASKED) == [
        {
            "kind": "processor",
            "id": "p1",
            "name": "Consume",
            "process_group_id": "g1",
            "process_group": "Ingest",
            "matches": ["Name: m0", "Name: m1", "Name: m2", "Name: m3", "Name: m4"],
        },
        {
            "kind": "controller-service",
            "id": "s1",
            "name": "Pool m",
            "process_group_id": "g2",
            "process_group": None,
            "matches": ["Name: Pool m"],
        },
    ]


def test_search_cannot_confirm_masked_values():
    results = {
        "parameterResults": [
            {"id": "c1", "name": "db.url", "matches": ["Value: jdbc:pg://db/app?user=app&password=hunter2"]},
            {"id": "c1", "name": "api.token", "matches": ["Value: abc123", "Name: api.token"]},
        ]
    }
    assert flatten_search_results(results, "hunter2", MASKED) == []
    assert flatten_search_results(results, "abc", MASKED) == []
    shown = flatten_search_results(results, "jdbc", MASKED)
    assert shown[0]["matches"] == ["Value: jdbc:pg://db/app?user=app&password=***"]
    assert flatten_search_results(results, "token", MASKED)[0]["matches"] == ["Name: api.token"]


def test_search_kinds_and_long_matches():
    groups = {"processGroupResults": [{"id": "g", "matches": ["y" * 1500]}]}
    assert flatten_search_results(groups, "y", MASKED)[0]["kind"] == "process-group"
    labels = {"labelResults": [{"id": "l", "matches": ["y" * 1500]}]}
    assert len(flatten_search_results(labels, "y", MASKED)[0]["matches"][0]) == 1000


CID = "0b1d5c3e-0199-1000-ffff-ffffd1a2b3c4"
RID = "4f2a9e10-019a-1000-0000-00007e8d1f22"
MASKED = frozenset({"password"})


@pytest.mark.parametrize(
    "method, path",
    [
        ("POST", f"/flowfile-queues/{CID}/listing-requests"),
        ("DELETE", f"/flowfile-queues/{CID}/listing-requests/{RID}"),
        ("POST", "/provenance"),
        ("DELETE", f"/provenance/{RID}"),
    ],
)
def test_allows_only_temporary_listing_and_provenance_requests(method, path):
    check_request(method, path)


@pytest.mark.parametrize(
    "method, path",
    [
        ("POST", f"/flowfile-queues/{CID}/drop-requests"),
        ("DELETE", f"/flowfile-queues/{CID}/drop-requests/{RID}"),
        ("POST", "/provenance-events/replays"),
        ("POST", "/provenance/replays"),
        ("PUT", f"/processors/{CID}"),
        ("PUT", "/provenance"),
        ("DELETE", "/provenance/../processors/x"),
        ("DELETE", f"/provenance/{RID}/../../processors/{CID}"),
        ("POST", "/provenance/x"),
        ("POST", f"/provenance/{RID}"),
        ("GET", "/provenance"),
        ("DELETE", f"/flowfile-queues/{CID}/listing-requests"),
        ("POST", f"/flowfile-queues/{CID}/listing-requests/{RID}"),
        ("POST", "/flowfile-queues/../drop-requests/listing-requests"),
        ("POST", f"/flowfile-queues/{CID}/listing-requests?x=1"),
    ],
)
def test_rejects_every_other_request(method, path):
    with pytest.raises(ValueError, match="not allowed"):
        check_request(method, path)


def test_formats_nifi_dates_in_utc():
    assert nifi_date(1791968400000) == "10/14/2026 09:00:00 UTC"


def test_json_content_is_masked_reserialized_and_sliced():
    raw = b'{"user": "\xc3\xa9va", "Password": "s3cret"}'
    assert content_slice(raw, True, 0, 1000, MASKED) == {
        "content": '{"user": "éva", "Password": "***"}',
        "next_offset": None,
    }
    assert content_slice(raw, True, 10, 5, MASKED) == {"content": 'éva",', "next_offset": 15}


def test_truncated_json_content_is_refused():
    with pytest.raises(ValueError, match="cannot be masked"):
        content_slice(b'{"password": "s3', False, 0, 10, MASKED)


def test_text_content_is_sliced_and_a_cut_character_is_dropped():
    assert content_slice(b"hello world", True, 6, 100, MASKED) == {"content": "world", "next_offset": None}
    assert content_slice(b"hello world", True, 0, 5, MASKED) == {"content": "hello", "next_offset": 5}
    assert content_slice(b"caf\xc3", False, 0, 3, MASKED) == {"content": "caf", "next_offset": 3}


def test_binary_content_is_hidden_unless_allowed():
    raw = b"\xff\xfe\x00\x01\x02"
    hidden = content_slice(raw, True, 0, 10, MASKED)
    assert hidden["content"]["binary"] is True and "base64" not in hidden["content"]
    assert content_slice(raw, True, 1, 2, MASKED, True) == {"content": {"base64": "/gA=", "bytes": 2}, "next_offset": 3}
    assert content_slice(raw, True, 3, 10, MASKED, True) == {
        "content": {"base64": "AQI=", "bytes": 2},
        "next_offset": None,
    }


def test_latin1_and_ndjson_content_is_masked():
    latin1 = '{"password": "hunter2", "name": "José"}'.encode("latin-1")
    assert "hunter2" not in content_slice(latin1, True, 0, 1000, MASKED)["content"]
    ndjson = b'{"email": "a@b.c"}\n{"email": "d@e.f"}'
    assert content_slice(ndjson, True, 0, 1000, frozenset({"email"}))["content"] == '{"email": "***"}\n{"email": "***"}'
    yaml = b"user: app\npassword: hunter2\nurl: https://app:s3cret@db.example/x"
    masked = content_slice(yaml, True, 0, 1000, MASKED)["content"]
    assert "hunter2" not in masked and "s3cret" not in masked and "user: app" in masked


def test_nifi_paths_refuse_traversal():
    for bad in (
        "/connections/../parameter-contexts/x",
        "/processors/%2e%2e/x",
        "/processors/a?b=1",
        "/processors/..",
        "/processors/a#b",
        "/processors/a\\b",
    ):
        with pytest.raises(ValueError):
            check_path(bad)
    check_path("/flow/process-groups/root/status")
    check_path("/processors/0189a1b2-0000-1000-8000-00000000abcd/state")


def test_summarizes_queue_listing():
    listing = {
        "id": RID,
        "finished": True,
        "queueSize": {"byteCount": 2048, "objectCount": 2},
        "maxResults": 100,
        "flowFileSummaries": [
            {
                "uuid": "a1",
                "filename": "one.json",
                "size": 1024,
                "position": 1,
                "queuedDuration": 5000,
                "lineageDuration": 9000,
                "penalized": False,
                "penaltyExpiresIn": 0,
                "mimeType": "application/json",
                "clusterNodeId": "n1",
                "clusterNodeAddress": "nfa11:8080",
                "uri": "https://x",
            },
            {"uuid": "a2", "size": 1024, "position": 2},
        ],
    }
    assert summarize_listing(listing, 1) == {
        "queue_size": {"flowfiles": 2, "bytes": 2048},
        "listed": 2,
        "items": [
            {
                "uuid": "a1",
                "filename": "one.json",
                "size": 1024,
                "position": 1,
                "queued_duration_ms": 5000,
                "lineage_duration_ms": 9000,
                "penalized": False,
                "mime_type": "application/json",
                "cluster_node_id": "n1",
                "cluster_node_address": "nfa11:8080",
            }
        ],
    }


def test_summarizes_flowfile_with_masked_attributes():
    flowfile = {
        "uuid": "a1",
        "filename": "one.json",
        "size": 1024,
        "mimeType": "application/json",
        "queuedDuration": 5000,
        "lineageDuration": 9000,
        "penalized": True,
        "clusterNodeId": "n1",
        "contentClaimSection": "1",
        "attributes": {"filename": "one.json", "PASSWORD": "s3cret"},
    }
    assert summarize_flowfile(flowfile, MASKED) == {
        "uuid": "a1",
        "filename": "one.json",
        "size": 1024,
        "mime_type": "application/json",
        "queued_duration_ms": 5000,
        "lineage_duration_ms": 9000,
        "penalized": True,
        "cluster_node_id": "n1",
        "attributes": {"filename": "one.json", "PASSWORD": "***"},
    }


EVENT = {
    "eventId": 42,
    "eventTime": "10/14/2026 09:00:00.123 UTC",
    "eventType": "ATTRIBUTES_MODIFIED",
    "componentId": "p1",
    "componentName": "UpdateAttribute",
    "componentType": "UpdateAttribute",
    "flowFileUuid": "a1",
    "fileSize": "1 KB",
    "details": None,
    "clusterNodeId": "n1",
    "clusterNodeAddress": "nfa11:8080",
    "parentUuids": [],
    "childUuids": ["c1"],
    "attributes": [
        {"name": "filename", "value": "one.json", "previousValue": "one.json"},
        {"name": "retry", "value": "2", "previousValue": "1"},
        {"name": "password", "value": "new", "previousValue": "old"},
    ],
    "inputContentAvailable": True,
    "outputContentAvailable": False,
}


def test_summarizes_provenance_event():
    assert summarize_event(EVENT) == {
        "event_id": 42,
        "event_time": "10/14/2026 09:00:00.123 UTC",
        "event_type": "ATTRIBUTES_MODIFIED",
        "component_id": "p1",
        "component_name": "UpdateAttribute",
        "component_type": "UpdateAttribute",
        "flowfile_uuid": "a1",
        "file_size": "1 KB",
        "details": None,
        "cluster_node_id": "n1",
        "parent_uuids": [],
        "child_uuids": ["c1"],
    }


def test_event_details_mask_attributes_and_keep_changed_previous_values():
    details = event_details(EVENT, MASKED)
    assert details["attributes"] == {"filename": "one.json", "retry": "2", "password": "***"}
    assert details["previous_attributes"] == {"retry": "1", "password": "***"}
    assert details["input_content_available"] is True
    assert details["output_content_available"] is False
    assert details["event_id"] == 42


def test_content_slice_masks_secret_pairs_in_text():
    raw = b"admin=reports&password=hunter2&casino=12"
    assert content_slice(raw, True, 0, 100, frozenset())["content"] == "admin=reports&password=***&casino=12"


def test_parameters_blank_secret_named_values():
    contexts = {
        "parameterContexts": [
            {
                "component": {
                    "id": "c1",
                    "name": "Common",
                    "parameters": [
                        {"parameter": {"name": "db_password", "value": "p1"}},
                        {"parameter": {"name": "endpoint", "value": "https://h/?token=t1&a=1"}},
                    ],
                }
            }
        ]
    }
    items, unreadable = flatten_parameters(contexts)
    assert [(i["name"], i["value"]) for i in items] == [
        ("db_password", "***"),
        ("endpoint", "https://h/?token=***&a=1"),
    ]
    assert unreadable == 0


def test_strip_descriptors_masks_secret_properties():
    component = {
        "config": {"descriptors": {"x": 1}, "properties": {"Password": "p", "Remote URL": "https://h/?apikey=k"}}
    }
    assert strip_descriptors(component) == {
        "config": {"properties": {"Password": "***", "Remote URL": "https://h/?apikey=***"}}
    }
