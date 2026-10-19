import json
from datetime import UTC, datetime

from dataplane_mcp.kafka import Decoder, lag_summary, message_matches
from dataplane_mcp.masking import mask_text
from dataplane_mcp.paging import to_millis

MASKED = frozenset({"email", "phone"})
DECODER = Decoder(MASKED)


def decode(raw, masked_fields):
    return Decoder(masked_fields)(raw)


def test_masks_nested_json_fields_case_insensitively():
    raw = b'{"id": 7, "Email": "a@b.c", "profile": {"phone": "123", "country": "MT"}, "tags": [{"email": "x"}]}'
    assert decode(raw, MASKED) == {
        "id": 7,
        "Email": "***",
        "profile": {"phone": "***", "country": "MT"},
        "tags": [{"email": "***"}],
    }


def test_reports_schema_framed_values_without_a_registry():
    assert decode(b"\x00\x00\x00\x01\x2a" + b"payload", MASKED) == {"schema_id": 298, "bytes": 12}


def test_plain_text_and_binary():
    assert decode(b"hello", MASKED) == "hello"
    assert decode(b"\xff\xfe", MASKED) == {"bytes": 2, "note": "binary data can't be masked, so it is not shown"}
    assert Decoder(MASKED, show_binary=True)(b"\xff\xfe") == {"bytes": 2, "base64": "//4="}
    assert decode(None, MASKED) is None


def test_relative_and_iso_times():
    now = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
    assert to_millis("-30m", now) == int(datetime(2026, 10, 14, 11, 30, tzinfo=UTC).timestamp() * 1000)
    assert to_millis("-1h", now) == int(datetime(2026, 10, 14, 11, 0, tzinfo=UTC).timestamp() * 1000)
    assert to_millis("-2d", now) == int(datetime(2026, 10, 12, 12, 0, tzinfo=UTC).timestamp() * 1000)
    assert to_millis("2026-10-14T10:00:00+00:00", now) == 1791972000000


def test_search_matches_key_contains_and_field():
    raw = b'{"player": {"id": 42, "email": "a@b.c"}, "items": [{"sku": "x1"}], "ok": true}'
    assert message_matches(b"k1", raw, DECODER, key="k1")
    assert not message_matches(b"k2", raw, DECODER, key="k1")
    assert not message_matches(None, raw, DECODER, key="k1")
    assert message_matches(b"k1", raw, DECODER, contains='"sku": "x1"')
    assert message_matches(b"k1", raw, DECODER, field="player.id", value="42")
    assert message_matches(b"k1", raw, DECODER, field="items.0.sku", value="x1")
    assert message_matches(b"k1", raw, DECODER, field="ok", value="true")
    assert not message_matches(b"k1", raw, DECODER, field="player.missing", value="42")
    assert not message_matches(b"k1", raw, DECODER, key="k1", contains="nope")


def test_search_cannot_reveal_masked_fields():
    raw = b'{"player": {"email": "a@b.c"}}'
    assert not message_matches(None, raw, DECODER, contains="a@b.c")
    assert not message_matches(None, raw, DECODER, field="player.email", value="a@b.c")
    assert message_matches(None, raw, DECODER, field="player.email", value="***")


def test_search_plain_text_and_avro():
    assert message_matches(None, b"hello world", DECODER, contains="lo wo")
    assert not message_matches(None, b"hello world", DECODER, field="a", value="b")
    assert not message_matches(None, b"\x00\x00\x00\x01\x2apayload", DECODER, contains="payload")
    assert message_matches(None, b"\x00\x00\x00\x01\x2apayload", DECODER)


def test_lag_summary():
    partitions = [
        {"topic": "b", "partition": 0, "committed": 10, "latest": 15, "lag": 5},
        {"topic": "a", "partition": 0, "committed": 3, "latest": 3, "lag": 0},
        {"topic": "a", "partition": 1, "committed": -1001, "latest": 7, "lag": None},
    ]
    assert lag_summary(partitions) == {
        "total_lag": 5,
        "partitions_committed": 2,
        "partitions_behind_retention": 0,
        "topics": ["a", "b"],
    }


def test_masks_secret_looking_keys_and_pairs_without_configuration():
    raw = b'{"user": "x", "db_password": "p1", "Authorization": "Bearer t", "url": "https://h/r?id=7&api_key=k2&x=1"}'
    assert decode(raw, frozenset()) == {
        "user": "x",
        "db_password": "***",
        "Authorization": "***",
        "url": "https://h/r?id=7&api_key=***&x=1",
    }


def test_masks_secret_pairs_in_form_text():
    assert decode(b"admin=reports&password=hunter2&casino=12&token=abc", frozenset()) == (
        "admin=reports&password=***&casino=12&token=***"
    )


def test_masking_long_text_stays_linear():
    import time

    started = time.monotonic()
    assert mask_text("a" * 50000 + "=1&password=x") == "a" * 50000 + "=1&password=***"
    assert time.monotonic() - started < 1


def test_key_filter_matches_the_masked_key_only():
    raw_key = b'{"email": "a@b.c", "id": 1}'
    assert not message_matches(raw_key, b"v", DECODER, key='{"email": "a@b.c", "id": 1}')
    assert message_matches(raw_key, b"v", DECODER, key='{"id": 1, "email": "***"}')
    assert message_matches(b"plain-key", b"v", DECODER, key="plain-key")


def test_latin1_ndjson_and_nested_json_values_are_masked():
    assert "hunter2" not in str(decode('{"password": "hunter2", "n": "José"}'.encode("latin-1"), MASKED))
    assert decode(b'{"email": "a@b.c"}\n{"email": "d@e.f"}', MASKED) == '{"email": "***"}\n{"email": "***"}'
    nested = decode(b'{"payload": "{\\"email\\": \\"a@b.c\\"}"}', MASKED)
    assert "a@b.c" not in json.dumps(nested)
