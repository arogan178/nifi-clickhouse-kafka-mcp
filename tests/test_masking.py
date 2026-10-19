import time

import pytest

from nifi_clickhouse_kafka_mcp.masking import mask_document, mask_text


@pytest.mark.parametrize(
    "text, masked",
    [
        ('{\\"password\\":\\"hunter2\\"}', '{\\"password\\":\\"***\\"}'),
        ("redis://:hunter2@h:6379", "redis://:***@h:6379"),
        ("postgres://app:hunter2@db/x", "postgres://app:***@db/x"),
        ("<password>hunter2</password>", "<password>***</password>"),
        ("password =hunter2", "password =***"),
        ("db_password: hunter2, user: app", "db_password: ***, user: app"),
        ("Authorization: Bearer abc.def", "Authorization: ***"),
        ('<Password type="x">hunter2</Password>', '<Password type="x">***</Password>'),
        ("curl -u admin:hunter2 https://x", "curl -u admin:*** https://x"),
        ("curl --user=admin:hunter2 https://x", "curl --user=admin:*** https://x"),
    ],
)
def test_masks_common_secret_forms(text, masked):
    assert mask_text(text) == masked


@pytest.mark.parametrize(
    "text",
    ["a." * 200000 + ":", "password" * 50000 + " =", "token: x, " * 40000, "<password>" * 40000, "x://" * 100000],
)
def test_hostile_input_stays_fast(text):
    started = time.monotonic()
    mask_document(text, frozenset({"email"}))
    assert time.monotonic() - started < 1
