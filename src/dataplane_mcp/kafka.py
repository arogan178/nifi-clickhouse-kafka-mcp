import base64
import io
import json
import time
from datetime import UTC, datetime

import requests
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

from .config import KafkaConfig, SchemaRegistryConfig
from .masking import decode_text, mask, mask_document
from .paging import to_millis

MAX_SAMPLE = 50
MAX_VALUE_CHARS = 4000
MAX_SCAN = 1_000_000
MAX_SEARCH_SECONDS = 120
_MISSING = object()


class Decoder:
    """Turns message bytes into masked JSON-safe values; Confluent-framed values via the schema registry."""

    def __init__(
        self, masked_fields: frozenset[str], registry: SchemaRegistryConfig | None = None, show_binary: bool = False
    ):
        self.masked_fields = masked_fields
        self.show_binary = show_binary
        self.registry = registry
        self._schemas: dict[int, tuple[str, object]] = {}
        if registry:
            self.session = requests.Session()
            if registry.username:
                self.session.auth = (registry.username, registry.password or "")
            if registry.ca_bundle:
                self.session.verify = registry.ca_bundle

    def schema(self, schema_id: int) -> tuple[str, object]:
        if schema_id not in self._schemas:
            response = self.session.get(f"{self.registry.url.rstrip('/')}/schemas/ids/{schema_id}", timeout=15)
            if not response.ok:
                raise RuntimeError(f"schema registry {response.status_code}: {response.text[:300]}")
            body = response.json()
            schema_type = body.get("schemaType", "AVRO")
            parsed = None
            if schema_type == "AVRO":
                import fastavro

                parsed = fastavro.parse_schema(json.loads(body["schema"]))
            self._schemas[schema_id] = (schema_type, parsed)
        return self._schemas[schema_id]

    def parse(self, raw: bytes | None):
        """The full masked value, or _MISSING when it can't be read as text, JSON or a registered schema."""
        if raw is None:
            return None
        if raw[:1] == b"\x00" and len(raw) > 5:
            return self._framed(raw)
        text = decode_text(raw)
        if text is None:
            return _MISSING
        try:
            return mask(json.loads(text), self.masked_fields)
        except ValueError:
            return mask_document(text, self.masked_fields)

    def _framed(self, raw: bytes):
        schema_id = int.from_bytes(raw[1:5], "big")
        if not self.registry:
            return _MISSING
        schema_type, parsed = self.schema(schema_id)
        if schema_type == "AVRO":
            import fastavro

            return mask(_jsonable(fastavro.schemaless_reader(io.BytesIO(raw[5:]), parsed, None)), self.masked_fields)
        if schema_type == "JSON":
            return mask(json.loads(raw[5:]), self.masked_fields)
        return _MISSING

    def __call__(self, raw: bytes | None):
        """The value for display: as parse(), cut to MAX_VALUE_CHARS, else a description of the bytes."""
        try:
            value = self.parse(raw)
        except Exception as e:
            value, error = _MISSING, str(e)[:300]
        else:
            error = None
        if value is _MISSING:
            described = {"bytes": len(raw)}
            if raw[:1] == b"\x00" and len(raw) > 5:
                described["schema_id"] = int.from_bytes(raw[1:5], "big")
                if self.registry:
                    described["schema_type"] = self._schemas.get(described["schema_id"], ("unknown",))[0]
            elif self.show_binary:
                described["base64"] = base64.b64encode(raw[:3000]).decode()
            else:
                described["note"] = "binary data can't be masked, so it is not shown"
            if error:
                described["decode_error"] = error
            return described
        if isinstance(value, str):
            return value[:MAX_VALUE_CHARS]
        return value


def _jsonable(value):
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _field(value, path: str):
    for part in path.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            return _MISSING
    return value


def message_matches(
    raw_key: bytes | None,
    raw_value: bytes | None,
    decoder: Decoder,
    contains: str | None = None,
    key: str | None = None,
    field: str | None = None,
    value: str | None = None,
) -> bool:
    if key is not None and not key_matches(raw_key, key, decoder):
        return False
    if contains is None and field is None:
        return True
    if raw_value is None:
        return False
    # Match on the masked value so a search can't reveal masked fields.
    try:
        parsed = decoder.parse(raw_value)
    except Exception:
        return False
    if parsed is _MISSING:
        return False
    text = parsed if isinstance(parsed, str) else json.dumps(parsed, ensure_ascii=False)
    if contains is not None and contains not in text:
        return False
    if field is not None:
        found = _field(parsed, field) if not isinstance(parsed, str) else _MISSING
        if found is _MISSING:
            return False
        if (found if isinstance(found, str) else json.dumps(found)) != value:
            return False
    return True


def key_matches(raw_key: bytes | None, key: str, decoder: Decoder) -> bool:
    """Compare with the key as shown, masked, so a guessed masked value can't be confirmed."""
    if raw_key is None:
        return False
    try:
        shown = decoder.parse(raw_key)
    except Exception:
        return False
    if shown is _MISSING:
        return False
    if isinstance(shown, str):
        if shown.strip()[:1] in ("{", "["):
            try:
                return json.loads(shown) == json.loads(key)
            except ValueError:
                pass
        return shown == key
    try:
        return shown == json.loads(key)
    except ValueError:
        return False


def lag_summary(partitions: list[dict]) -> dict:
    return {
        "total_lag": sum(p["lag"] for p in partitions if p["lag"] is not None),
        "partitions_committed": sum(1 for p in partitions if p["committed"] >= 0),
        "partitions_behind_retention": sum(1 for p in partitions if p.get("behind_retention")),
        "topics": sorted({p["topic"] for p in partitions}),
    }


def headers(raw: list | None, decoder: "Decoder") -> list | None:
    """Message headers as [key, value] pairs in order, so repeated keys are kept."""
    if not raw:
        return None
    return [[k, decoder(v)] for k, v in raw]


def record(m, decoder: "Decoder") -> dict:
    return {
        "partition": m.partition(),
        "offset": m.offset(),
        "timestamp": m.timestamp()[1],
        "key": decoder(m.key()),
        "value": decoder(m.value()),
        "headers": headers(m.headers(), decoder),
    }


class Kafka:
    """Read-only Kafka client: it never subscribes or commits, so it can't move any consumer group's offsets."""

    def __init__(self, cfg: KafkaConfig, masked_fields: frozenset[str], show_binary: bool = False):
        self.conf = {k: str(v).lower() if isinstance(v, bool) else v for k, v in cfg.config.items()}
        self.decode = Decoder(masked_fields, cfg.schema_registry, show_binary)
        self._admin = None

    @property
    def admin(self) -> AdminClient:
        # Created on first use, so starting the server doesn't need every cluster reachable.
        if self._admin is None:
            self._admin = AdminClient(self.conf)
        return self._admin

    def _consumer(self) -> Consumer:
        # librdkafka insists on a group.id; assign() without commits never joins or touches it.
        return Consumer({**self.conf, "group.id": "dataplane-mcp-unused", "enable.auto.commit": False})

    def cluster(self) -> dict:
        metadata = self.admin.list_topics(timeout=15)
        topics = [t for name, t in metadata.topics.items() if not name.startswith("__")]
        under_replicated = [
            {"topic": t.topic, "partition": p.id, "replicas": list(p.replicas), "in_sync": list(p.isrs)}
            for t in topics
            for p in t.partitions.values()
            if len(p.isrs) < len(p.replicas)
        ]
        offline = [{"topic": t.topic, "partition": p.id} for t in topics for p in t.partitions.values() if p.leader < 0]
        return {
            "cluster_id": metadata.cluster_id,
            "controller_id": metadata.controller_id,
            "brokers": sorted(
                ({"id": b.id, "host": b.host, "port": b.port} for b in metadata.brokers.values()), key=lambda b: b["id"]
            ),
            "topics": len(topics),
            "partitions": sum(len(t.partitions) for t in topics),
            "under_replicated_partitions": {"total": len(under_replicated), "items": under_replicated[:50]},
            "offline_partitions": {"total": len(offline), "items": offline[:50]},
        }

    def topics(self, contains: str | None) -> list[dict]:
        metadata = self.admin.list_topics(timeout=15)
        return sorted(
            (
                {"topic": name, "partitions": len(t.partitions)}
                for name, t in metadata.topics.items()
                if not contains or contains in name
            ),
            key=lambda t: t["topic"],
        )

    def describe_topic(self, topic: str) -> dict:
        consumer = self._consumer()
        try:
            metadata = consumer.list_topics(topic, timeout=15).topics[topic]
            if metadata.error:
                raise RuntimeError(str(metadata.error))
            partitions = []
            for p in sorted(metadata.partitions):
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
                partitions.append({"partition": p, "earliest": low, "latest": high, "messages": high - low})
        finally:
            consumer.close()
        resource = ConfigResource(ResourceType.TOPIC, topic)
        configs = self.admin.describe_configs([resource])[resource].result(timeout=15)
        return {
            "topic": topic,
            "partitions": partitions,
            "configs": {k: v.value for k, v in configs.items() if not v.is_default},
        }

    def consumer_groups(self) -> list[dict]:
        result = self.admin.list_consumer_groups(request_timeout=15).result()
        return sorted(
            ({"group": g.group_id, "state": g.state.name if g.state else None} for g in result.valid),
            key=lambda g: g["group"],
        )

    def consumer_lag(self, group: str) -> dict:
        states = {g["group"]: g["state"] for g in self.consumer_groups()}
        if group not in states:
            raise ValueError(f"consumer group {group!r} does not exist; use kafka_consumer_groups to find it")
        partitions = self.partition_lag(group)
        summary = lag_summary(partitions)
        result = {"group": group, "state": states[group], "total_lag": summary["total_lag"], "partitions": partitions}
        if not summary["partitions_committed"]:
            result["note"] = "the group exists but has no committed offsets"
        return result

    def partition_lag(self, group: str) -> list[dict]:
        request = [ConsumerGroupTopicPartitions(group)]
        committed = self.admin.list_consumer_group_offsets(request)[group].result(timeout=15)
        consumer = self._consumer()
        try:
            lag = []
            for tp in committed.topic_partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=15)
                lag.append(
                    {
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "committed": tp.offset,
                        "earliest": low,
                        "latest": high,
                        "lag": high - tp.offset if tp.offset >= 0 else None,
                        # Retention already deleted messages the group never read.
                        "behind_retention": 0 <= tp.offset < low,
                    }
                )
        finally:
            consumer.close()
        return sorted(lag, key=lambda r: (r["topic"], r["partition"]))

    def sample(self, topic: str, partition: int, start: str, limit: int) -> dict:
        limit = max(1, min(limit, MAX_SAMPLE))
        consumer = self._consumer()
        try:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=15)
            if start == "latest":
                offset = max(low, high - limit)
            elif start == "earliest":
                offset = low
            elif start.isdigit():
                offset = int(start)
            else:
                millis = to_millis(start)
                offset = consumer.offsets_for_times([TopicPartition(topic, partition, millis)], timeout=15)[0].offset
                if offset < 0:
                    return {"earliest": low, "latest": high, "messages": []}
            consumer.assign([TopicPartition(topic, partition, offset)])
            messages = []
            while len(messages) < limit and offset + len(messages) < high:
                batch = consumer.consume(num_messages=limit - len(messages), timeout=5)
                if not batch:
                    break
                for m in batch:
                    if m.error():
                        raise RuntimeError(str(m.error()))
                    messages.append(record(m, self.decode))
        finally:
            consumer.close()
        return {"earliest": low, "latest": high, "messages": messages}

    def search(
        self,
        topic: str,
        contains: str | None,
        key: str | None,
        field: str | None,
        value: str | None,
        start: str,
        end: str | None,
        max_scan: int,
        limit: int,
        max_seconds: int,
        resume: dict | None,
    ) -> dict:
        if (field is None) != (value is None):
            raise ValueError("field and value must be given together")
        max_scan, limit = max(1, min(max_scan, MAX_SCAN)), max(1, limit)
        deadline = time.monotonic() + max(1, min(max_seconds, MAX_SEARCH_SECONDS))
        now = datetime.now(UTC)
        consumer = self._consumer()
        try:
            metadata = consumer.list_topics(topic, timeout=15).topics[topic]
            if metadata.error:
                raise RuntimeError(str(metadata.error))
            partitions = sorted(metadata.partitions)
            starts, ends = {}, {}
            for p in partitions:
                starts[p], ends[p] = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
            if start != "earliest":
                starts = self._offsets_at(consumer, topic, to_millis(start, now), ends)
            if end is not None:
                ends = self._offsets_at(consumer, topic, to_millis(end, now), ends)
            if resume:
                # JSON object keys arrive as strings.
                starts.update({int(p): int(o) for p, o in resume.items() if int(p) in starts})
            reached = dict(starts)
            active = {p for p in partitions if starts[p] < ends[p]}
            consumer.assign([TopicPartition(topic, p, starts[p]) for p in active])
            matches, scanned, stopped = [], 0, "end"
            while active and stopped == "end":
                if time.monotonic() >= deadline:
                    stopped = "time"
                    break
                batch = consumer.consume(num_messages=1000, timeout=10)
                if not batch:
                    # Transaction markers take offsets but are never delivered, so a partition can stop short of
                    # its end offset; the consumer's position shows whether it really got there.
                    for tp in consumer.position([TopicPartition(topic, p) for p in active]):
                        if tp.offset >= ends[tp.partition]:
                            reached[tp.partition] = ends[tp.partition]
                    break
                for m in batch:
                    if m.error():
                        raise RuntimeError(str(m.error()))
                    p = m.partition()
                    if p not in active:
                        continue
                    if m.offset() >= ends[p]:
                        active.discard(p)
                        consumer.pause([TopicPartition(topic, p)])
                        continue
                    scanned += 1
                    reached[p] = m.offset() + 1
                    if message_matches(m.key(), m.value(), self.decode, contains, key, field, value):
                        matches.append(record(m, self.decode))
                    if reached[p] >= ends[p]:
                        active.discard(p)
                        consumer.pause([TopicPartition(topic, p)])
                    if len(matches) >= limit:
                        stopped = "limit"
                        break
                    if scanned >= max_scan:
                        stopped = "max_scan"
                        break
        finally:
            consumer.close()
        unfinished = {p: reached[p] for p in partitions if reached[p] < ends[p]}
        if unfinished and stopped == "end":
            stopped = "idle"
        return {
            "matches": matches,
            "scanned": scanned,
            "stopped": stopped,
            "per_partition_end": {p: {"reached": reached[p], "end": ends[p]} for p in partitions},
            "resume": unfinished or None,
        }

    @staticmethod
    def _offsets_at(consumer: Consumer, topic: str, millis: int, highs: dict[int, int]) -> dict[int, int]:
        found = consumer.offsets_for_times([TopicPartition(topic, p, millis) for p in highs], timeout=15)
        # -1 means no message at or after that time, so the partition's high watermark.
        return {tp.partition: tp.offset if tp.offset >= 0 else highs[tp.partition] for tp in found}
