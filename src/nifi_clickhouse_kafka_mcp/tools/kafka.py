from ..config import KafkaConfig
from ..kafka import Kafka, lag_summary
from ..paging import page
from ..server import Toolbox, env_type


def register(box: Toolbox, configs: dict[str, KafkaConfig]) -> None:
    kafka = {name: Kafka(cfg, box.cfg.masked_fields, box.cfg.show_binary) for name, cfg in configs.items()}
    Env = env_type(list(configs))

    @box.tool
    def kafka_cluster(user: str, env: Env) -> dict:
        """Brokers, controller, topic and partition counts, and under-replicated or offline partitions."""
        return kafka[env].cluster()

    @box.tool
    def kafka_topics(user: str, env: Env, contains: str | None = None, offset: int = 0, limit: int = 200) -> dict:
        """List Kafka topics sorted by name, optionally filtered by a substring."""
        return page(kafka[env].topics(contains), offset, limit)

    @box.tool
    def kafka_describe_topic(user: str, env: Env, topic: str) -> dict:
        """Partitions with earliest/latest offsets and non-default configs of a Kafka topic."""
        return kafka[env].describe_topic(topic)

    @box.tool
    def kafka_consumer_groups(
        user: str, env: Env, contains: str | None = None, with_lag: bool = False, offset: int = 0, limit: int = 50
    ) -> dict:
        """List consumer groups (id and state) sorted by id, optionally filtered by a substring.

        with_lag adds total lag, partitions with committed offsets and topics for each group on the page;
        it costs one offsets lookup per group, so keep limit small.
        """
        groups = [g for g in kafka[env].consumer_groups() if not contains or contains in g["group"]]
        result = page(groups, offset, limit)
        if with_lag:
            for g in result["items"]:
                g.update(lag_summary(kafka[env].partition_lag(g["group"])))
        return result

    @box.tool
    def kafka_consumer_lag(user: str, env: Env, group: str) -> dict:
        """Committed offsets and lag per partition for an existing consumer group (see kafka_consumer_groups).

        behind_retention marks partitions where the committed offset is older than the earliest retained
        message, so the group has already lost data there.
        """
        return kafka[env].consumer_lag(group)

    @box.tool
    def kafka_sample(
        user: str, env: Env, topic: str, partition: int = 0, start: str = "latest", limit: int = 10
    ) -> dict:
        """Read up to 50 messages from one partition without joining any consumer group.

        start is "latest" (the last messages), "earliest", an offset, an ISO timestamp or relative "-1h".
        earliest == latest means the partition is empty. Keys, values and headers are decoded (JSON, text,
        Schema Registry Avro/JSON Schema when configured, else base64) and masked.
        """
        return kafka[env].sample(topic, partition, start, limit)

    @box.tool
    def kafka_search(
        user: str,
        env: Env,
        topic: str,
        contains: str | None = None,
        key: str | None = None,
        field: str | None = None,
        value: str | None = None,
        start: str = "earliest",
        end: str | None = None,
        max_scan: int = 100000,
        limit: int = 20,
        max_seconds: int = 60,
        resume: dict[str, int] | None = None,
    ) -> dict:
        """Scan all partitions of a topic for messages matching every given filter, without joining a group.

        key: the message key equals this string. contains: substring of the value text (JSON and decoded
        Avro values are matched as JSON with masked fields already masked). field + value: a dotted JSON
        path (e.g. "player.id", list indexes allowed) whose value, as a string, equals value.
        start: "earliest", an ISO timestamp, or relative "-30m"/"-1h"/"-2d". end: same formats, default now.
        Stops after limit matches, max_scan messages scanned (at most 1,000,000) or max_seconds (at most 120);
        "idle" means no messages arrived for 10 seconds before the end. stopped says which; per_partition_end gives each partition's reached and end offsets. To continue,
        repeat the call with the same filters, start and end, plus resume set to the returned resume
        (partition -> next offset; None once every partition is done).
        """
        return kafka[env].search(topic, contains, key, field, value, start, end, max_scan, limit, max_seconds, resume)
