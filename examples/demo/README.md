# Demo stack

A local NiFi 2, ClickHouse and Kafka with some data in each, to try nifi-clickhouse-kafka-mcp without touching real systems.

```bash
docker compose up -d
```

ClickHouse and Kafka are ready in seconds, NiFi in about a minute (`curl -k https://localhost:8443/nifi-api/flow/about`
answers 401 when it is up).

**NiFi:** a flow whose queue backs up behind a stopped processor, and a parameter context with secrets in it:

```bash
uv run --with requests python seed_nifi.py
```

**Kafka:** 300 JSON orders on a 3-partition topic, and a consumer group that has read 120 of them:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 3
python3 -c '
import json
for i in range(300):
    print(f"order-{i}|" + json.dumps({"id": i, "customer": {"email": f"user{i % 50}@example.com", "tier": ["gold", "silver"][i % 2]}, "amount": i * 1.5, "api_key": "k-secret"}))
' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders --reader-property parse.key=true --reader-property 'key.separator=|'
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --group billing --from-beginning --max-messages 120 > /dev/null
```

**ClickHouse** seeds itself from `clickhouse-init.sql`: 5,000 orders, and a `mcp` user that can only read.

Then point a client at [config.yaml](config.yaml):

```bash
claude mcp add data -- uvx nifi-clickhouse-kafka-mcp --config "$PWD/config.yaml"
```

Things to ask:

- "What's wrong in the demo NiFi flow?"
- "Show me a FlowFile waiting in the orders queue."
- "How far behind is the billing consumer group?"
- "Find the Kafka message for order-42."
- "Which ClickHouse columns hold customer data?"
- "Insert a row into shop.orders." (it can't)

`NIFI_CLICKHOUSE_KAFKA_MCP_DEMO=1 uv run pytest tests/integration` from the repository root runs the end-to-end tests against this
stack. `docker compose down` removes it.
