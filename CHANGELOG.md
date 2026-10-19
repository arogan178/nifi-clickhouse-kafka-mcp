# Changelog

## 0.1.0

First public release.

- NiFi: health roll-up, flow tree, search, process groups, components, processor state, controller services,
  bulletins, parameters, queues, FlowFiles with content, provenance. mTLS, username/password and token auth.
- ClickHouse: streamed queries with row and size caps, bound parameters and stats; databases, tables, table
  descriptions, column search, health. Statements checked with ClickHouse's own parser; table functions refused
  anywhere in a query unless allowlisted.
- Kafka: cluster overview, topics, consumer groups and lag with retention check, sampling and resumable search
  without joining a group; Schema Registry Avro and JSON Schema decoding; headers.
- stdio and streamable HTTP; OIDC bearer tokens with group gate and protected resource metadata, or a trusted
  user header from an authenticating proxy; audit log;
  Prometheus metrics; secret and field masking.
