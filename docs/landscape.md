# Landscape: NiFi, ClickHouse and Kafka MCP servers

Surveyed 2026-10-19 from GitHub (`gh api`, issue lists, READMEs) and live `.well-known` metadata. Star counts are a snapshot.
The point of this file is to remember what users of other servers ask for and complain about, so this project answers it.

## NiFi

No dominant server. Most target NiFi 1.x, run over stdio only, and are read-write by default.

| Project | Stars | Transport | Auth | RW | Notes |
|---|---|---|---|---|---|
| ms82119/NiFiMCP | 34 | stdio + own REST/chat UI | basic, mTLS (PR #21) | RW, ~30 tools, flow building | Tested on 1.23/1.28 |
| cloudera/NiFi-MCP-Server | 5 | stdio | Knox token/cookie; standalone + mTLS stuck in PR #1 | read-only by default (24 read / 42 write) | `get_flow_health_status`, auto-detects 1.x/2.x |
| alexxonline/nifi-mcp | 0 | stdio | user/password JWT, refresh on 401 | write tools unregistered unless opted in | `nifi_get_lineage`; verified on 1.15.3 only |
| newen-systems/nifi-mcp | 0 | stdio, HTTP | Keycloak introspection, per-user proxied mTLS | writes on by default | NiFi 2.11 |
| gaojie197710-tech/rosetta-nifi-mcp | 1 | stdio | mTLS | read-only unless opted in, 44 tools | "worst first" summaries, `diagnose` |
| AlekseiSeleznev/nifi-mcp-universal | 1 | HTTP + dashboard | API key | read-only default, 69 tools | multi-connection |
| Giojacke/mcp-nifipilot | 1 | stdio, SSE | JWT | readonly mode | NiFi 2.2 |

User pain: weak NiFi 2.x and auth coverage (kevinbtalbert/NiFi-MCP-Server#3: "how to use without Knox"), and navigating big flows
(ms82119/NiFiMCP#1: recursive listing, name/regex filters, parameter lookup, dependency graph).

Note: a gateway that proxies the end user to NiFi 2 is authorised on the end user's rights alone, so it can write wherever
the user can. This project uses its own view-only identity instead.

## ClickHouse

`ClickHouse/mcp-clickhouse` (889 stars, v0.7.0) dominates: stdio/HTTP/SSE, bearer or OIDC, `readonly=1` by default,
paginated `list_tables`, `params`, big ints as strings. `Altinity/altinity-mcp` (38 stars, Go) adds server-side row caps,
JWT-claim roles, dynamic tools from views and Prometheus metrics.

Recurring pain:
- Results too large: mcp-clickhouse#25 (list_tables on 100+ tables), #223 (3M rows = 231 MiB payload, no cap, open);
  altinity-mcp#124 (rewriting SQL to add LIMIT breaks queries).
- Read-only that isn't: mcp-clickhouse#131 (agent ran DROP/CREATE/INSERT via a "select" tool), #248 (KILL QUERY works
  under readonly=1, open); altinity-mcp#56, #183 (OpenAPI write bypass).
- Remote without auth: mcp-clickhouse#68, #171 (OAuth broke in an update); altinity-mcp#180-182.
- Per-user credentials and audit: mcp-clickhouse#155 ("track the user activity").
- One process per cluster: altinity-mcp#132, #136.
- Timeouts and blocking: mcp-clickhouse#74 (hardcoded 30 s), #128 (one long query blocks list_tools).
- Kafka/S3 engine tables crash table listing: mcp-clickhouse#42, #47.
- Packaging: stale image (#161), slow start (#160).

Worth copying: `EXPLAIN ESTIMATE` before heavy queries, big ints as strings, bound parameters, paged table listing with
filters, read_rows/elapsed in results, write tools never registered rather than blocked.

## Kafka

| Project | Stars | Transport | Auth to the MCP | RW | Notes |
|---|---|---|---|---|---|
| confluentinc/mcp-confluent | 168 | stdio, HTTP, SSE | static API key header | RW, per-connection read_only | SR decoding (Avro/JSON/Protobuf), lag; creates a random consumer group per read |
| kanapuli/mcp-kafka | 81 | stdio | none | RW | fixed consumer group; dormant since 2025-03 |
| tuannvm/kafka-mcp-server | 56 | stdio, HTTP | OAuth 2.1 | RW | cluster_overview, lag report prompts |
| aywengo/kafka-schema-reg-mcp | 32 | stdio, HTTP | OAuth 2.1 | RW + read-only mode | Schema Registry only |
| streamnative/streamnative-mcp-server | 24 | stdio, SSE | - | RW + read-only | Kafka + Pulsar |
| conduktor/kafka-mcp | 5 | inside Conduktor Console | Console RBAC + audit | mostly read | get-record-at, lineage |
| Netroforge/kafka-mcp | 1 | stdio | none | read-only unless opted in | assign-only reads, 4 KB caps, test that fails on write imports |
| sanjay-amu/kafka-sentinel-mcp | 3 | stdio, SSE | none | read-only | replay_readiness, incident_snapshot |

Recurring pain:
- Secured clusters: mcp-confluent#21, #84 (SASL PLAIN forced on mTLS clusters), #54.
- Multiple clusters: mcp-confluent#65, epic #532.
- Decoding: mcp-confluent#438 (keys not SR-decoded), #597 (repeated headers merged), #158 (Protobuf).
- Message search: mcp-confluent#476, tuannvm#84.
- Offset side effects: streamnative#11 (consume commits wrong offset).
- Client schema compatibility: arrays without `items` (mcp-confluent#581, streamnative#26), defaults marked required (#482).
- SSE fragility: mcp-confluent#337, #425; streamnative#38.

## Remote gateway patterns (GitHub, Grafana, Sentry, Cloudflare)

- OAuth: protected resource metadata plus an authorization server with dynamic client registration, or Claude.ai/ChatGPT/VS
  Code cannot connect (github-mcp-server#1404, #549, #132). Grafana has only static tokens and users ask for OIDC (#284, #289).
- Read-only and toolsets: GitHub `--read-only` beats explicit tool lists; `X-MCP-Readonly`/`X-MCP-Toolsets` headers. Grafana
  `--disable-write` removes raw SQL tools because "they can write when the datasource credentials permit it": the two-lock
  argument, stated publicly.
- Context size: github-mcp-server#142 (64k tokens for 30 commits); Grafana summary tools and Loki byte/time guardrails.
- Too many tools: mcp-grafana#1092, github-mcp-server#352. Toolset selection matters.
- HTTP hardening: Host/Origin checks; refuse to bind non-loopback without auth; `/healthz` and `/metrics`.
- PII redaction requested: mcp-grafana#1124.
- Packaging: uvx, Docker, Helm chart, stdio and streamable HTTP, no SSE.

## What this project does about it

| Pain | Answer here |
|---|---|
| Results blow the context | every list pages; ClickHouse streams and cuts at max rows/chars with `truncated`; global result cap |
| Read-only that isn't | two locks: only read tools exist and only allowlisted requests are sent, plus read-only backend identities; ClickHouse statements checked on ClickHouse's own parse, table functions allowlisted |
| Remote without auth / static keys | OIDC bearer tokens from any provider, group gate, protected resource metadata; refuses unauthenticated non-loopback HTTP |
| No per-user audit | JSON audit line per call with the user; ClickHouse `log_comment` carries the user into `system.query_log` |
| One cluster per process | named environments, each with any of NiFi, ClickHouse, Kafka |
| Consumer group side effects | Kafka reads use assign() and never join or commit |
| Keys/values not decoded | Schema Registry decoding of keys and values (Avro, JSON Schema) |
| Message search | bounded, resumable search by key, substring or JSON field over a time window |
| NiFi 2 + auth gaps | NiFi 2 first; mTLS, username/password, bearer token |
| Big NiFi flows | search, health roll-up worst first, paged parameters, queue and provenance inspection |
| Secrets/PII in output | secret-looking keys and pairs masked everywhere, plus configured field names |
