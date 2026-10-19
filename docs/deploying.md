# Deploying for a team

Run one dataplane-mcp over HTTP, let people log in with their own accounts, and give the server read-only identities
on every backend.

## 1. Read-only backend identities

This is the second lock. Set it up even though the gateway only sends reads.

### NiFi

Create an identity for the gateway (a client certificate such as `CN=dataplane-mcp`, or an LDAP/single-user login) and
give it only these read policies:

| Policy | Resource | Needed for |
|---|---|---|
| view the user interface | `/flow` | everything |
| view the component | `/process-groups/<root id>` (inherited below) | flow, components, health, state |
| view parameter contexts | `/parameter-contexts` | parameters |
| view the data | `/data/process-groups/<root id>` | queue listings, FlowFile content |
| view provenance | `/provenance`, `/provenance-data/process-groups/<root id>` | provenance tools |

NiFi answers 400 to a `Host` it doesn't know, so the hostname in `url` must be in `nifi.web.proxy.host` (or be the
node's own name). This bites when the gateway reaches NiFi through a load balancer or a Docker host alias.

Never grant **proxy user requests**. NiFi 2 authorizes a proxied request on the end user's rights alone, so a proxying
gateway could write wherever that user can.

[scripts/nifi_grant.py](../scripts/nifi_grant.py) grants these on a secured cluster using a node certificate.

### ClickHouse

```sql
CREATE USER mcp_reader IDENTIFIED BY '...' SETTINGS readonly = 2, max_execution_time = 60 READONLY;
GRANT SELECT ON analytics.* TO mcp_reader;
GRANT SELECT ON system.* TO mcp_reader;     -- schema discovery and clickhouse_health
```

`readonly = 2` lets the gateway send the limits in `clickhouse.settings`; `READONLY` stops queries from raising them.
With `readonly = 1` leave `settings` empty and put limits in the user's profile.

A few ClickHouse functions reach the network or disk without any grant (`hasColumnInTable` connects to any host,
for example). The gateway refuses them and every outward table function, but as a backstop also limit the ClickHouse
servers' own egress to what their engines and dictionaries need.

Keep sensitive columns out of reach with column grants (`GRANT SELECT(id, amount) ON shop.orders TO mcp_reader`) or row
policies. Name-based masking can be bypassed with an alias.

### Kafka

Read and Describe on topics, Describe on groups, nothing else. With Strimzi:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: mcp-reader
  labels: {strimzi.io/cluster: my-cluster}
spec:
  authentication: {type: scram-sha-512}
  authorization:
    type: simple
    acls:
      - resource: {type: topic, name: "*", patternType: literal}
        operations: [Read, Describe, DescribeConfigs]
      - resource: {type: group, name: "*", patternType: literal}
        operations: [Describe]
      - resource: {type: cluster}
        operations: [Describe]
```

The gateway never joins a group, so it needs no group Read.

## 2. Identity provider

The server is an OAuth resource server. It validates tokens from your OpenID Connect provider and publishes
[protected resource metadata](https://datatracker.ietf.org/doc/html/rfc9728) at
`/.well-known/oauth-protected-resource/mcp`, so clients find the provider on their own.

A token is accepted when it is signed by the issuer, unexpired, its `aud` contains `auth.audience`, and (if set) the
`groups_claim` contains `required_group`. The user named in audit logs comes from `username_claim`, then `sub`.

**Keycloak.** [scripts/keycloak_client.py](../scripts/keycloak_client.py) creates a public client with PKCE, an
audience mapper and a group mapper, plus the access group:

```bash
KEYCLOAK_ADMIN_USER=admin KEYCLOAK_ADMIN_PASSWORD=... python scripts/keycloak_client.py https://keycloak.example.com acme
```

Users then connect with a fixed client id:

```bash
claude mcp add --transport http --client-id dataplane-mcp --callback-port 47130 dataplane https://mcp.example.com/mcp
```

Clients that register themselves (Claude.ai, ChatGPT, VS Code) need dynamic client registration on the provider. In
Keycloak that is the realm's client registration policies; they also need the audience and group mappers, for example
through a default client scope.

**Other providers.** Any provider with OIDC discovery works. Set `audience` to what the provider puts in `aud` for your
API, and point `groups_claim` at wherever it puts groups or roles (`groups`, `roles`, `realm_access.roles`, a namespaced
claim, ...).

### No identity provider: behind an authenticating proxy

If your team already signs in through oauth2-proxy, Cloudflare Access, Tailscale or similar, leave `auth` out and name
the header the proxy sets with the user's identity:

```yaml
trusted_user_header: X-Auth-Request-Email          # oauth2-proxy
# trusted_user_header: Cf-Access-Authenticated-User-Email   # Cloudflare Access
# trusted_user_header: Tailscale-User-Login                 # Tailscale Serve
```

Requests to `/mcp` without the header get 401, and the header's value is the user in the audit log and in ClickHouse's
`log_comment`. The server trusts the header completely, so it must only be reachable through the proxy: bind it to
localhost or a private network the proxy alone can reach, and make the proxy strip the header from incoming requests.

## 3. Run it

Docker:

```bash
docker run -p 8080:8080 -v $PWD/config.yaml:/config/config.yaml:ro \
  -e CLICKHOUSE_PASSWORD -e KAFKA_PASSWORD ghcr.io/arogan178/dataplane-mcp
```

The image listens on 8080 in HTTP mode. Put TLS in front of it (ingress or load balancer) and set `public_url` to the
external address; requests with any other `Host` are refused.

On Kubernetes, mount the config from a ConfigMap and secrets from a Secret as environment variables, run as the image's
non-root user 1000, and probe `GET /healthz`.

Without `auth`, the server refuses to listen on anything but loopback unless you pass `--allow-unauthenticated`, which
is only for when a proxy in front authenticates every request.

## 4. Audit and metrics

Each tool call writes one JSON line to the `dataplane_mcp.audit` logger on stderr:

```json
{"user": "alice", "tool": "clickhouse_query", "args": {"env": "prod", "sql": "SELECT ..."}, "outcome": "ok", "ms": 30, "chars": 328}
```

ClickHouse queries also carry `log_comment = 'dataplane-mcp user=alice'`, so `system.query_log` shows who asked.

With `metrics: true` in the config, `GET /metrics` serves Prometheus metrics: `dataplane_mcp_tool_calls_total{tool, env, outcome}` and
`dataplane_mcp_tool_duration_seconds{tool}`. Like `/healthz` it needs no token and shows environment and tool names, so
expose it only to your monitoring network.
