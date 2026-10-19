"""Give the nifi-clickhouse-kafka-mcp identity view-only access in a secured NiFi cluster.

Never give the gateway "proxy user requests": NiFi 2 authorizes a proxied request on the end user's
rights alone, so a proxying gateway could write wherever the user can.

The gateway identity must already exist (users.xml or your user group provider). Runs with a node's
certificate, acting as the initial admin through NiFi's proxy header:

    python3 nifi_grant.py https://<node>:8443/nifi-api node.pem node.key ca.pem "<initial admin>" "CN=nifi-clickhouse-kafka-mcp" [--data]

--data also grants view the data and view provenance on the root group, which the queue, FlowFile and
provenance tools need. These are read policies; nothing here grants modify.
"""

import argparse
import json
import ssl
import urllib.error
import urllib.request

POLICIES = [("read", "/flow"), ("read", "/parameter-contexts")]


class NiFiAdmin:
    def __init__(self, base, cert, key, ca, admin):
        self.base = base.rstrip("/")
        self.context = ssl.create_default_context(cafile=ca)
        self.context.load_cert_chain(cert, key)
        self.admin = admin

    def call(self, method, path, body=None):
        request = urllib.request.Request(
            f"{self.base}{path}",
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"X-ProxiedEntitiesChain": f"<{self.admin}>", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, context=self.context, timeout=30) as response:
                return json.loads(response.read() or b"null")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise RuntimeError(f"{method} {path}: {e.code} {e.read().decode()[:500]}") from e

    def user_id(self, identity):
        for user in self.call("GET", "/tenants/users")["users"]:
            if user["component"]["identity"] == identity:
                return user["id"]
        raise SystemExit(f"{identity} is not a NiFi user yet; add it to your user group provider first")

    def grant(self, action, resource, user_id, identity):
        policy = self.call("GET", f"/policies/{action}{resource}")
        # NiFi returns the nearest inherited policy when none exists for this exact resource.
        if policy is None or policy["component"]["resource"] != resource:
            self.call(
                "POST",
                "/policies",
                {
                    "revision": {"version": 0},
                    "component": {"action": action, "resource": resource, "users": [{"id": user_id}]},
                },
            )
            print(f"created {action} {resource} for {identity}")
            return
        users = policy["component"]["users"]
        if any(u["id"] == user_id for u in users):
            return
        policy["component"]["users"] = users + [{"id": user_id}]
        self.call(
            "PUT", f"/policies/{policy['id']}", {"revision": policy["revision"], "component": policy["component"]}
        )
        print(f"granted {action} {resource} to {identity}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("base")
    parser.add_argument("cert")
    parser.add_argument("key")
    parser.add_argument("ca")
    parser.add_argument("admin")
    parser.add_argument("gateway")
    parser.add_argument("--data", action="store_true")
    args = parser.parse_args()

    nifi = NiFiAdmin(args.base, args.cert, args.key, args.ca, args.admin)
    root = f"/process-groups/{nifi.call('GET', '/flow/process-groups/root')['processGroupFlow']['id']}"
    user_id = nifi.user_id(args.gateway)
    policies = POLICIES + [("read", root)]
    if args.data:
        policies += [("read", f"/data{root}"), ("read", "/provenance"), ("read", f"/provenance-data{root}")]
    for action, resource in policies:
        nifi.grant(action, resource, user_id, args.gateway)


if __name__ == "__main__":
    main()
