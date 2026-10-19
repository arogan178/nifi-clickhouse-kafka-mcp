"""Create or update a public OAuth client and access group for nifi-clickhouse-kafka-mcp in a Keycloak realm.

    KEYCLOAK_ADMIN_USER=... KEYCLOAK_ADMIN_PASSWORD=... \\
        python keycloak_client.py https://keycloak.example.com my-realm [--client-id nifi-clickhouse-kafka-mcp] [--group mcp-users]

The base URL is what comes before /realms (include /auth on Keycloak 16 and older). The default redirect
URIs match `claude mcp add --client-id <id> --callback-port 47130`. Clients that register themselves
(Claude.ai, VS Code, ...) need Keycloak's dynamic client registration instead; see docs/deploying.md.
"""

import argparse
import os

import requests


def client(client_id: str, redirect_uris: list[str]) -> dict:
    return {
        "clientId": client_id,
        "name": f"{client_id} (MCP)",
        "enabled": True,
        "publicClient": True,
        "standardFlowEnabled": True,
        "implicitFlowEnabled": False,
        "directAccessGrantsEnabled": False,
        "serviceAccountsEnabled": False,
        "redirectUris": redirect_uris,
        "webOrigins": [],
        "attributes": {"pkce.code.challenge.method": "S256"},
        "protocolMappers": [
            {
                "name": "audience",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-audience-mapper",
                "config": {
                    "included.custom.audience": client_id,
                    "access.token.claim": "true",
                    "id.token.claim": "false",
                },
            },
            {
                "name": "groups",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-group-membership-mapper",
                "config": {
                    "claim.name": "groups",
                    "full.path": "false",
                    "access.token.claim": "true",
                    "id.token.claim": "false",
                    "userinfo.token.claim": "false",
                },
            },
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("base")
    parser.add_argument("realm")
    parser.add_argument("--client-id", default="nifi-clickhouse-kafka-mcp")
    parser.add_argument("--group", default="mcp-users")
    parser.add_argument("--callback-port", type=int, default=47130)
    args = parser.parse_args()
    base = args.base.rstrip("/")
    wanted = client(
        args.client_id,
        [f"http://localhost:{args.callback_port}/callback", f"http://127.0.0.1:{args.callback_port}/callback"],
    )

    token = requests.post(
        f"{base}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": os.environ["KEYCLOAK_ADMIN_USER"],
            "password": os.environ["KEYCLOAK_ADMIN_PASSWORD"],
        },
        timeout=30,
    )
    token.raise_for_status()
    api = requests.Session()
    api.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
    admin = f"{base}/admin/realms/{args.realm}"

    existing = api.get(f"{admin}/clients", params={"clientId": args.client_id}, timeout=30).json()
    if existing:
        client_uuid = existing[0]["id"]
        for mapper in api.get(f"{admin}/clients/{client_uuid}/protocol-mappers/models", timeout=30).json():
            api.delete(
                f"{admin}/clients/{client_uuid}/protocol-mappers/models/{mapper['id']}", timeout=30
            ).raise_for_status()
        settings = {k: v for k, v in wanted.items() if k != "protocolMappers"}
        api.put(f"{admin}/clients/{client_uuid}", json=settings, timeout=30).raise_for_status()
        for mapper in wanted["protocolMappers"]:
            api.post(
                f"{admin}/clients/{client_uuid}/protocol-mappers/models", json=mapper, timeout=30
            ).raise_for_status()
        print(f"updated client {args.client_id}")
    else:
        api.post(f"{admin}/clients", json=wanted, timeout=30).raise_for_status()
        print(f"created client {args.client_id}")

    groups = api.get(f"{admin}/groups", params={"search": args.group, "exact": "true"}, timeout=30).json()
    if any(g["name"] == args.group for g in groups):
        print(f"group {args.group} exists")
    else:
        api.post(f"{admin}/groups", json={"name": args.group}, timeout=30).raise_for_status()
        print(f"created group {args.group}")


if __name__ == "__main__":
    main()
