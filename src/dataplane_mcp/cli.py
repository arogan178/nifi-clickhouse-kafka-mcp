import argparse
import logging
import os
import sys
from urllib.parse import urlparse

from mcp.server.transport_security import TransportSecuritySettings

from . import config
from .server import build_server, package_version

LOOPBACK = {"127.0.0.1", "localhost", "::1"}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="dataplane-mcp", description="Read-only MCP server for NiFi, ClickHouse and Kafka"
    )
    parser.add_argument("--config", default=os.environ.get("DATAPLANE_MCP_CONFIG", "config.yaml"))
    parser.add_argument(
        "--transport", choices=["stdio", "http"], default=os.environ.get("DATAPLANE_MCP_TRANSPORT", "stdio")
    )
    parser.add_argument("--host", default=os.environ.get("DATAPLANE_MCP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DATAPLANE_MCP_PORT", "8080")))
    parser.add_argument(
        "--allow-unauthenticated",
        action="store_true",
        help="serve HTTP without auth on a non-loopback address, e.g. behind an authenticating proxy",
    )
    parser.add_argument("--check", action="store_true", help="validate the config, list the tools and exit")
    parser.add_argument("--version", action="version", version=package_version())
    args = parser.parse_args(argv)

    # stdout carries the protocol in stdio mode, so logs always go to stderr.
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    try:
        cfg = config.load(args.config)
    except (OSError, ValueError) as e:
        sys.exit(f"dataplane-mcp: {args.config}: {e}")
    try:
        server = build_server(cfg)
    except OSError as e:
        # Missing or unreadable certificate and key files.
        sys.exit(f"dataplane-mcp: {e.strerror or e}: {e.filename or ''}")

    if args.check:
        for name, env in cfg.environments.items():
            systems = [s for s in ("nifi", "clickhouse", "kafka") if getattr(env, s) is not None]
            print(f"{name}: {', '.join(systems)}")
        print(f"tools: {', '.join(server.toolbox.registered)}")
        return

    if args.transport == "stdio":
        server.run("stdio")
        return

    if not cfg.auth and args.host not in LOOPBACK and not args.allow_unauthenticated:
        sys.exit(
            f"dataplane-mcp: refusing to serve HTTP on {args.host} without auth; configure auth, "
            "bind to 127.0.0.1, or pass --allow-unauthenticated if a proxy authenticates callers"
        )
    security = None
    if cfg.public_url:
        security = TransportSecuritySettings(allowed_hosts=[urlparse(cfg.public_url).netloc])
    server.run(
        "streamable-http",
        host=args.host,
        port=args.port,
        # Plain JSON replies: some proxies cut long-lived SSE response streams.
        json_response=True,
        transport_security=security,
    )


if __name__ == "__main__":
    main()
