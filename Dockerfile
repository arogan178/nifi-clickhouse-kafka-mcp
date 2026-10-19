FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN uv sync --frozen --no-dev --extra avro && useradd -m -u 1000 mcp

# Numeric so Kubernetes can verify runAsNonRoot.
USER 1000
ENV PATH="/app/.venv/bin:$PATH" PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    DATAPLANE_MCP_TRANSPORT=http DATAPLANE_MCP_HOST=0.0.0.0 DATAPLANE_MCP_CONFIG=/config/config.yaml
EXPOSE 8080
ENTRYPOINT ["dataplane-mcp"]
