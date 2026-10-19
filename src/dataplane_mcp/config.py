import os
import re
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class NiFiMTLS(Model):
    type: Literal["mtls"]
    cert: str
    key: str


class NiFiPassword(Model):
    type: Literal["password"]
    username: str
    password: str


class NiFiToken(Model):
    type: Literal["token"]
    token: str


class NiFiNoAuth(Model):
    type: Literal["none"]


class NiFiConfig(Model):
    url: str
    auth: Annotated[NiFiMTLS | NiFiPassword | NiFiToken | NiFiNoAuth, Field(discriminator="type")]
    ca_bundle: str | None = None
    verify_tls: bool = True
    timeout_seconds: float = 30


class ClickHouseConfig(Model):
    url: str
    user: str = "default"
    password: str = ""
    database: str | None = None
    ca_bundle: str | None = None
    verify_tls: bool = True
    cluster: str | None = None
    max_rows: int = 1000
    timeout_seconds: float = 120
    # Sent with every query, e.g. max_execution_time. Leave empty for a readonly=1 user, which can't change settings.
    settings: dict[str, str | int | float] = {}
    # Extra table functions to allow beyond the internal ones (numbers, merge, clusterAllReplicas, ...).
    allowed_table_functions: list[str] = []


class SchemaRegistryConfig(Model):
    url: str
    username: str | None = None
    password: str | None = None
    ca_bundle: str | None = None


class KafkaConfig(Model):
    # librdkafka properties, passed through unchanged: bootstrap.servers, security.protocol, sasl.*, ssl.*
    config: dict[str, str | int | bool]
    schema_registry: SchemaRegistryConfig | None = None

    @model_validator(mode="after")
    def _needs_bootstrap(self):
        if "bootstrap.servers" not in self.config:
            raise ValueError("kafka.config needs bootstrap.servers")
        forbidden = {"group.id", "enable.auto.commit"} & set(self.config)
        if forbidden:
            raise ValueError(f"kafka.config must not set {', '.join(sorted(forbidden))}; the gateway never commits")
        return self


class Environment(Model):
    description: str | None = None
    nifi: NiFiConfig | None = None
    clickhouse: ClickHouseConfig | None = None
    kafka: KafkaConfig | None = None


class AuthConfig(Model):
    issuer: str
    audience: str
    required_group: str | None = None
    groups_claim: str = "groups"
    username_claim: str = "preferred_username"


class Config(Model):
    public_url: str | None = None
    auth: AuthConfig | None = None
    masked_fields: frozenset[str] = frozenset()
    disabled_tools: frozenset[str] = frozenset()
    max_result_chars: int = 400_000
    # Binary payloads can't be masked, so they are described rather than returned unless this is on.
    show_binary: bool = False
    environments: dict[str, Environment]

    @model_validator(mode="after")
    def _check(self):
        if not self.environments:
            raise ValueError("define at least one environment")
        if self.auth and not self.public_url:
            raise ValueError("auth needs public_url, the address clients use to reach the server")
        return self


def expand(value, path: str = ""):
    """Replace ${VAR} and ${VAR:-default} in every string, so secrets stay out of the file."""
    if isinstance(value, dict):
        return {k: expand(v, f"{path}.{k}" if path else str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [expand(v, f"{path}[{i}]") for i, v in enumerate(value)]
    if not isinstance(value, str):
        return value

    def substitute(match: re.Match) -> str:
        name, default = match[1], match[2]
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise ValueError(f"{path}: environment variable {name} is not set")

    return ENV_REF.sub(substitute, value)


def load(path: str | Path) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    raw = expand(raw)
    raw["masked_fields"] = [f.lower() for f in raw.get("masked_fields") or []]
    return Config.model_validate(raw)
