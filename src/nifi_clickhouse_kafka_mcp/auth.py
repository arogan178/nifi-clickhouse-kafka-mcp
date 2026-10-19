import json
import logging
import time
import urllib.request

import anyio
import jwt
from mcp.server.auth.provider import AccessToken

from .config import AuthConfig

log = logging.getLogger(__name__)
JWKS_REFRESH_SECONDS = 60
ALGORITHMS = ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512"]


def _claim(claims: dict, path: str):
    value = claims
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


class OIDCVerifier:
    """Validates bearer tokens from any OpenID Connect provider (Keycloak, Entra ID, Okta, Auth0, Dex, ...)."""

    def __init__(self, cfg: AuthConfig, resource: str):
        self.cfg = cfg
        # Compared with iss exactly as configured: some providers (Auth0) end it with a slash.
        self.issuer = cfg.issuer
        self.resource = resource
        self._jwks: jwt.PyJWKClient | None = None
        # -inf, not 0: monotonic time starts at boot, so a fresh container would wait a minute for its first refresh.
        self._refreshed = float("-inf")

    def jwks(self) -> jwt.PyJWKClient:
        if self._jwks is None:
            url = f"{self.issuer.rstrip('/')}/.well-known/openid-configuration"
            with urllib.request.urlopen(url, timeout=15) as response:
                jwks_uri = json.load(response)["jwks_uri"]
            self._jwks = jwt.PyJWKClient(jwks_uri, cache_keys=True)
        return self._jwks

    def signing_key(self, token: str) -> jwt.PyJWK:
        kid = jwt.get_unverified_header(token).get("kid")
        keys = {k.key_id: k for k in self.jwks().get_signing_keys()}
        # Unknown key ids refetch the key set at most once a minute, so bad tokens can't flood the provider.
        if kid not in keys and time.monotonic() - self._refreshed > JWKS_REFRESH_SECONDS:
            self._refreshed = time.monotonic()
            keys = {k.key_id: k for k in self.jwks().get_signing_keys(refresh=True)}
        if kid not in keys:
            raise jwt.PyJWKClientError(f"unknown signing key {kid!r}")
        return keys[kid]

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            key = await anyio.to_thread.run_sync(self.signing_key, token)
            claims = jwt.decode(
                token,
                key.key,
                algorithms=ALGORITHMS,
                audience=self.cfg.audience,
                issuer=self.issuer,
                options={"require": ["exp", "iss", "aud"]},
            )
        except jwt.PyJWTError:
            return None
        except Exception:
            # An unreachable identity provider must reject the request, not fail it.
            log.exception("could not verify token with %s", self.issuer)
            return None
        username = _claim(claims, self.cfg.username_claim) or claims.get("sub")
        if not username:
            return None
        if self.cfg.required_group:
            groups = _claim(claims, self.cfg.groups_claim) or []
            if isinstance(groups, str):
                groups = groups.split()
            # Keycloak's group mapper emits full paths ("/group") unless "Full group path" is off.
            if self.cfg.required_group not in {str(g).lstrip("/") for g in groups}:
                return None
        return AccessToken(
            token=token,
            client_id=claims.get("azp") or claims.get("client_id") or "",
            scopes=(claims.get("scope") or "").split(),
            expires_at=claims["exp"],
            resource=self.resource,
            subject=str(username),
            claims=claims,
        )
