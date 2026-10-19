import time

import anyio
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from dataplane_mcp.auth import OIDCVerifier
from dataplane_mcp.config import AuthConfig

ISSUER = "https://idp.example/realms/acme"
RESOURCE = "https://mcp.example/mcp"
KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


class StaticKeys:
    class Key:
        key = KEY.public_key()
        key_id = None

    fetches = 0

    def get_signing_keys(self, refresh=False):
        self.fetches += refresh
        return [self.Key()]


def verifier():
    v = OIDCVerifier(AuthConfig(issuer=ISSUER, audience="dataplane-mcp", required_group="mcp-users"), RESOURCE)
    v._jwks = StaticKeys()
    return v


def token(**overrides):
    claims = {
        "iss": ISSUER,
        "aud": ["dataplane-mcp", "account"],
        "exp": int(time.time()) + 300,
        "preferred_username": "some.engineer",
        "groups": ["mcp-users"],
        "azp": "dataplane-mcp",
    }
    claims.update(overrides)
    return jwt.encode({k: v for k, v in claims.items() if v is not None}, KEY, algorithm="RS256")


def verify(raw):
    return anyio.run(verifier().verify_token, raw)


def test_accepts_member_and_names_the_engineer():
    access = verify(token())
    assert access.subject == "some.engineer"
    assert access.resource == RESOURCE


def test_accepts_full_group_path():
    assert verify(token(groups=["/mcp-users"])).subject == "some.engineer"


@pytest.mark.parametrize(
    "overrides",
    [
        {"groups": ["other-team"]},
        {"groups": None},
        {"aud": "account"},
        {"iss": "https://evil.example/realms/acme"},
        {"exp": int(time.time()) - 10},
        {"preferred_username": None},
    ],
)
def test_rejects(overrides):
    assert verify(token(**overrides)) is None


def test_rejects_token_signed_by_another_key():
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged = jwt.encode(
        {
            "iss": ISSUER,
            "aud": "dataplane-mcp",
            "exp": int(time.time()) + 300,
            "preferred_username": "x",
            "groups": ["mcp-users"],
        },
        other,
        algorithm="RS256",
    )
    assert verify(forged) is None


def test_nested_groups_claim_and_sub_fallback():
    v = OIDCVerifier(
        AuthConfig(
            issuer=ISSUER,
            audience="dataplane-mcp",
            required_group="mcp-users",
            groups_claim="realm_access.roles",
            username_claim="email",
        ),
        RESOURCE,
    )
    v._jwks = StaticKeys()
    raw = token(groups=None, preferred_username=None, sub="u-1", realm_access={"roles": ["mcp-users"]})
    assert anyio.run(v.verify_token, raw).subject == "u-1"


def test_no_required_group_accepts_any_valid_token():
    v = OIDCVerifier(AuthConfig(issuer=ISSUER, audience="dataplane-mcp"), RESOURCE)
    v._jwks = StaticKeys()
    assert anyio.run(v.verify_token, token(groups=None)).subject == "some.engineer"


def test_unreachable_identity_provider_rejects():
    v = OIDCVerifier(AuthConfig(issuer="http://127.0.0.1:9", audience="dataplane-mcp"), RESOURCE)
    assert anyio.run(v.verify_token, token()) is None


def test_issuer_with_trailing_slash_is_compared_as_configured():
    v = OIDCVerifier(AuthConfig(issuer=ISSUER + "/", audience="dataplane-mcp"), RESOURCE)
    v._jwks = StaticKeys()
    assert anyio.run(v.verify_token, token(iss=ISSUER + "/")).subject == "some.engineer"
    assert anyio.run(v.verify_token, token()) is None


def test_unknown_key_ids_refetch_at_most_once_a_minute():
    v = verifier()
    for _ in range(5):
        forged = jwt.encode({"iss": ISSUER}, KEY, algorithm="RS256", headers={"kid": "nope"})
        assert anyio.run(v.verify_token, forged) is None
    assert v._jwks.fetches == 1
