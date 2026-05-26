"""Tests for the OIDC JWT verifier.

The JwksHarness fixture starts a real local HTTP server on 127.0.0.1:<random>,
generates an RSA keypair, and serves a JWKS document at /connect/jwks. Tests
sign JWTs against this private key and verify them through JwtVerifier; the
verifier fetches the JWKS over the loopback HTTP path, exercising the same
code path that runs in production.
"""

import json
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from qiita_common.api_paths import LOOPBACK_HOST


@pytest.fixture(autouse=True)
def _workspace_root_env(monkeypatch):
    """Settings.from_env() (called by several AuthRocket tests below) now
    requires WORK_TICKET_WORKSPACE_ROOT, UPLOAD_STAGING_ROOT, and
    CONTACT_EMAIL. Set defaults so those tests can focus on the OIDC
    surface they actually care about."""
    monkeypatch.setenv("WORK_TICKET_WORKSPACE_ROOT", "/tmp/qiita-test-ws-unused")
    monkeypatch.setenv("UPLOAD_STAGING_ROOT", "/tmp/qiita-test-staging-unused")
    monkeypatch.setenv("CONTACT_EMAIL", "qiita-test@example.org")


# ---------------------------------------------------------------------------
# JWKS harness
# ---------------------------------------------------------------------------


class JwksHarness:
    """Local HTTP server that serves a JWKS document and signs JWTs.

    Use as a context manager or via the `jwks_harness` pytest fixture.
    Counts JWKS fetches so tests can assert caching/refresh behavior.
    """

    def __init__(self) -> None:
        self.fetch_count = 0
        self._lock = threading.Lock()
        self._private_key = self._gen_key()
        self._kid = f"kid-{secrets.token_hex(4)}"
        self._jwks = self._build_jwks(self._private_key, self._kid)
        self._server = HTTPServer((LOOPBACK_HOST, 0), self._make_handler())
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def issuer(self) -> str:
        return f"http://{LOOPBACK_HOST}:{self.port}"

    @property
    def jwks_url(self) -> str:
        return f"{self.issuer}/connect/jwks"

    def sign(
        self,
        claims: dict,
        *,
        kid: str | None = None,
        key=None,
    ) -> str:
        """Sign a JWT with the harness's private key (or a caller-supplied one)."""
        return jwt.encode(
            claims,
            key or self._private_key,
            algorithm="RS256",
            headers={"kid": kid or self._kid},
        )

    def rotate_key(self) -> str:
        """Replace the served key. Returns the new kid."""
        with self._lock:
            self._private_key = self._gen_key()
            self._kid = f"kid-{secrets.token_hex(4)}"
            self._jwks = self._build_jwks(self._private_key, self._kid)
        return self._kid

    @property
    def current_kid(self) -> str:
        return self._kid

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)

    @staticmethod
    def _gen_key():
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)

    @staticmethod
    def _build_jwks(private_key, kid: str) -> dict:
        public_jwk = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
        return {"keys": [{**public_jwk, "kid": kid, "alg": "RS256", "use": "sig"}]}

    def _make_handler(self):
        harness = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/connect/jwks":
                    with harness._lock:
                        harness.fetch_count += 1
                        body = json.dumps(harness._jwks).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_error(404)

            def log_message(self, *args, **kwargs):
                # Silence the access log; tests assert via fetch_count.
                pass

        return Handler


@pytest.fixture
def jwks_harness():
    h = JwksHarness()
    yield h
    h.shutdown()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# Bound the JWT `aud` claim and the verifier's `audience` arg together — if the
# two drift, audience-binding tests pass for the wrong reason. Centralizing
# also lets the LoginRocket Web tests deliberately leave the claim absent
# without confusing this constant for "not configured."
_TEST_AUDIENCE = "test-audience"


def _claims(jwks_harness, **overrides) -> dict:
    """Default claim set; override per test.

    Includes the historical OIDC-strict shape (`aud`, `email_verified`,
    `auth_time`-eligible) so legacy-strict tests keep working. Tests that
    exercise the LoginRocket Web shape pop these via `_pop` or pass
    overrides like `aud=None` and then strip the key before signing.
    """
    now = int(time.time())
    base = {
        "iss": jwks_harness.issuer,
        "aud": _TEST_AUDIENCE,
        "sub": "test-subject-123",
        "email": "alice@example.com",
        "email_verified": True,
        "iat": now,
        "exp": now + 3600,
    }
    base.update(overrides)
    return base


def _loginrocket_claims(jwks_harness, **overrides) -> dict:
    """Default claim set in the LoginRocket Web shape — no `aud`, no
    `email_verified`, no `auth_time`. Used by tests that exercise the
    softened verifier path."""
    now = int(time.time())
    base = {
        "iss": jwks_harness.issuer,
        "sub": "lr-subject-123",
        "email": "lr-test@example.com",
        "iat": now,
        "exp": now + 3600,
    }
    base.update(overrides)
    return base


def _verifier(jwks_harness, *, audience: str | None = _TEST_AUDIENCE, leeway: int = 30):
    from qiita_control_plane.auth.oidc import JwtVerifier

    return JwtVerifier(
        jwks_url=jwks_harness.jwks_url,
        issuer=jwks_harness.issuer,
        audience=audience,
        leeway_seconds=leeway,
    )


# ---------------------------------------------------------------------------
# Happy-path
# ---------------------------------------------------------------------------


def test_verify_valid_jwt_returns_identity(jwks_harness):
    token = jwks_harness.sign(_claims(jwks_harness))
    identity = _verifier(jwks_harness).verify(token)
    assert identity.issuer == jwks_harness.issuer
    assert identity.subject == "test-subject-123"
    assert identity.email == "alice@example.com"
    assert identity.auth_time is None


def test_verify_returns_auth_time_when_present(jwks_harness):
    auth_time = int(time.time()) - 30
    token = jwks_harness.sign(_claims(jwks_harness, auth_time=auth_time))
    identity = _verifier(jwks_harness).verify(token)
    assert identity.auth_time == auth_time


# ---------------------------------------------------------------------------
# Rejection cases
# ---------------------------------------------------------------------------


def test_verify_rejects_expired_jwt(jwks_harness):
    from qiita_control_plane.auth.oidc import InvalidJwt

    now = int(time.time())
    token = jwks_harness.sign(_claims(jwks_harness, exp=now - 3600))
    with pytest.raises(InvalidJwt):
        _verifier(jwks_harness).verify(token)


def test_verify_rejects_bad_signature(jwks_harness):
    from qiita_control_plane.auth.oidc import InvalidJwt

    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    # Sign with an unrelated key but include the harness's kid header so the
    # JWKS lookup succeeds; the signature verify should fail.
    token = jwks_harness.sign(_claims(jwks_harness), key=other_key)
    with pytest.raises(InvalidJwt):
        _verifier(jwks_harness).verify(token)


def test_verify_rejects_wrong_audience(jwks_harness):
    from qiita_control_plane.auth.oidc import InvalidJwt

    token = jwks_harness.sign(_claims(jwks_harness, aud="some-other-aud"))
    with pytest.raises(InvalidJwt):
        _verifier(jwks_harness).verify(token)


def test_verify_rejects_wrong_issuer(jwks_harness):
    from qiita_control_plane.auth.oidc import InvalidJwt, JwtVerifier

    token = jwks_harness.sign(_claims(jwks_harness))
    v = JwtVerifier(
        jwks_url=jwks_harness.jwks_url,
        issuer="http://different-issuer.example",
        audience=_TEST_AUDIENCE,
        leeway_seconds=30,
    )
    with pytest.raises(InvalidJwt):
        v.verify(token)


def test_verify_rejects_missing_email(jwks_harness):
    from qiita_control_plane.auth.oidc import InvalidJwt

    claims = _claims(jwks_harness)
    del claims["email"]
    token = jwks_harness.sign(claims)
    with pytest.raises(InvalidJwt, match="email"):
        _verifier(jwks_harness).verify(token)


def test_verify_accepts_token_without_email_verified(jwks_harness):
    """LoginRocket Web tokens omit `email_verified`. The verifier no longer
    requires it — the realm is responsible for enforcing email verification
    at signup, and we trust the issuer rather than re-checking the claim."""
    claims = _claims(jwks_harness)
    del claims["email_verified"]
    token = jwks_harness.sign(claims)
    identity = _verifier(jwks_harness).verify(token)
    assert identity.email == "alice@example.com"


def test_verify_accepts_token_with_email_verified_false(jwks_harness):
    """No longer rejected at verifier level. The realm-side policy is the
    gate; if the IdP issues a token with `email_verified=false`, that's an
    operator-policy issue (the realm should refuse to issue at all)."""
    token = jwks_harness.sign(_claims(jwks_harness, email_verified=False))
    identity = _verifier(jwks_harness).verify(token)
    assert identity.email == "alice@example.com"


def test_verify_rejects_missing_sub(jwks_harness):
    from qiita_control_plane.auth.oidc import InvalidJwt

    claims = _claims(jwks_harness)
    del claims["sub"]
    token = jwks_harness.sign(claims)
    with pytest.raises(InvalidJwt):
        _verifier(jwks_harness).verify(token)


def test_verify_rejects_hs256_token(jwks_harness):
    """Algorithm-confusion regression guard: even a JWT whose signature would
    verify against an attacker-supplied symmetric key must be rejected because
    the verifier pins algorithms=["RS256"]."""
    from qiita_control_plane.auth.oidc import InvalidJwt

    # Sign with HS256 using an attacker-chosen secret. PyJWT must refuse to
    # validate this against the JwtVerifier's RSA public key because the
    # algorithm allowlist is RS256-only.
    token = jwt.encode(
        _claims(jwks_harness),
        "attacker-supplied-secret",
        algorithm="HS256",
        headers={"kid": jwks_harness.current_kid},
    )
    with pytest.raises(InvalidJwt):
        _verifier(jwks_harness).verify(token)


# ---------------------------------------------------------------------------
# Leeway
# ---------------------------------------------------------------------------


def test_verify_honors_leeway_seconds(jwks_harness):
    """A JWT expired by 5s within a 30s leeway window verifies."""
    now = int(time.time())
    token = jwks_harness.sign(_claims(jwks_harness, exp=now - 5))
    identity = _verifier(jwks_harness, leeway=30).verify(token)
    assert identity.email == "alice@example.com"


def test_verify_rejects_beyond_leeway(jwks_harness):
    from qiita_control_plane.auth.oidc import InvalidJwt

    now = int(time.time())
    token = jwks_harness.sign(_claims(jwks_harness, exp=now - 60))
    with pytest.raises(InvalidJwt):
        _verifier(jwks_harness, leeway=30).verify(token)


# ---------------------------------------------------------------------------
# Audience array handling
# ---------------------------------------------------------------------------


def test_verify_accepts_audience_as_string_matching_configured_aud(jwks_harness):
    token = jwks_harness.sign(_claims(jwks_harness, aud=_TEST_AUDIENCE))
    _verifier(jwks_harness).verify(token)


def test_verify_accepts_audience_as_list_containing_configured_aud(jwks_harness):
    token = jwks_harness.sign(_claims(jwks_harness, aud=["other-aud", _TEST_AUDIENCE, "third"]))
    _verifier(jwks_harness).verify(token)


def test_verify_rejects_audience_as_list_without_configured_aud(jwks_harness):
    from qiita_control_plane.auth.oidc import InvalidJwt

    token = jwks_harness.sign(_claims(jwks_harness, aud=["x", "y"]))
    with pytest.raises(InvalidJwt):
        _verifier(jwks_harness).verify(token)


# ---------------------------------------------------------------------------
# JWKS caching & rotation
# ---------------------------------------------------------------------------


def test_verifier_caches_jwks_across_calls(jwks_harness):
    """Multiple verifies with the same kid should fetch JWKS at most once
    (PyJWKClient's in-process cache; no network hit on repeat)."""
    v = _verifier(jwks_harness)
    for _ in range(5):
        token = jwks_harness.sign(_claims(jwks_harness))
        v.verify(token)
    assert jwks_harness.fetch_count == 1


def test_verifier_refreshes_jwks_on_unknown_kid(jwks_harness):
    """Rotating the harness's key forces PyJWKClient to refetch JWKS on
    the next verify (the new kid isn't in the cache)."""
    v = _verifier(jwks_harness)
    token1 = jwks_harness.sign(_claims(jwks_harness))
    v.verify(token1)
    fetches_before = jwks_harness.fetch_count
    old_kid = jwks_harness.current_kid

    new_kid = jwks_harness.rotate_key()
    assert new_kid != old_kid  # sanity: rotation actually changed the kid
    token2 = jwks_harness.sign(_claims(jwks_harness))
    v.verify(token2)

    assert jwks_harness.fetch_count > fetches_before


# ---------------------------------------------------------------------------
# Construction / fail-fast
# ---------------------------------------------------------------------------


def test_verifier_rejects_construction_with_empty_url():
    from qiita_control_plane.auth.oidc import JwtVerifier

    with pytest.raises(ValueError, match="jwks_url"):
        JwtVerifier(jwks_url="", issuer="x", audience="y", leeway_seconds=30)


def test_verifier_rejects_construction_with_empty_issuer():
    from qiita_control_plane.auth.oidc import JwtVerifier

    with pytest.raises(ValueError, match="issuer"):
        JwtVerifier(jwks_url="http://x", issuer="", audience="y", leeway_seconds=30)


def test_verifier_normalizes_empty_audience_to_none():
    """An empty-string audience is treated as None (audience-skipped) rather
    than verify-against-empty-string. Matches Settings.from_env's `or None`
    pattern; otherwise an empty AUTHROCKET_AUDIENCE env var would silently
    misconfigure the verifier.
    """
    from qiita_control_plane.auth.oidc import JwtVerifier

    v = JwtVerifier(jwks_url="http://x", issuer="y", audience="", leeway_seconds=30)
    assert v.audience is None


def test_verifier_constructs_with_audience_none():
    """LoginRocket Web realms have no `aud` claim; constructing with
    audience=None is the supported way to skip audience binding."""
    from qiita_control_plane.auth.oidc import JwtVerifier

    v = JwtVerifier(jwks_url="http://x", issuer="y", audience=None, leeway_seconds=30)
    assert v.audience is None


def test_authrocket_verifier_fails_fast_on_missing_settings(monkeypatch):
    """from_settings raises when required AUTHROCKET_* env vars aren't populated.
    AUTHROCKET_AUDIENCE is no longer required (LoginRocket Web emits no `aud`)."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    import base64
    import secrets as _secrets

    monkeypatch.setenv(
        "HMAC_SECRET_KEY",
        base64.b64encode(_secrets.token_bytes(32)).decode(),
    )
    monkeypatch.delenv("AUTHROCKET_ISSUER", raising=False)
    monkeypatch.delenv("AUTHROCKET_AUDIENCE", raising=False)
    monkeypatch.delenv("AUTHROCKET_JWKS_URL", raising=False)

    from qiita_control_plane.auth.oidc import AuthRocketVerifier
    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    with pytest.raises(RuntimeError, match="missing env"):
        AuthRocketVerifier.from_settings(settings)


def test_authrocket_verifier_constructs_when_all_env_set(monkeypatch, jwks_harness):
    """Sanity: with valid env, from_settings returns a working verifier.
    Includes AUTHROCKET_AUDIENCE for the OIDC-with-aud case (e.g. an OAuth2
    Server integration on a higher AuthRocket plan tier)."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    import base64
    import secrets as _secrets

    monkeypatch.setenv(
        "HMAC_SECRET_KEY",
        base64.b64encode(_secrets.token_bytes(32)).decode(),
    )
    monkeypatch.setenv("AUTHROCKET_ISSUER", jwks_harness.issuer)
    monkeypatch.setenv("AUTHROCKET_AUDIENCE", _TEST_AUDIENCE)
    monkeypatch.setenv("AUTHROCKET_JWKS_URL", jwks_harness.jwks_url)

    from qiita_control_plane.auth.oidc import AuthRocketVerifier
    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    v = AuthRocketVerifier.from_settings(settings)

    token = jwks_harness.sign(_claims(jwks_harness))
    identity = v.verify(token)
    assert identity.email == "alice@example.com"


def test_authrocket_verifier_constructs_when_audience_unset(monkeypatch, jwks_harness):
    """LoginRocket Web realm: AUTHROCKET_AUDIENCE unset, verifier still
    constructs and accepts tokens that lack the `aud` claim."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    import base64
    import secrets as _secrets

    monkeypatch.setenv(
        "HMAC_SECRET_KEY",
        base64.b64encode(_secrets.token_bytes(32)).decode(),
    )
    monkeypatch.setenv("AUTHROCKET_ISSUER", jwks_harness.issuer)
    monkeypatch.delenv("AUTHROCKET_AUDIENCE", raising=False)
    monkeypatch.setenv("AUTHROCKET_JWKS_URL", jwks_harness.jwks_url)

    from qiita_control_plane.auth.oidc import AuthRocketVerifier
    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    v = AuthRocketVerifier.from_settings(settings)
    assert v.audience is None

    token = jwks_harness.sign(_loginrocket_claims(jwks_harness))
    identity = v.verify(token)
    assert identity.email == "lr-test@example.com"
    assert identity.auth_time is None


# ---------------------------------------------------------------------------
# LoginRocket Web token shape (no aud, no email_verified, no auth_time)
# ---------------------------------------------------------------------------


def test_verify_accepts_token_without_aud_when_audience_is_none(jwks_harness):
    """The headline LoginRocket Web case: token has no `aud`, verifier
    constructed with audience=None accepts it."""
    token = jwks_harness.sign(_loginrocket_claims(jwks_harness))
    identity = _verifier(jwks_harness, audience=None).verify(token)
    assert identity.email == "lr-test@example.com"
    assert identity.auth_time is None


def test_verify_rejects_token_without_aud_when_audience_is_set(jwks_harness):
    """Conversely: when audience IS configured, a token without `aud` is
    still rejected. The softening is opt-in, not always-on."""
    from qiita_control_plane.auth.oidc import InvalidJwt

    token = jwks_harness.sign(_loginrocket_claims(jwks_harness))
    with pytest.raises(InvalidJwt):
        _verifier(jwks_harness, audience=_TEST_AUDIENCE).verify(token)


def test_verify_accepts_token_without_auth_time(jwks_harness):
    """auth_time is no longer used as a freshness anchor; absence is fine.
    OIDCIdentity.auth_time should be None."""
    token = jwks_harness.sign(_loginrocket_claims(jwks_harness))
    identity = _verifier(jwks_harness, audience=None).verify(token)
    assert identity.auth_time is None


def test_verify_propagates_auth_time_when_present(jwks_harness):
    """If a realm does emit auth_time (OAuth2 Server integration), it's
    surfaced unchanged for callers that want it."""
    auth_time = int(time.time()) - 30
    token = jwks_harness.sign(_loginrocket_claims(jwks_harness, auth_time=auth_time))
    identity = _verifier(jwks_harness, audience=None).verify(token)
    assert identity.auth_time == auth_time
