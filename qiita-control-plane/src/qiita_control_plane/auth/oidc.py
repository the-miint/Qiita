"""OIDC JWT verification.

JwtVerifier is the pure verifier — takes JWKS URL, issuer, audience, and
leeway. AuthRocketVerifier is the config-bound wrapper that constructs from
a Settings object. Tests instantiate JwtVerifier directly against a local
JWKS harness; production wires AuthRocketVerifier from Settings at lifespan.

We accept only RS256 (the algorithm AuthRocket uses) and require:
  - signature verifies against the JWKS-fetched key matching the JWT's `kid`
  - `iss` matches the configured issuer
  - `aud` contains the configured audience (str or list per OIDC spec)
  - `exp` is in the future (within leeway)
  - `email_verified` is **boolean True** — the string "true" is rejected
    because IdPs that emit coerced strings haven't been audited
  - `email` and `sub` claims are present

`auth_time` is optional at the verifier level — returned as a property of
the OIDCIdentity (or None if absent). Callers that need freshness
(POST /auth/pat) check `now - auth_time` against their own threshold.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import jwt
from jwt import InvalidTokenError, PyJWKClient


class InvalidJwt(Exception):
    """Raised when a JWT fails any verification check."""


@dataclass(frozen=True, slots=True)
class OIDCIdentity:
    issuer: str
    subject: str
    email: str
    auth_time: int | None  # epoch seconds, or None if claim absent
    raw_claims: dict[str, Any]


class JwtVerifier:
    """Pure JWKS-backed JWT verifier for AuthRocket-style OIDC tokens.

    The PyJWKClient caches keys in memory (5-minute lifespan by default) and
    refreshes on encountering an unknown `kid` — this handles IdP key rotation
    without requiring a redeploy. Tests inject a local JWKS server via this
    class; production uses AuthRocketVerifier (below).
    """

    def __init__(
        self,
        *,
        jwks_url: str,
        issuer: str,
        audience: str,
        leeway_seconds: int = 30,
    ) -> None:
        if not jwks_url:
            raise ValueError("jwks_url is required")
        if not issuer:
            raise ValueError("issuer is required")
        if not audience:
            raise ValueError("audience is required")
        self.jwks_url = jwks_url
        self.issuer = issuer
        self.audience = audience
        self.leeway_seconds = leeway_seconds
        self._jwks_client = PyJWKClient(jwks_url)

    def verify(self, token: str) -> OIDCIdentity:
        """Verify a JWT. Returns an OIDCIdentity on success; raises InvalidJwt
        on any rejection. Does not log the token contents on failure."""
        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token).key
        except (jwt.PyJWKClientError, jwt.DecodeError) as exc:
            raise InvalidJwt(f"could not resolve signing key: {type(exc).__name__}") from exc

        try:
            claims = jwt.decode(
                token,
                key=signing_key,
                algorithms=["RS256"],
                audience=self.audience,
                issuer=self.issuer,
                leeway=self.leeway_seconds,
                options={"require": ["exp", "iss", "aud", "sub"]},
            )
        except InvalidTokenError as exc:
            # Don't leak claim content into the message.
            raise InvalidJwt(f"jwt validation failed: {type(exc).__name__}") from exc

        # email_verified must be the boolean True. A string "true" indicates
        # an IdP we don't trust to have done its own format coercion.
        ev = claims.get("email_verified")
        if ev is not True:
            raise InvalidJwt("email_verified claim missing or not boolean true")

        email = claims.get("email")
        if not isinstance(email, str) or not email:
            raise InvalidJwt("email claim missing or not a string")

        sub = claims.get("sub")
        if not isinstance(sub, str) or not sub:
            raise InvalidJwt("sub claim missing or not a string")

        # auth_time is optional at the verifier level.
        # OIDC's NumericDate type technically allows non-integer values, but
        # AuthRocket emits integer epoch seconds — we stay strict because a
        # different shape would indicate an IdP we haven't audited.
        auth_time = claims.get("auth_time")
        if auth_time is not None and not isinstance(auth_time, int):
            raise InvalidJwt("auth_time claim, when present, must be an integer")

        return OIDCIdentity(
            issuer=self.issuer,
            subject=sub,
            email=email,
            auth_time=auth_time,
            raw_claims=claims,
        )


class AuthRocketVerifier(JwtVerifier):
    """JwtVerifier bound to a Settings instance. Constructed at lifespan."""

    @classmethod
    def from_settings(cls, settings: Settings) -> AuthRocketVerifier:  # noqa: F821
        """Build from Settings, raising if any AUTHROCKET_* env is missing.

        This is the fail-fast point: lifespan calls from_settings, and a
        misconfigured prod deployment refuses to boot rather than silently
        running with auth disabled.
        """
        missing = []
        if not settings.authrocket_issuer:
            missing.append("AUTHROCKET_ISSUER")
        if not settings.authrocket_audience:
            missing.append("AUTHROCKET_AUDIENCE")
        if not settings.authrocket_jwks_url:
            missing.append("AUTHROCKET_JWKS_URL")
        if missing:
            raise RuntimeError(f"AuthRocketVerifier cannot be constructed: missing env: {missing}")
        return cls(
            jwks_url=settings.authrocket_jwks_url,
            issuer=settings.authrocket_issuer,
            audience=settings.authrocket_audience,
            leeway_seconds=settings.authrocket_jwt_leeway_seconds,
        )
