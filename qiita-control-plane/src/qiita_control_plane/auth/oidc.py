"""OIDC JWT verification.

JwtVerifier is the pure verifier — takes JWKS URL, issuer, optional audience,
and leeway. AuthRocketVerifier is the config-bound wrapper that constructs
from a Settings object. Tests instantiate JwtVerifier directly against a
local JWKS harness; production wires AuthRocketVerifier from Settings at
lifespan.

We accept only RS256 (the algorithm AuthRocket uses) and require:
  - signature verifies against the JWKS-fetched key matching the JWT's `kid`
  - `iss` matches the configured issuer
  - `exp` is in the future (within leeway)
  - `email` and `sub` claims are present

We *optionally* check:
  - `aud` contains the configured audience (str or list per OIDC spec) when
    `audience` is set. AuthRocket's LoginRocket Web realm-scoped tokens omit
    `aud` entirely, and the realm's JWKS is the trust boundary in that mode;
    this verifier accepts `audience=None` to support that.

Other claims AuthRocket may or may not emit:
  - `email_verified` — historically strict-checked here (boolean True only),
    but LoginRocket Web tokens omit it; the realm enforces email verification
    as policy at signup, so we trust the issuer rather than the claim.
  - `auth_time` — surfaced on `OIDCIdentity` if present, else `None`. Not used
    as a freshness anchor — AuthRocket re-emits the same JWT across cached
    sessions so `auth_time`/`iat` don't advance between PAT mints. Freshness
    is anchored in `auth.handoff` via a server-side signed cookie set before
    the AuthRocket round-trip.

Note on `fastapi.security.OpenIdConnect`: that helper is a header-reader
plus an OpenAPI metadata hook, not a verifier — it parses `Bearer <token>`
off the request and stores the well-known URL for Swagger UI but does
not fetch JWKS or check signatures/claims. The two stack rather than
substitute; this module is the verifier.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import jwt
from jwt import InvalidTokenError, PyJWKClient

if TYPE_CHECKING:
    # `Settings` is qiita_control_plane.config.Settings — the Pydantic-shaped
    # config object that reads AUTHROCKET_* env vars at app startup. Imported
    # under TYPE_CHECKING to keep this module free of runtime config coupling.
    from qiita_control_plane.config import Settings


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
        audience: str | None,
        leeway_seconds: int,
    ) -> None:
        if not jwks_url:
            raise ValueError("jwks_url is required")
        if not issuer:
            raise ValueError("issuer is required")
        # `audience` is optional. Pass None on LoginRocket Web realms whose
        # JWTs lack the `aud` claim — verification then skips audience binding.
        # Normalize empty string to None to match Settings.from_env's pattern
        # (`os.environ.get(...) or None`); otherwise an empty AUTHROCKET_AUDIENCE
        # env var would silently configure verify-against-empty-string instead
        # of skip-audience.
        self.jwks_url = jwks_url
        self.issuer = issuer
        self.audience = audience or None
        self.leeway_seconds = leeway_seconds
        self._jwks_client = PyJWKClient(jwks_url)

    def verify(self, token: str) -> OIDCIdentity:
        """Verify a JWT. Returns an OIDCIdentity on success; raises InvalidJwt
        on any rejection. Does not log the token contents on failure."""
        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token).key
        except (jwt.PyJWKClientError, jwt.DecodeError) as exc:
            raise InvalidJwt(f"could not resolve signing key: {type(exc).__name__}") from exc

        # `aud` is conditionally required: when an audience is configured, we
        # require + validate the claim. When it isn't (LoginRocket Web realm),
        # we skip both — the realm's JWKS is the trust boundary and there's
        # no per-app audience to bind to.
        require_claims = ["exp", "iss", "sub"]
        decode_kwargs: dict[str, object] = {}
        if self.audience is not None:
            require_claims.append("aud")
            decode_kwargs["audience"] = self.audience

        try:
            claims = jwt.decode(
                token,
                key=signing_key,
                algorithms=["RS256"],
                issuer=self.issuer,
                leeway=self.leeway_seconds,
                options={"require": require_claims},
                **decode_kwargs,
            )
        except InvalidTokenError as exc:
            # Don't leak claim content into the message.
            raise InvalidJwt(f"jwt validation failed: {type(exc).__name__}") from exc

        email = claims.get("email")
        if not isinstance(email, str) or not email:
            raise InvalidJwt("email claim missing or not a string")

        sub = claims.get("sub")
        if not isinstance(sub, str) or not sub:
            raise InvalidJwt("sub claim missing or not a string")

        # auth_time is optional at the verifier level. Surfaced on OIDCIdentity
        # for callers that want to use it; on LoginRocket Web realms it's
        # absent entirely, so freshness lives at the route layer (auth.handoff).
        # OIDC's NumericDate type technically allows non-integer values; we
        # stay strict because a different shape would indicate an IdP we
        # haven't audited.
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
    def from_settings(cls, settings: Settings) -> AuthRocketVerifier:
        """Build from Settings, raising if a required AUTHROCKET_* env is missing.

        This is the fail-fast point: lifespan calls from_settings, and a
        misconfigured prod deployment refuses to boot rather than silently
        running with auth disabled.

        `AUTHROCKET_AUDIENCE` is *not* required — LoginRocket Web realms emit
        tokens without an `aud` claim, and the verifier skips audience binding
        when `audience=None`. Set the env var only on realms that emit `aud`
        (e.g. an OAuth2 Server integration on a higher AuthRocket plan tier).
        """
        missing = []
        if not settings.authrocket_issuer:
            missing.append("AUTHROCKET_ISSUER")
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
