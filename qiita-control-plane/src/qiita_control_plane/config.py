"""Control plane configuration — reads from environment variables."""

import base64
import os
from dataclasses import dataclass

from qiita_common.config import require_env

# Field defaults for the auth-related Settings knobs. Defined once at module
# scope so the dataclass declaration and the from_env() env-var fallback
# can't drift independently.
_DEFAULT_JWT_LEEWAY_SECONDS = 30
_DEFAULT_PAT_MAX_AUTH_AGE_SECONDS = 300
_DEFAULT_TOKEN_TTL_DAYS = 90


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    hmac_secret_key: bytes
    data_plane_url: str
    # AuthRocket OIDC fields. Optional in Settings — required only at
    # AuthRocketVerifier construction time, which is wired into lifespan.
    # Letting them default to None here keeps tests that don't exercise
    # the auth path from having to set every AUTHROCKET_* env var.
    authrocket_issuer: str | None = None
    authrocket_audience: str | None = None
    authrocket_jwks_url: str | None = None
    authrocket_jwt_leeway_seconds: int = _DEFAULT_JWT_LEEWAY_SECONDS
    authrocket_pat_max_auth_age_seconds: int = _DEFAULT_PAT_MAX_AUTH_AGE_SECONDS
    token_default_ttl_days: int = _DEFAULT_TOKEN_TTL_DAYS

    @classmethod
    def from_env(cls) -> Settings:
        raw = require_env("HMAC_SECRET_KEY")
        try:
            secret = base64.b64decode(raw)
        except Exception as exc:
            raise RuntimeError("HMAC_SECRET_KEY must be valid base64") from exc
        if len(secret) < 16:
            raise RuntimeError("HMAC_SECRET_KEY must decode to at least 16 bytes")

        issuer = os.environ.get("AUTHROCKET_ISSUER") or None
        # JWKS URL defaults from issuer when issuer is set; explicit override wins.
        jwks_url = os.environ.get("AUTHROCKET_JWKS_URL")
        if not jwks_url and issuer:
            jwks_url = f"{issuer.rstrip('/')}/connect/jwks"

        return cls(
            database_url=require_env("DATABASE_URL"),
            hmac_secret_key=secret,
            data_plane_url=os.environ.get("DATA_PLANE_URL", "grpc://localhost:50051"),
            authrocket_issuer=issuer,
            authrocket_audience=os.environ.get("AUTHROCKET_AUDIENCE") or None,
            authrocket_jwks_url=jwks_url,
            authrocket_jwt_leeway_seconds=int(
                os.environ.get("AUTHROCKET_JWT_LEEWAY_SECONDS", str(_DEFAULT_JWT_LEEWAY_SECONDS))
            ),
            authrocket_pat_max_auth_age_seconds=int(
                os.environ.get(
                    "AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS",
                    str(_DEFAULT_PAT_MAX_AUTH_AGE_SECONDS),
                )
            ),
            token_default_ttl_days=int(
                os.environ.get("QIITA_TOKEN_DEFAULT_TTL_DAYS", str(_DEFAULT_TOKEN_TTL_DAYS))
            ),
        )
