"""Control plane configuration — reads from environment variables."""

import base64
import os
from dataclasses import dataclass
from pathlib import Path

from qiita_common.config import require_env

# Field defaults for the auth-related Settings knobs. Defined once at module
# scope so the dataclass declaration and the from_env() env-var fallback
# can't drift independently.
_DEFAULT_JWT_LEEWAY_SECONDS = 30
_DEFAULT_PAT_MAX_AUTH_AGE_SECONDS = 300
_DEFAULT_TOKEN_TTL_DAYS = 90
# Cookie holding {state, timestamp_ms, cli, port} is set on /auth/login and
# read on /auth/handoff. The window bounds how long a user may take to
# complete the AuthRocket round-trip; longer windows expand replay risk.
_DEFAULT_AUTH_HANDOFF_FRESHNESS_SECONDS = 60
# Single-use code handed back to the CLI's loopback; redeemed at /auth/cli-exchange.
# Short TTL so an intercepted code dies within seconds.
_DEFAULT_CLI_LOGIN_CODE_TTL_SECONDS = 30

# Hard cap on a single POST /sequence-range allocation. The bigint domain
# is 2^63 so a runaway loop in a compute step could otherwise burn an
# arbitrary slice of the sequence_idx space; this gives a generous
# upper bound (10^10) while making accidental over-allocation rejected
# at the route layer rather than absorbed silently.
_DEFAULT_MAX_SEQUENCE_MINT_COUNT = 10_000_000_000


_DEFAULT_CP_TO_CO_TOKEN_PATH = Path("/etc/qiita/cp-to-co.token")


def _parse_positive_int_env(var: str, default: int) -> int:
    """Read `var` from the environment as a positive int, or fall back to
    `default`. Raises RuntimeError naming the variable on a non-int value
    or on a value <= 0 — "fail loudly with context" per the project ethos.
    """
    raw = os.environ.get(var)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{var} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise RuntimeError(f"{var} must be positive, got {value}")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    hmac_secret_key: bytes
    data_plane_url: str
    # Compute-orchestrator dispatch. Both fields optional: when
    # `compute_orchestrator_url` is None, the CP boots without an HTTP client
    # and any work-ticket dispatch route returns 503 — useful for tests and
    # for environments without an orchestrator (e.g. CP-only smoke).
    compute_orchestrator_url: str | None = None
    cp_to_co_token_path: Path = _DEFAULT_CP_TO_CO_TOKEN_PATH
    # AuthRocket fields. Optional in Settings — required only at
    # AuthRocketVerifier construction time, which is wired into lifespan.
    # Letting them default to None here keeps tests that don't exercise
    # the auth path from having to set every AUTHROCKET_* env var.
    #
    # `authrocket_audience` stays optional in two senses: (a) tests don't have
    # to set it, and (b) on LoginRocket Web realms the JWTs lack the `aud`
    # claim entirely — the verifier skips audience checking when this is None.
    # See `auth.oidc.JwtVerifier` for the rationale.
    authrocket_issuer: str | None = None
    authrocket_audience: str | None = None
    authrocket_jwks_url: str | None = None
    # Realm's loginrocket subdomain base URL (e.g.
    # https://merry-lion-7652.e2.loginrocket.com). Required for the /auth/login
    # → AuthRocket redirect to construct.
    authrocket_loginrocket_url: str | None = None
    authrocket_jwt_leeway_seconds: int = _DEFAULT_JWT_LEEWAY_SECONDS
    authrocket_pat_max_auth_age_seconds: int = _DEFAULT_PAT_MAX_AUTH_AGE_SECONDS
    token_default_ttl_days: int = _DEFAULT_TOKEN_TTL_DAYS
    # Externally-resolvable URL of the control plane itself (e.g.
    # https://qiita.example.com). Used to build the redirect_uri AuthRocket
    # bounces back to. Required for /auth/login.
    qiita_endpoint_url: str | None = None
    auth_handoff_freshness_seconds: int = _DEFAULT_AUTH_HANDOFF_FRESHNESS_SECONDS
    cli_login_code_ttl_seconds: int = _DEFAULT_CLI_LOGIN_CODE_TTL_SECONDS
    max_sequence_mint_count: int = _DEFAULT_MAX_SEQUENCE_MINT_COUNT
    # Filesystem root the data plane writes DoPut uploads under. The runner
    # resolves `*_upload_idx` keys in a work_ticket's action_context to
    # `{root}/uploads/{idx}/upload.parquet` (compute_upload_staging_path)
    # before invoking workflow steps. Default matches the data plane's
    # `UPLOAD_STAGING_ROOT` default; in production both sides set the env
    # var to the same shared-filesystem path.
    upload_staging_root: Path = Path("/scratch/ephemeral/staging")
    # Per-work_ticket workspace root the runner mints attempt subdirs under
    # (`<workspace_root>/<work_ticket_idx>/<entry-name>/attempt-<N>/`).
    # Production points this at the shared scratch filesystem; integration
    # tests override via the WORKSPACE_ROOT env var so the runner doesn't
    # try to create `/scratch/ephemeral/workspace` on CI agents.
    workspace_root: Path = Path("/scratch/ephemeral/workspace")

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

        compute_orchestrator_url = os.environ.get("COMPUTE_ORCHESTRATOR_URL") or None
        cp_to_co_token_path = Path(
            os.environ.get("CP_TO_CO_TOKEN_PATH", str(_DEFAULT_CP_TO_CO_TOKEN_PATH))
        )

        return cls(
            database_url=require_env("DATABASE_URL"),
            hmac_secret_key=secret,
            data_plane_url=os.environ.get("DATA_PLANE_URL", "grpc://localhost:50051"),
            compute_orchestrator_url=compute_orchestrator_url,
            cp_to_co_token_path=cp_to_co_token_path,
            authrocket_issuer=issuer,
            authrocket_audience=os.environ.get("AUTHROCKET_AUDIENCE") or None,
            authrocket_jwks_url=jwks_url,
            authrocket_loginrocket_url=os.environ.get("AUTHROCKET_LOGINROCKET_URL") or None,
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
            qiita_endpoint_url=os.environ.get("QIITA_ENDPOINT_URL") or None,
            auth_handoff_freshness_seconds=int(
                os.environ.get(
                    "AUTH_HANDOFF_FRESHNESS_SECONDS",
                    str(_DEFAULT_AUTH_HANDOFF_FRESHNESS_SECONDS),
                )
            ),
            cli_login_code_ttl_seconds=int(
                os.environ.get(
                    "CLI_LOGIN_CODE_TTL_SECONDS",
                    str(_DEFAULT_CLI_LOGIN_CODE_TTL_SECONDS),
                )
            ),
            max_sequence_mint_count=_parse_positive_int_env(
                "QIITA_MAX_SEQUENCE_MINT_COUNT",
                _DEFAULT_MAX_SEQUENCE_MINT_COUNT,
            ),
            upload_staging_root=Path(
                os.environ.get("UPLOAD_STAGING_ROOT", "/scratch/ephemeral/staging")
            ),
            workspace_root=Path(os.environ.get("WORKSPACE_ROOT", "/scratch/ephemeral/workspace")),
        )
