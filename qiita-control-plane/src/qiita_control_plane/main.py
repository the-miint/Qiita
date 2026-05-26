"""Control plane FastAPI application."""

from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from fastapi import Depends, FastAPI
from fastapi.staticfiles import StaticFiles
from qiita_common.log import install_authorization_scrub
from qiita_common.models import HealthResponse

from .auth.oidc import AuthRocketVerifier
from .config import Settings
from .db import close_pool, get_pool
from .deps import get_db_pool
from .dispatch import (
    build_compute_backend_client,
    drain_running_dispatches,
    recover_orphaned_tickets,
)
from .landing import router as landing_router
from .routes import api_router

_STATIC_DIR = Path(__file__).resolve().parent / "static"

# Bound on how long we wait for in-flight dispatches at shutdown. systemd's
# default TimeoutStopSec is 90s; staying under that lets us cancel cleanly
# before SIGKILL. Unfinished tasks are picked up by recover_orphaned_tickets
# on the next startup as a safety net.
_DISPATCH_DRAIN_TIMEOUT_SECONDS = 60.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    install_authorization_scrub()

    settings = Settings.from_env()
    app.state.pool = await get_pool(settings.database_url)
    app.state.settings = settings
    # Build the OIDC verifier eagerly when AUTHROCKET_* is set.
    # AuthRocketVerifier.from_settings raises on missing env, which makes
    # a misconfigured prod boot fail fast. Tests inject their own verifier
    # into app.state.oidc_verifier directly via the JWKS harness.
    if settings.authrocket_issuer:
        app.state.oidc_verifier = AuthRocketVerifier.from_settings(settings)
    else:
        app.state.oidc_verifier = None

    # Compute-orchestrator dispatch wiring. The CP itself dispatches
    # workflows in-process via asyncio tasks (no polling worker). When
    # COMPUTE_ORCHESTRATOR_URL is unset, dispatch routes return 503;
    # everything else still works (auth, references, admin, etc.).
    app.state.compute_backend_client = build_compute_backend_client(
        base_url=settings.compute_orchestrator_url,
        token_path=settings.cp_to_co_token_path,
    )
    app.state.running_dispatches = set()
    # Recover any tickets left in non-terminal state by a previous CP
    # process — they have no live owner. Marked FAILED with a 'cp
    # restarted' reason; operators redrive via /work-ticket/{idx}/run.
    await recover_orphaned_tickets(app.state.pool)

    try:
        yield
    finally:
        await drain_running_dispatches(
            app.state.running_dispatches,
            timeout_seconds=_DISPATCH_DRAIN_TIMEOUT_SECONDS,
        )
        if app.state.compute_backend_client is not None:
            await app.state.compute_backend_client.close()
        await close_pool(app.state.pool)


app = FastAPI(title="qiita-control-plane", lifespan=lifespan)
app.include_router(api_router)
app.include_router(landing_router)
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/health")
async def health(pool: asyncpg.Pool = Depends(get_db_pool)) -> HealthResponse:
    try:
        await pool.fetchval("SELECT 1")
    except Exception:
        return HealthResponse(status="degraded", service="qiita-control-plane")
    return HealthResponse(status="ok", service="qiita-control-plane")
