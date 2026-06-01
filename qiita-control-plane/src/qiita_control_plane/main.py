"""Control plane FastAPI application."""

from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from fastapi import Depends, FastAPI, Request
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from qiita_common.log import install_authorization_scrub
from qiita_common.models import HealthResponse, HealthStatus

from .auth.oidc import AuthRocketVerifier
from .config import Settings
from .db import close_pool, get_pool
from .deps import get_db_pool
from .dispatch import (
    build_compute_backend_client,
    drain_running_dispatches,
    recover_orphaned_tickets,
)
from .health import aggregate_health
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


# docs_url / redoc_url are disabled here so we can re-serve the Swagger UI
# and ReDoc shells below from our own /static mount instead of FastAPI's
# default jsdelivr CDN. The deploy host (and many viewers' browsers) may
# have no outbound internet or a strict CSP, which renders the CDN-backed
# pages blank; the vendored assets in static/ make /docs and /redoc depend
# only on this service. openapi_url stays at its default (/openapi.json) and
# is still generated live from the router tree, so new endpoints appear
# automatically. See docs/api-docs.md.
app = FastAPI(
    title="qiita-control-plane",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)
app.include_router(api_router)
app.include_router(landing_router)
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/docs", include_in_schema=False)
async def swagger_ui() -> HTMLResponse:
    """Interactive Swagger UI, served from vendored assets (no CDN)."""
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=f"{app.title} — API docs",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="/static/swagger-ui-bundle.js",
        swagger_css_url="/static/swagger-ui.css",
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect() -> HTMLResponse:
    """OAuth2 redirect target the Swagger UI 'Authorize' flow posts back to."""
    return get_swagger_ui_oauth2_redirect_html()


@app.get("/redoc", include_in_schema=False)
async def redoc_ui() -> HTMLResponse:
    """Read-only ReDoc rendering, served from vendored assets (no CDN)."""
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=f"{app.title} — API docs",
        redoc_js_url="/static/redoc.standalone.js",
        with_google_fonts=False,
    )


@app.get("/health")
async def health(
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> HealthResponse:
    """Aggregated `/health` covering CP + CO + DP with a short cache.

    The legacy single-pool probe is now `_probe_cp` inside
    `qiita_control_plane.health`; everything that consumes only the
    top-level `status` field (Makefile target, landing JS) stays
    backwards-compatible.  The new `services` dict carries the
    per-component breakdown that drives the landing page's three
    status pills.
    """
    settings: Settings = request.app.state.settings
    return await aggregate_health(
        pool=pool,
        compute_orchestrator_url=settings.compute_orchestrator_url,
        data_plane_url=settings.data_plane_url,
    )


@app.get("/healthz")
async def healthz() -> HealthResponse:
    """Cheap liveness probe: 200 iff this CP process is up and serving.

    Deliberately distinct from `/health` — that route aggregates CP +
    CO + DP, which is heavier and, when called from the orchestrator's
    compute-readiness checker, would cascade a CO→CP probe back through
    the CP's own aggregator. Liveness wants neither the DB hit nor the
    downstream fan-out, so this touches no dependencies: no DB, no auth,
    no downstream calls. The compute-orchestrator readiness checker GETs
    this path with the CO→CP bearer and asserts only the 200; the token
    is ignored here because "can this process answer at all" is the only
    question liveness asks. `services` stays unset — there's no aggregate
    to report.
    """
    return HealthResponse(status=HealthStatus.OK.value, service="qiita-control-plane")
