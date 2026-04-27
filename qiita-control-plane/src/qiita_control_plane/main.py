"""Control plane FastAPI application."""

from contextlib import asynccontextmanager

import asyncpg
from fastapi import Depends, FastAPI
from qiita_common.models import HealthResponse

from .auth.oidc import AuthRocketVerifier
from .config import Settings
from .db import close_pool, get_pool
from .deps import get_db_pool
from .routes import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    app.state.pool = await get_pool(settings.database_url)
    app.state.settings = settings
    # Phase E: build the OIDC verifier eagerly when AUTHROCKET_* is set.
    # AuthRocketVerifier.from_settings raises on missing env, which makes
    # a misconfigured prod boot fail fast. Tests inject their own verifier
    # into app.state.oidc_verifier directly via the JWKS harness.
    if settings.authrocket_issuer:
        app.state.oidc_verifier = AuthRocketVerifier.from_settings(settings)
    else:
        app.state.oidc_verifier = None
    yield
    await close_pool(app.state.pool)


app = FastAPI(title="qiita-control-plane", lifespan=lifespan)
app.include_router(api_router)


@app.get("/health")
async def health(pool: asyncpg.Pool = Depends(get_db_pool)) -> HealthResponse:
    try:
        await pool.fetchval("SELECT 1")
    except Exception:
        return HealthResponse(status="degraded", service="qiita-control-plane")
    return HealthResponse(status="ok", service="qiita-control-plane")
