"""Control plane FastAPI application."""

from contextlib import asynccontextmanager

import asyncpg
from fastapi import Depends, FastAPI
from qiita_common.models import HealthResponse

from .config import Settings
from .db import close_pool, get_pool
from .deps import get_db_pool
from .routes import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    app.state.pool = await get_pool(settings.database_url)
    app.state.settings = settings
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
