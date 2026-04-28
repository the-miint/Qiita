import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from qiita_common.log import AuthorizationScrubFilter
from qiita_common.models import HealthResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Scrub `Authorization: Bearer ...` from every log record. The
    # orchestrator forwards bearers to the control plane on callbacks, so
    # any logged request payload is a leak risk without this filter.
    logging.getLogger().addFilter(AuthorizationScrubFilter())
    yield


app = FastAPI(title="qiita-compute-orchestrator", lifespan=lifespan)


@app.get("/health")
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="qiita-compute-orchestrator")
