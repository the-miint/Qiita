from contextlib import asynccontextmanager

from fastapi import FastAPI
from qiita_common.log import install_authorization_scrub
from qiita_common.models import HealthResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    install_authorization_scrub()
    yield


app = FastAPI(title="qiita-compute-orchestrator", lifespan=lifespan)


@app.get("/health")
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="qiita-compute-orchestrator")
