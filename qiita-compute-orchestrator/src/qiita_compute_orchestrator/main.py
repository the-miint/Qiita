from contextlib import asynccontextmanager

from fastapi import FastAPI
from qiita_common.auth_constants import API_PREFIX
from qiita_common.log import install_authorization_scrub
from qiita_common.models import HealthResponse

from .backend import ComputeBackend
from .backends.local import LocalBackend
from .backends.slurm import SlurmBackend
from .config import BACKEND_LOCAL, BACKEND_SLURM, Settings
from .step import router as step_router


def _build_backend(backend_type: str) -> ComputeBackend:
    if backend_type == BACKEND_LOCAL:
        return LocalBackend()
    if backend_type == BACKEND_SLURM:
        return SlurmBackend()
    raise RuntimeError(f"unknown COMPUTE_BACKEND={backend_type!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    install_authorization_scrub()
    settings = Settings.from_env()
    app.state.settings = settings
    app.state.backend = _build_backend(settings.backend_type)
    app.state.cp_to_co_token = settings.cp_to_co_token
    yield


app = FastAPI(title="qiita-compute-orchestrator", lifespan=lifespan)
app.include_router(step_router, prefix=API_PREFIX)


@app.get("/health")
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="qiita-compute-orchestrator")
