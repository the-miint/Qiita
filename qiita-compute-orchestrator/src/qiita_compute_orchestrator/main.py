from contextlib import asynccontextmanager

from fastapi import FastAPI
from qiita_common.auth_constants import API_PREFIX
from qiita_common.log import install_authorization_scrub
from qiita_common.models import HealthResponse

from .backend import ComputeBackend
from .backends.local import LocalBackend
from .backends.slurm import SlurmBackend
from .config import BACKEND_LOCAL, BACKEND_SLURM, Settings
from .slurm import SlurmrestdClient
from .step import router as step_router


def _build_backend(settings: Settings) -> ComputeBackend:
    if settings.backend_type == BACKEND_LOCAL:
        return LocalBackend()
    if settings.backend_type == BACKEND_SLURM:
        if settings.slurm is None:
            # config.from_env() refuses to construct Settings with
            # slurm=None when backend_type=slurm; this branch is
            # defense-in-depth for direct callers (tests).
            raise RuntimeError("SlurmBackend requires slurm settings; got Settings(slurm=None)")
        client = SlurmrestdClient(
            base_url=settings.slurm.base_url,
            jwt_path=settings.slurm.jwt_path,
            user_name=settings.slurm.user_name,
            api_version=settings.slurm.api_version,
        )
        return SlurmBackend(
            client=client,
            partition=settings.slurm.partition,
            account=settings.slurm.account,
            poll_interval_seconds=settings.slurm.poll_interval_seconds,
            job_timeout_seconds=settings.slurm.job_timeout_seconds,
        )
    raise RuntimeError(f"unknown COMPUTE_BACKEND={settings.backend_type!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    install_authorization_scrub()
    settings = Settings.from_env()
    app.state.settings = settings
    app.state.backend = _build_backend(settings)
    app.state.cp_to_co_token = settings.cp_to_co_token
    try:
        yield
    finally:
        # SlurmBackend owns an httpx client; close it on shutdown so
        # asyncio doesn't warn about an unclosed transport.
        backend = app.state.backend
        if isinstance(backend, SlurmBackend):
            await backend._client.close()  # noqa: SLF001 — module-private close


app = FastAPI(title="qiita-compute-orchestrator", lifespan=lifespan)
app.include_router(step_router, prefix=API_PREFIX)


@app.get("/health")
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="qiita-compute-orchestrator")
