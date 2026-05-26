from contextlib import asynccontextmanager

from fastapi import FastAPI
from qiita_common.auth_constants import API_PREFIX
from qiita_common.log import install_authorization_scrub
from qiita_common.models import HealthResponse

from .backend import ComputeBackend
from .backends.local import LocalBackend
from .backends.slurm import SlurmBackend
from .config import BACKEND_LOCAL, BACKEND_SLURM, Settings, install_settings
from .jobs import scan_native_jobs
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
            native_python=settings.slurm.native_python,
        )
    raise RuntimeError(f"unknown COMPUTE_BACKEND={settings.backend_type!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    install_authorization_scrub()
    settings = Settings.from_env()
    # Install once so make_cp_client / get_settings hit the cached
    # instance for every subsequent step. Misconfig already crashed on
    # the Settings.from_env() line above; install_settings just makes
    # the resolved value available to non-FastAPI code paths
    # (sequence_range.make_cp_client) without re-reading the env.
    install_settings(settings)
    app.state.settings = settings
    app.state.backend = _build_backend(settings)
    app.state.cp_to_co_token = settings.cp_to_co_token
    # Refuse to start with a malformed native-job tree. Surfacing a
    # missing `Inputs` or `execute` export at boot beats discovering
    # it on the first submission.
    scan_native_jobs()
    try:
        yield
    finally:
        # Backends own their own resources; aclose() is a no-op for
        # LocalBackend and closes the httpx client for SlurmBackend.
        await app.state.backend.aclose()


app = FastAPI(title="qiita-compute-orchestrator", lifespan=lifespan)
app.include_router(step_router, prefix=API_PREFIX)


@app.get("/health")
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="qiita-compute-orchestrator")
