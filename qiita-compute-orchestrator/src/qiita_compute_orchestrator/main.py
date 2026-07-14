from contextlib import asynccontextmanager

from fastapi import FastAPI
from qiita_common.auth_constants import API_PREFIX
from qiita_common.log import install_authorization_scrub
from qiita_common.models import HealthResponse, HealthStatus

from .backend import ComputeBackend
from .backends.local import LocalBackend
from .backends.slurm import SlurmBackend
from .config import BACKEND_LOCAL, BACKEND_SLURM, Settings, install_settings
from .jobs import scan_native_jobs
from .reference_artifact import router as reference_artifact_router
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
            native_python=settings.slurm.native_python,
            # Forward the outbound CO→CP token + CP URL so SLURM jobs
            # can re-resolve Settings.from_env(require_cp_to_co_token=False)
            # on the compute node without reading deploy-host-local
            # /etc/qiita/*.token. The inbound CP→CO bearer is *not*
            # forwarded — the launcher never serves the /step/* routes.
            # See SlurmBackend.submit_step's extra_env wiring.
            co_to_cp_token=settings.co_to_cp_token,
            cp_url=settings.cp_url,
            # Propagated into the job env so native steps that derive a
            # persistent path from it (build_rype_index → the .ryxdi) resolve
            # the real PATH_SCRATCH on the compute node instead of the
            # $TMPDIR/qiita default. See SlurmBackend.submit_step's extra_env wiring.
            path_scratch=settings.path_scratch,
            # Propagated into the job env for the same reason as path_scratch:
            # native index builders derive {path_derived}/references/{idx}/...
            path_derived=settings.path_derived,
            # Propagated into the job env so a native job that streams reference
            # chunks (Flight DoGet) resolves the real data-plane origin on the
            # compute node instead of the localhost default.
            data_plane_url=settings.data_plane_url,
            qos=settings.slurm.qos,
            # PATH_DERIVED/images is validated in Settings.from_env when
            # backend_type=slurm — non-None here on the production path.
            path_derived_images=settings.path_derived_images,
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
    # (cp_client.make_cp_client) without re-reading the env.
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
app.include_router(reference_artifact_router, prefix=API_PREFIX)


@app.get("/health")
async def health() -> HealthResponse:
    # Process-liveness only. The CP's `/health` aggregator reads
    # `body["status"] == HealthStatus.OK.value` here to populate
    # `services.co` in its per-service breakdown — any change to
    # that contract (return shape, status field name, or the value
    # the CP compares against) needs the CP aggregator updated in
    # the same PR. A real slurmrestd-reachability probe at this
    # endpoint is tracked as a follow-up; today this returning OK
    # means the FastAPI process is up, not that it can dispatch.
    return HealthResponse(status=HealthStatus.OK.value, service="qiita-compute-orchestrator")
