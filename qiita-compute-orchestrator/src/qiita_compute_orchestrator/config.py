"""Compute orchestrator configuration.

The orchestrator is mostly a passive HTTP service: it accepts
`POST /step/*` from the control-plane runner, dispatches to its
ComputeBackend, and returns the outputs. It also makes a single class
of outbound call -- POST /api/v1/sequence-range (the CP route lives at
qiita-control-plane/src/qiita_control_plane/routes/sequence_range.py)
-- so two credentials live here: the inbound shared bearer
(cp_to_co_token) and the outbound compute service-account PAT
(co_to_cp_token).

Settings access pattern (asymmetric):

  - Orchestrator FastAPI service: lifespan handler calls Settings.from_env()
    and install_settings(...) so misconfig (missing token, missing env)
    fails the boot. Every subsequent get_settings() call returns the
    cached value with no I/O.

  - SLURM launcher (`python -m qiita_compute_orchestrator.jobs`) and any
    CLI invocation: do NOT call install_settings. get_settings() falls
    back to Settings.from_env() on first call, so jobs that don't reach
    for the CP (e.g., a future native step that only reads from disk)
    never resolve credentials. Jobs that DO reach for CP fail per-step
    if env is missing — same surface as any other per-job dependency.
"""

from __future__ import annotations

import os
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .slurm import DEFAULT_SLURMRESTD_API_VERSION

# Default install location for the shared CP↔CO bearer token in
# production (file mode 0400, owned by the orchestrator user). Mirrors
# the pattern at /etc/qiita/orchestrator.token used elsewhere.
DEFAULT_CP_TO_CO_TOKEN_PATH = "/etc/qiita/cp-to-co.token"

# Default install location for the compute service-account PAT the
# orchestrator presents on outbound calls to the control plane (e.g.
# POST /api/v1/sequence-range). Distinct from CP_TO_CO_TOKEN above —
# this is CO → CP, presented as Bearer auth on httpx requests. The
# token belongs to the `compute-worker` service-account principal
# whose provisioning is documented in
# docs/runbooks/compute-service-account-provisioning.md.
DEFAULT_CO_TO_CP_TOKEN_PATH = "/etc/qiita/co-to-cp.token"

BACKEND_LOCAL = "local"
BACKEND_SLURM = "slurm"

# SLURM polling defaults. The interval is the gap between
# `GET /slurm/.../job/{id}` calls when waiting for a non-terminal
# job; 10s is a reasonable trade-off between latency and slurmrestd
# load. The total timeout caps a single step's wall time at 24h —
# a workflow that exceeds 24h either has a misconfigured walltime
# (declare it longer in YAML) or is stuck (kill from outside).
DEFAULT_SLURM_POLL_INTERVAL_SECONDS = 10
DEFAULT_SLURM_JOB_TIMEOUT_SECONDS = 24 * 60 * 60


@dataclass(frozen=True, slots=True)
class SlurmSettings:
    """SLURM-specific config. Required only when backend_type=slurm;
    when LocalBackend is in use these fields are unset and SlurmBackend
    is never constructed."""

    base_url: str  # slurmrestd URL, e.g. http://slurmrestd-host:6820
    jwt_path: Path  # readable by the orchestrator user
    user_name: str  # SLURM job-execution user (e.g. "qiita-job"), not the orchestrator's own user
    partition: str  # SLURM partition (e.g. "qiita")
    account: str  # SLURM account for usage reporting
    api_version: str  # default v0.0.40
    poll_interval_seconds: int
    job_timeout_seconds: int
    # Python executable the native-step SBATCH script invokes via
    # `srun <native_python> -m qiita_compute_orchestrator.jobs ...`.
    # Default "python" assumes compute nodes already have a Python on
    # PATH with qiita_compute_orchestrator installed. Sites where the
    # cluster does NOT carry the orchestrator's venv set this to an
    # absolute path on the shared filesystem (the orchestrator host's
    # venv interpreter, visible from compute nodes).
    native_python: str
    # Optional SLURM QOS to set on submit. Empty string means "omit
    # qos from the submit body" — the cluster falls back to the
    # SLURMRESTD_USER_NAME's default QOS. Set explicitly so the
    # orchestrator doesn't depend on user-default state.
    qos: str


@dataclass(frozen=True, slots=True)
class Settings:
    backend_type: str
    # Shared scratch base root (PATH_SCRATCH). The readiness probe checks
    # PATH_SCRATCH/ticket for writability; the control plane derives the same
    # PATH_SCRATCH/ticket (the per-ticket workspace SLURM jobs run in), so set
    # PATH_SCRATCH identically across all three env files. Optional in dev —
    # falls back to $TMPDIR/qiita. Deliberately NOT validated as absolute here
    # (unlike the CP/DP, which require + assert absolute): the orchestrator
    # never mints under this path itself — the CP creates the per-ticket
    # subdir and POSTs the absolute path to the CO — so the value only feeds
    # the diagnostic readiness probe. The CP's own absolute check is the
    # fail-fast that matters; this mirrors main's prior SHARED_FILESYSTEM_ROOT
    # posture.
    path_scratch: str
    # The shared bearer token CP-to-CO calls present. Loaded from a file
    # in production; env-var fallback only when QIITA_ALLOW_TOKEN_ENV=true.
    cp_to_co_token: str
    # Control plane base URL the orchestrator hits for outbound calls
    # (sequence-range mint, future CO→CP endpoints). Includes scheme +
    # host + port; route paths from qiita_common.api_paths get appended.
    cp_url: str
    # PAT belonging to the compute-worker service-account principal,
    # presented as Bearer auth on outbound CO→CP calls. Same file-or-env
    # resolution pattern as cp_to_co_token.
    co_to_cp_token: str
    # SLURM config — non-None only when backend_type=slurm.
    slurm: SlurmSettings | None = None
    # Shared-FS dir where built SIFs land, derived as PATH_DERIVED/images.
    # Required for SLURM container workflows (SlurmBackend joins it with the
    # YAML's bare `container:` filename at submit time). None on the
    # launcher path and on LocalBackend-only deploys.
    path_derived_images: Path | None = None

    @classmethod
    def from_env(cls, *, require_cp_to_co_token: bool = True) -> Settings:
        """Resolve a Settings from environment.

        `require_cp_to_co_token` is True for the orchestrator FastAPI
        service (the lifespan handler) — `cp_to_co_token` is the shared
        bearer it must present-compare on inbound `POST /step/*`, so
        a missing token is a boot-time fatal.

        It's False on the SLURM-launcher / CLI path (set by the
        no-install fallback in `get_settings()` below). Those processes
        never serve inbound traffic; they only ever *make* outbound
        CO→CP calls, which use `co_to_cp_token`. Skipping
        `cp_to_co_token` resolution lets us drop `CP_TO_CO_TOKEN` from
        the SLURM job env entirely, which narrows the `scontrol show
        job` exposure to just the outbound PAT.

        ``PATH_DERIVED`` is resolved only when ``backend_type=slurm``:
        SlurmBackend joins ``PATH_DERIVED/images`` with the YAML's bare
        ``container:`` SIF filename at submit time, so a misconfigured
        production deploy fails at boot rather than at the first container
        step. Validation is strict: absolute path that exists and is a
        directory.
        """
        backend_type = os.environ.get("COMPUTE_BACKEND", BACKEND_LOCAL)
        slurm = _resolve_slurm_settings() if backend_type == BACKEND_SLURM else None
        path_derived_images = (
            _resolve_path_derived_images() if backend_type == BACKEND_SLURM else None
        )
        return cls(
            backend_type=backend_type,
            path_scratch=os.environ.get(
                "PATH_SCRATCH",
                os.environ.get("TMPDIR", "/tmp") + "/qiita",
            ),
            cp_to_co_token=_resolve_token("cp_to_co") if require_cp_to_co_token else "",
            cp_url=_resolve_cp_url(),
            co_to_cp_token=_resolve_token("co_to_cp"),
            slurm=slurm,
            path_derived_images=path_derived_images,
        )


def _resolve_slurm_settings() -> SlurmSettings:
    """Read SLURM env vars and bail loudly on missing-required so a
    misconfigured `COMPUTE_BACKEND=slurm` boot fails at startup instead
    of at the first /step/* call."""

    def _required(name: str) -> str:
        v = os.environ.get(name)
        if not v:
            raise RuntimeError(f"orchestrator: COMPUTE_BACKEND=slurm requires {name} to be set")
        return v

    return SlurmSettings(
        base_url=_required("SLURMRESTD_URL"),
        jwt_path=Path(_required("SLURMRESTD_JWT_PATH")),
        user_name=_required("SLURMRESTD_USER_NAME"),
        partition=_required("SLURM_PARTITION"),
        account=_required("SLURM_ACCOUNT"),
        api_version=os.environ.get("SLURMRESTD_API_VERSION", DEFAULT_SLURMRESTD_API_VERSION),
        poll_interval_seconds=int(
            os.environ.get("SLURM_POLL_INTERVAL_SECONDS", str(DEFAULT_SLURM_POLL_INTERVAL_SECONDS))
        ),
        job_timeout_seconds=int(
            os.environ.get("SLURM_JOB_TIMEOUT_SECONDS", str(DEFAULT_SLURM_JOB_TIMEOUT_SECONDS))
        ),
        native_python=os.environ.get("SLURM_NATIVE_PYTHON", "python"),
        qos=os.environ.get("SLURM_QOS", ""),
    )


def _resolve_path_derived_images() -> Path:
    """Resolve PATH_DERIVED/images to a validated absolute directory path.

    PATH_DERIVED is the derived (built-artifact) filesystem root; built
    SIFs live under PATH_DERIVED/images. Validation is strict — boot-time
    fail-fast is the contract: an operator who forgets PATH_DERIVED sees
    the error before the systemd unit reaches Ready, not when the first
    container step submits.
    """
    raw = os.environ.get("PATH_DERIVED")
    if not raw:
        raise RuntimeError(
            "orchestrator: COMPUTE_BACKEND=slurm requires PATH_DERIVED"
            " (the derived-artifact filesystem root, e.g."
            " /scratch/persistent). Built SIFs live under PATH_DERIVED/images,"
            " which SlurmBackend joins with the YAML's bare `container:`"
            " filename at submit time."
        )
    base = Path(raw)
    if not base.is_absolute():
        raise RuntimeError(f"orchestrator: PATH_DERIVED must be absolute, got {raw!r}")
    path = base / "images"
    if not path.exists():
        raise RuntimeError(f"orchestrator: PATH_DERIVED/images does not exist: {path}")
    if not path.is_dir():
        raise RuntimeError(f"orchestrator: PATH_DERIVED/images is not a directory: {path}")
    return path


def _resolve_cp_url() -> str:
    """The control plane base URL for outbound CO→CP calls. Defaults
    to http://localhost:8080 for dev; production sets QIITA_CP_URL to
    the nginx-fronted https origin (e.g. https://qiita-miint.ucsd.edu).
    """
    return os.environ.get("QIITA_CP_URL", "http://localhost:8080").rstrip("/")


def _resolve_token(kind: Literal["cp_to_co", "co_to_cp"]) -> str:
    """Resolve a bearer token by direction.

    cp_to_co: shared bearer the control-plane runner presents on
              inbound POST /step/*.
    co_to_cp: compute-worker service-account PAT the orchestrator
              presents on outbound calls (e.g. POST /sequence-range);
              provisioning is documented in
              docs/runbooks/compute-service-account-provisioning.md.

    Same precedence for both:
      1. {DIRECTION}_TOKEN_PATH (default under /etc/qiita). If the
         file exists, use it — the production drop-in.
      2. {DIRECTION}_TOKEN env var, gated on QIITA_ALLOW_TOKEN_ENV=true.
         Dev / CI must explicitly opt in; prod never sets the flag, so
         a leaked env var alone can't drive auth.
    """
    if kind == "cp_to_co":
        path_env, default_path, env_var, label = (
            "CP_TO_CO_TOKEN_PATH",
            DEFAULT_CP_TO_CO_TOKEN_PATH,
            "CP_TO_CO_TOKEN",
            "CP↔CO",
        )
        runbook_hint = ""
    else:  # "co_to_cp"
        path_env, default_path, env_var, label = (
            "CO_TO_CP_TOKEN_PATH",
            DEFAULT_CO_TO_CP_TOKEN_PATH,
            "CO_TO_CP_TOKEN",
            "CO→CP",
        )
        runbook_hint = (
            " See docs/runbooks/compute-service-account-provisioning.md"
            " for the production provisioning flow."
        )

    path = Path(os.environ.get(path_env, default_path))
    if path.is_file():
        return path.read_text().strip()

    if os.environ.get("QIITA_ALLOW_TOKEN_ENV", "false").lower() == "true":
        token = os.environ.get(env_var)
        if token:
            return token.strip()

    raise RuntimeError(
        f"orchestrator: no {label} token available. Either install the token"
        f" at {path} (mode 0400) or set QIITA_ALLOW_TOKEN_ENV=true and"
        f" {env_var} for dev/CI.{runbook_hint}"
    )


# ContextVar-backed install/get to drive the asymmetric pattern
# documented in the module header. Default = None means "not installed
# yet"; get_settings() then falls back to Settings.from_env() so the
# SLURM launcher / CLI paths work without an explicit install step.
_settings_ctx: ContextVar[Settings | None] = ContextVar("qiita_co_settings", default=None)


def install_settings(settings: Settings) -> None:
    """Cache a Settings instance for subsequent get_settings() calls.

    The FastAPI lifespan handler calls this at boot so misconfig
    (missing CO_TO_CP_TOKEN, unreadable token file, etc.) surfaces as
    a boot-time RuntimeError before any work_ticket can be accepted.

    The SLURM launcher does NOT call this — its main() runs once per
    job, and we want jobs that don't reach for the CP to skip the
    Settings resolution entirely. Same applies to ad-hoc CLI
    invocations for debugging.

    Tests use this to inject a fake Settings; production code only
    calls it from the lifespan handler.
    """
    _settings_ctx.set(settings)


def get_settings() -> Settings:
    """Return the installed Settings, or resolve fresh from env if no
    install has happened yet.

    Service path: lifespan called install_settings at boot, get_settings
    returns the cached value with no I/O.

    Launcher / CLI path: install_settings was never called, get_settings
    calls Settings.from_env() lazily. Jobs that never invoke this don't
    pay the token-resolution cost. We pass
    `require_cp_to_co_token=False` because that token is *inbound*
    auth on `POST /step/*` — the launcher / CLI never serves that
    route, so demanding it would force the orchestrator to propagate
    the token through SLURM env just to satisfy a path no job
    exercises.
    """
    s = _settings_ctx.get()
    if s is not None:
        return s
    return Settings.from_env(require_cp_to_co_token=False)
