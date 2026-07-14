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
# token belongs to the compute service-account principal (site-chosen
# name; `compute` on the live deploy) whose provisioning is documented
# in docs/runbooks/compute-service-account-provisioning.md.
DEFAULT_CO_TO_CP_TOKEN_PATH = "/etc/qiita/co-to-cp.token"

# Production install path of the orchestrator env file. Its mere existence is
# the "this is a real orchestrator host" signal the compute-readiness CLI uses
# to turn a misinvocation into a loud failure instead of a benign skip (see
# cli/compute_readiness.py::_run_all_checks). Deliberately NOT keyed on
# SLURMRESTD_URL / CO_TO_CP_TOKEN_PATH env presence — those can legitimately be
# set in a dev/CI orchestrator boot and would cause false failures; the
# /etc/qiita path is what a dev box / CI runner does not have.
ORCHESTRATOR_ENV_PATH = "/etc/qiita/compute-orchestrator.env"

# The one correct operator invocation for running the compute-readiness
# diagnostic (and anything else that resolves the CO→CP token). It MUST run as
# the orchestrator service account sourcing the orchestrator env file: the
# command resolves COMPUTE_BACKEND / SLURM_* from compute-orchestrator.env and
# reads the 0400 qiita-orch:qiita-orch co-to-cp.token, neither of which is
# reachable as qiita-api sourcing control-plane.env. Shared by _resolve_token's
# permission error and the compute-readiness misinvocation guard so the two
# can't drift (the recurring redeploy defect).
CORRECT_COMPUTE_READINESS_INVOCATION = (
    "sudo -u qiita-orch bash -c 'set -a; source /etc/qiita/compute-orchestrator.env; "
    "set +a; qiita-admin compute-readiness'"
)

BACKEND_LOCAL = "local"
BACKEND_SLURM = "slurm"

# Default data-plane gRPC origin for outbound Flight DoGet from native jobs
# (reference-chunk streaming). Mirrors the control plane's own DATA_PLANE_URL
# default. Overridden in production via DATA_PLANE_URL so the compute node
# DoGets against the nginx-fronted gRPC origin. Not fail-fast (has a default),
# unlike the SLURM-only required vars.
DEFAULT_DATA_PLANE_URL = "grpc://localhost:50051"


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
    # Derived-artifact base root (PATH_DERIVED). Native index builders write
    # their persistent, reusable artifacts here:
    # `{path_derived}/references/{idx}/{rype,minimap2}/...`. Resolved on every
    # backend (LocalBackend smoke runs build indexes too) and leniently — like
    # path_scratch, NOT validated as absolute/existing here. Distinct from
    # `path_derived_images` below (the strict, SLURM-only `PATH_DERIVED/images`
    # SIF dir). Both read PATH_DERIVED; the SLURM backend propagates it into the
    # native-job env (see SlurmBackend.submit_step) so the launcher's
    # get_settings() resolves the real value, not the $TMPDIR/qiita/derived
    # dev fallback. On a SLURM deploy PATH_DERIVED is already mandatory —
    # from_env() raises via _resolve_path_derived_images() if it's unset — so
    # this lenient fallback only ever applies to LocalBackend/dev.
    path_derived: str
    # The shared bearer token CP-to-CO calls present. Loaded from a file
    # in production; env-var fallback only when QIITA_ALLOW_TOKEN_ENV=true.
    cp_to_co_token: str
    # Control plane base URL the orchestrator hits for outbound calls
    # (sequence-range mint, future CO→CP endpoints). Includes scheme +
    # host + port; route paths from qiita_common.api_paths get appended.
    cp_url: str
    # PAT belonging to the compute service-account principal (site-chosen
    # name; `compute` on the live deploy), presented as Bearer auth on
    # outbound CO→CP calls. Same file-or-env resolution pattern as cp_to_co_token.
    co_to_cp_token: str
    # Data-plane gRPC origin native jobs DoGet reference chunks from. Propagated
    # into the SLURM job env like PATH_DERIVED so the launcher's get_settings()
    # resolves the real origin on the compute node (see SlurmBackend). Has a
    # default (DEFAULT_DATA_PLANE_URL) so it is NOT fail-fast — a deploy that
    # forgets it falls back to localhost rather than keeping the unit down.
    data_plane_url: str = DEFAULT_DATA_PLANE_URL
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
            path_derived=os.environ.get(
                "PATH_DERIVED",
                os.environ.get("TMPDIR", "/tmp") + "/qiita/derived",
            ),
            cp_to_co_token=_resolve_token("cp_to_co") if require_cp_to_co_token else "",
            cp_url=_resolve_cp_url(),
            co_to_cp_token=_resolve_token("co_to_cp"),
            data_plane_url=os.environ.get("DATA_PLANE_URL", DEFAULT_DATA_PLANE_URL),
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
    co_to_cp: compute service-account PAT (site-chosen name; `compute`
              on the live deploy) the orchestrator presents on outbound
              calls (e.g. POST /sequence-range); provisioning is documented
              in docs/runbooks/compute-service-account-provisioning.md.

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
        # Installed 0440 root:qiita-services (first-deploy.md step 7); both
        # qiita-api and qiita-orch read it via the qiita-services group.
        perm_hint = "installed mode 0440 root:qiita-services — check the file's group/mode"
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
        # Installed 0400 qiita-orch:qiita-orch (provisioning runbook); only
        # qiita-orch can read it — a PermissionError here is almost always the
        # CLI run as the wrong user.
        perm_hint = (
            "installed mode 0400 owned by qiita-orch — run as the qiita-orch service account"
        )

    path = Path(os.environ.get(path_env, default_path))
    if path.is_file():
        try:
            return path.read_text().strip()
        except PermissionError as exc:
            # The file is present but unreadable by the current user — for the
            # CO→CP token this is almost always the compute-readiness CLI run as
            # qiita-api (the token is 0400 qiita-orch). Without this branch the
            # error fell through to the generic "no token available" message
            # below, which misleads the operator into thinking the token is
            # missing. Name the real cause + the fix; the perms guidance is
            # per-kind so a CP↔CO PermissionError isn't told to "run as
            # qiita-orch" (that token is the shared root:qiita-services bearer).
            hint = f"\n    {CORRECT_COMPUTE_READINESS_INVOCATION}" if kind == "co_to_cp" else ""
            raise RuntimeError(
                f"orchestrator: {label} token file {path} exists but the current"
                f" user cannot read it ({exc.strerror}). It is {perm_hint}."
                f"{hint}{runbook_hint}"
            ) from exc

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
