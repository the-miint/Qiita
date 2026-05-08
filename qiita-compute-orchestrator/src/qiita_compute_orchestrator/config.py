"""Compute orchestrator configuration.

The orchestrator is a passive HTTP service: it accepts `POST /step/run`
from the control-plane runner, dispatches to its ComputeBackend, and
returns the outputs. It has no outbound calls in v1, so there is no
credential for talking _to_ the control plane — only the inbound bearer
token used to authenticate CP-originated requests.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .slurm import DEFAULT_SLURMRESTD_API_VERSION

# Default install location for the shared CP↔CO bearer token in
# production (file mode 0400, owned by the orchestrator user). Mirrors
# the pattern at /etc/qiita/orchestrator.token used elsewhere.
DEFAULT_CP_TO_CO_TOKEN_PATH = "/etc/qiita/cp-to-co.token"

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

    base_url: str  # http://slurm-controller:6820
    jwt_path: Path  # readable by the orchestrator user
    user_name: str  # SLURM user identity (typically qiita-orch)
    partition: str  # SLURM partition (e.g. "qiita")
    account: str  # SLURM account for usage reporting
    api_version: str  # default v0.0.40
    poll_interval_seconds: int
    job_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class Settings:
    backend_type: str
    shared_filesystem_root: str
    # The shared bearer token CP-to-CO calls present. Loaded from a file
    # in production; env-var fallback only when QIITA_ALLOW_TOKEN_ENV=true.
    cp_to_co_token: str
    # SLURM config — non-None only when backend_type=slurm.
    slurm: SlurmSettings | None = None

    @classmethod
    def from_env(cls) -> Settings:
        backend_type = os.environ.get("COMPUTE_BACKEND", BACKEND_LOCAL)
        slurm = _resolve_slurm_settings() if backend_type == BACKEND_SLURM else None
        return cls(
            backend_type=backend_type,
            shared_filesystem_root=os.environ.get(
                "SHARED_FILESYSTEM_ROOT",
                os.environ.get("TMPDIR", "/tmp") + "/qiita",
            ),
            cp_to_co_token=_resolve_cp_to_co_token(),
            slurm=slurm,
        )


def _resolve_slurm_settings() -> SlurmSettings:
    """Read SLURM env vars and bail loudly on missing-required so a
    misconfigured `COMPUTE_BACKEND=slurm` boot fails at startup instead
    of at the first /step/run call."""

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
    )


def _resolve_cp_to_co_token() -> str:
    """Resolve the inbound bearer token shared with the control plane.

    Order of precedence:
      1. CP_TO_CO_TOKEN_PATH (default /etc/qiita/cp-to-co.token).
         If the file exists, use it. The env-var path is the production
         drop-in.
      2. CP_TO_CO_TOKEN (env var). Only honoured when
         QIITA_ALLOW_TOKEN_ENV=true — dev / CI explicitly opt in; prod
         never sets the flag, so a leaked env var alone can't drive auth.
    """
    path = Path(os.environ.get("CP_TO_CO_TOKEN_PATH", DEFAULT_CP_TO_CO_TOKEN_PATH))
    if path.is_file():
        return path.read_text().strip()

    if os.environ.get("QIITA_ALLOW_TOKEN_ENV", "false").lower() == "true":
        token = os.environ.get("CP_TO_CO_TOKEN")
        if token:
            return token.strip()

    raise RuntimeError(
        f"orchestrator: no CP↔CO token available. Either install the token at"
        f" {path} (mode 0400) or set QIITA_ALLOW_TOKEN_ENV=true and"
        f" CP_TO_CO_TOKEN for dev/CI."
    )
