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

# Default install location for the shared CP↔CO bearer token in
# production (file mode 0400, owned by the orchestrator user). Mirrors
# the pattern at /etc/qiita/orchestrator.token used elsewhere.
DEFAULT_CP_TO_CO_TOKEN_PATH = "/etc/qiita/cp-to-co.token"

BACKEND_LOCAL = "local"
BACKEND_SLURM = "slurm"


@dataclass(frozen=True, slots=True)
class Settings:
    backend_type: str
    shared_filesystem_root: str
    # The shared bearer token CP-to-CO calls present. Loaded from a file
    # in production; env-var fallback only when QIITA_ALLOW_TOKEN_ENV=true.
    cp_to_co_token: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            backend_type=os.environ.get("COMPUTE_BACKEND", BACKEND_LOCAL),
            shared_filesystem_root=os.environ.get(
                "SHARED_FILESYSTEM_ROOT",
                os.environ.get("TMPDIR", "/tmp") + "/qiita",
            ),
            cp_to_co_token=_resolve_cp_to_co_token(),
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
