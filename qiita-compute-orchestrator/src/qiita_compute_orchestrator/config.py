"""Compute orchestrator configuration."""

import os
from dataclasses import dataclass
from pathlib import Path

from qiita_common.config import require_env


@dataclass(frozen=True, slots=True)
class Settings:
    control_plane_url: str
    backend_type: str
    shared_filesystem_root: str
    # Phase I auth: exactly one of these is populated. api_token_path is the
    # default / production path (file mode 0400 owned by the qiita user).
    # api_token (env var) is the dev/CI escape hatch, only honoured when
    # QIITA_ALLOW_TOKEN_ENV=true.
    api_token_path: Path | None
    api_token: str | None

    @classmethod
    def from_env(cls) -> Settings:
        control_plane_url = require_env("CONTROL_PLANE_URL")
        backend_type = os.environ.get("COMPUTE_BACKEND", "local")
        shared_filesystem_root = os.environ.get(
            "SHARED_FILESYSTEM_ROOT",
            os.environ.get("TMPDIR", "/tmp") + "/qiita",
        )

        api_token, api_token_path = _resolve_api_token()

        return cls(
            control_plane_url=control_plane_url,
            backend_type=backend_type,
            shared_filesystem_root=shared_filesystem_root,
            api_token_path=api_token_path,
            api_token=api_token,
        )


def _resolve_api_token() -> tuple[str | None, Path | None]:
    """Resolve the orchestrator's control-plane PAT.

    Order of precedence:
      1. CONTROL_PLANE_API_TOKEN_PATH (default /etc/qiita/orchestrator.token).
         If the file exists, use it. The env-var path is the production
         drop-in.
      2. CONTROL_PLANE_API_TOKEN (env var). Only honoured when
         QIITA_ALLOW_TOKEN_ENV=true — dev / CI explicitly opt in; prod
         never sets the flag, so a leaked env var alone can't drive auth.

    Raises RuntimeError with an actionable message if neither source yields
    a token. Returns (api_token, api_token_path) where exactly one is set.
    """
    path_str = os.environ.get("CONTROL_PLANE_API_TOKEN_PATH", "/etc/qiita/orchestrator.token")
    path = Path(path_str)
    if path.is_file():
        return (None, path)

    if os.environ.get("QIITA_ALLOW_TOKEN_ENV", "false").lower() == "true":
        token = os.environ.get("CONTROL_PLANE_API_TOKEN")
        if token:
            return (token.strip(), None)

    raise RuntimeError(
        f"orchestrator: no API token available. Either install the token at"
        f" {path_str} (mode 0400) or set QIITA_ALLOW_TOKEN_ENV=true and"
        f" CONTROL_PLANE_API_TOKEN for dev/CI."
    )
