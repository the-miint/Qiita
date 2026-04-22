"""Compute orchestrator configuration."""

import os
from dataclasses import dataclass

from qiita_common.config import require_env


@dataclass(frozen=True, slots=True)
class Settings:
    control_plane_url: str
    backend_type: str
    shared_filesystem_root: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            control_plane_url=require_env("CONTROL_PLANE_URL"),
            backend_type=os.environ.get("COMPUTE_BACKEND", "local"),
            shared_filesystem_root=os.environ.get(
                "SHARED_FILESYSTEM_ROOT",
                os.environ.get("TMPDIR", "/tmp") + "/qiita",
            ),
        )
