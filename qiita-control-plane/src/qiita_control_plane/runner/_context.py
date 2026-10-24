"""The values one workflow run holds fixed, built once by `run_workflow`."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import asyncpg
from qiita_common.actions import ActionCeiling
from qiita_common.compute_backend_client import ComputeBackendClient


@dataclass(frozen=True)
class RunContext:
    """What `run_workflow` passes unchanged to every entry it dispatches.

    Values that change per entry or per attempt (the entry, the bound outputs,
    the attempt workspace, the resource overrides) stay parameters of the
    functions that take this."""

    pool: asyncpg.Pool
    work_ticket_idx: int
    backend_client: ComputeBackendClient
    signing_key: bytes
    data_plane_url: str
    scope_target: dict[str, Any]
    action_ceiling: ActionCeiling
    max_retries: int
    poll_interval_seconds: float
    resume: bool
    dispatch_cb: Callable[[int], Any] | None
    max_inflight: int
