"""A `RunContext` for tests that call a runner function directly."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..fanout_dispatch import DEFAULT_FANOUT_MAX_INFLIGHT
from ..runner._context import RunContext
from ..step_progress import StepProgressRow


def run_context(
    *,
    pool: Any,
    work_ticket_idx: int,
    scope_target: dict[str, Any],
    backend_client: Any = None,
    signing_key: bytes | None = None,
    data_plane_url: str | None = None,
    action_ceiling: Any = None,
    max_retries: int | None = None,
    poll_interval_seconds: float | None = None,
    workspace: Path | None = None,
    prior_progress: list[StepProgressRow] | None = None,
    resume: bool = False,
    dispatch_cb: Callable[[int], Any] | None = None,
    max_inflight: int = DEFAULT_FANOUT_MAX_INFLIGHT,
    mem_gb_override: int | None = None,
) -> RunContext:
    """The context a run would build. `resume`, `dispatch_cb` and
    `max_inflight` default as `run_workflow`'s do, and `mem_gb_override` as for
    a ticket with no resource override; every other argument the test does not
    name is `None`, so a path that reaches one fails on it rather than using a
    stand-in."""
    return RunContext(
        pool=pool,
        work_ticket_idx=work_ticket_idx,
        backend_client=backend_client,
        signing_key=signing_key,
        data_plane_url=data_plane_url,
        scope_target=scope_target,
        action_ceiling=action_ceiling,
        max_retries=max_retries,
        poll_interval_seconds=poll_interval_seconds,
        resume=resume,
        dispatch_cb=dispatch_cb,
        max_inflight=max_inflight,
        mem_gb_override=mem_gb_override,
        workspace=workspace,
        prior_progress=prior_progress,
    )
