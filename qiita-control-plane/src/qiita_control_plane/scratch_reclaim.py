"""Reclaim finished tickets' scratch workspaces, keeping each attempt's `logs/`.

Only the children of `ticket/<idx>/<step>/attempt-<n>/` other than `logs/` are deleted,
so the step-logs route keeps serving and the ticket's directory skeleton stays.
"""

from __future__ import annotations

import os
import stat
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import asyncpg
from qiita_common.actions import STEP_LOGS_SUBDIR
from qiita_common.models import TERMINAL_STEP_PROGRESS_STATES, WorkTicketState

from .backfill.syndna_read_count import plan_backfill as plan_syndna_backfill
from .ena_import import DOWNLOAD_ENA_STUDY_ACTION_ID
from .ena_import.registration import fetch_pool_download_ticket
from .workspace import ATTEMPT_DIR_PREFIX, ticket_workspace

RECLAIMABLE_STATES = (WorkTicketState.COMPLETED.value, WorkTicketState.NO_DATA.value)
_SUPERSEDABLE_STATES = (WorkTicketState.FAILED.value, WorkTicketState.CANCELLED.value)

# Allocated in the advisory-lock registry in `repositories/__init__.py`.
RECLAIM_LOCK_KEY = 4_310_290_150

_MAX_BIGINT = 2**63 - 1

_ELIGIBLE_SQL = """
SELECT wt.work_ticket_idx, wt.action_id, wt.state, wt.sequenced_pool_idx
  FROM qiita.work_ticket wt
 WHERE wt.updated_at < $1
   AND ($2::text IS NULL OR wt.action_id = $2)
   AND ($3::bigint IS NULL OR wt.work_ticket_idx = $3)
   AND (wt.state = ANY($4::qiita.work_ticket_state[])
        OR (wt.state = ANY($5::qiita.work_ticket_state[])
            AND wt.action_id = $6
            AND wt.sequenced_pool_idx IS NOT NULL))
   AND NOT EXISTS (
         SELECT 1 FROM qiita.work_ticket_step wts
          WHERE wts.work_ticket_idx = wt.work_ticket_idx
            AND wts.state <> ALL($7::text[])
       )
 ORDER BY wt.work_ticket_idx
"""


class ReclaimLockHeld(RuntimeError):
    """Another reclaim run holds the advisory lock."""


@dataclass(frozen=True, slots=True)
class Usage:
    total_bytes: int
    reclaimable_bytes: int
    log_bytes: int
    inodes: int
    kept_inodes: int


_NO_USAGE = Usage(0, 0, 0, 0, 0)


@dataclass(frozen=True, slots=True)
class TicketPlan:
    """One workspace; an orphan (no work_ticket row) has no action or state."""

    work_ticket_idx: int
    action_id: str | None
    state: str | None
    usage: Usage
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ReclaimPlan:
    ticket_root: Path
    cutoff: datetime
    action_id: str | None
    tickets: list[TicketPlan] = field(default_factory=list)
    orphans: list[TicketPlan] = field(default_factory=list)
    syndna_skipped: list[int] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ApplyResult:
    reclaimed: list[TicketPlan] = field(default_factory=list)
    skipped: list[tuple[int, str]] = field(default_factory=list)
    failures: list[tuple[int, str]] = field(default_factory=list)


def _workspace_dir(ticket_root: Path, work_ticket_idx: int) -> Path:
    root = ticket_root.resolve()
    path = ticket_workspace(root, work_ticket_idx)
    if path.is_symlink():
        raise ValueError(f"{path} is a symlink; refusing to reclaim it")
    if path.resolve().parent != root:
        raise ValueError(f"{path} is not directly under {root}")
    return path


def _raise(exc: OSError) -> None:
    raise exc


def _attempt_dirs(ticket_dir: Path) -> list[Path]:
    attempts = []
    for step in os.scandir(ticket_dir):
        if not step.is_dir(follow_symlinks=False):
            continue
        for attempt in os.scandir(step.path):
            if attempt.is_dir(follow_symlinks=False) and attempt.name.startswith(
                ATTEMPT_DIR_PREFIX
            ):
                attempts.append(Path(attempt.path))
    return attempts


def measure_workspace(ticket_root: Path, work_ticket_idx: int) -> Usage:
    """Size one workspace per inode. An inode is reclaimable only when every one of its
    links sits in the deleted area; the rest of its bytes are still held elsewhere."""
    ticket_dir = _workspace_dir(ticket_root, work_ticket_idx)
    seen: dict[tuple[int, int], os.stat_result] = {}
    freed_links: dict[tuple[int, int], int] = {}
    kept: set[tuple[int, int]] = set()
    in_logs: set[tuple[int, int]] = set()

    def visit(path: str, parts: tuple[str, ...]) -> None:
        st = os.lstat(path)
        key = (st.st_dev, st.st_ino)
        seen[key] = st
        under_attempt = len(parts) >= 3 and parts[1].startswith(ATTEMPT_DIR_PREFIX)
        if under_attempt and parts[2] != STEP_LOGS_SUBDIR:
            freed_links[key] = freed_links.get(key, 0) + 1
            return
        kept.add(key)
        if under_attempt:
            in_logs.add(key)

    visit(str(ticket_dir), ())
    for dirpath, dirnames, filenames in os.walk(ticket_dir, onerror=_raise):
        base = Path(dirpath).relative_to(ticket_dir).parts
        for name in dirnames + filenames:
            visit(os.path.join(dirpath, name), (*base, name))

    files = {k: st for k, st in seen.items() if not stat.S_ISDIR(st.st_mode)}
    return Usage(
        total_bytes=sum(st.st_size for st in files.values()),
        reclaimable_bytes=sum(
            st.st_size for k, st in files.items() if freed_links.get(k) == st.st_nlink
        ),
        log_bytes=sum(st.st_size for k, st in files.items() if k in in_logs),
        inodes=len(seen),
        kept_inodes=len(kept),
    )


def _remove_tree(path: str, collect) -> None:
    """Delete `path` without following symlinks, restoring owner write on read-only
    directories we own (container output dirs are 0550)."""
    try:
        st = os.lstat(path)
        if st.st_uid == os.geteuid() and not st.st_mode & stat.S_IWUSR:
            os.chmod(path, st.st_mode | stat.S_IWUSR)
        entries = list(os.scandir(path))
    except OSError as exc:
        collect(os.chmod, path, exc)
        return
    for entry in entries:
        if entry.is_dir(follow_symlinks=False):
            _remove_tree(entry.path, collect)
            continue
        try:
            os.unlink(entry.path)
        except OSError as exc:
            collect(os.unlink, entry.path, exc)
    try:
        os.rmdir(path)
    except OSError as exc:
        collect(os.rmdir, path, exc)


def reclaim_workspace(ticket_root: Path, work_ticket_idx: int) -> list[str]:
    """Delete everything under each attempt except `logs/`; return the failures, not raise."""
    errors: list[str] = []

    def collect(_func, path, exc) -> None:
        errors.append(f"{path}: {exc}")

    try:
        for attempt in _attempt_dirs(_workspace_dir(ticket_root, work_ticket_idx)):
            for child in list(os.scandir(attempt)):
                if child.name == STEP_LOGS_SUBDIR:
                    continue
                if child.is_dir(follow_symlinks=False):
                    _remove_tree(child.path, collect)
                    continue
                try:
                    os.unlink(child.path)
                except OSError as exc:
                    collect(os.unlink, child.path, exc)
    except (OSError, ValueError) as exc:
        errors.append(str(exc))
    return errors


@asynccontextmanager
async def reclaim_lock(pool: asyncpg.Pool) -> AsyncIterator[None]:
    async with pool.acquire() as conn:
        if not await conn.fetchval("SELECT pg_try_advisory_lock($1)", RECLAIM_LOCK_KEY):
            raise ReclaimLockHeld("another scratch reclaim is running")
        try:
            yield
        finally:
            await conn.fetchval("SELECT pg_advisory_unlock($1)", RECLAIM_LOCK_KEY)


async def _eligible(
    pool: asyncpg.Pool,
    *,
    cutoff: datetime,
    action_id: str | None,
    work_ticket_idx: int | None = None,
) -> list[asyncpg.Record]:
    rows = await pool.fetch(
        _ELIGIBLE_SQL,
        cutoff,
        action_id,
        work_ticket_idx,
        list(RECLAIMABLE_STATES),
        list(_SUPERSEDABLE_STATES),
        DOWNLOAD_ENA_STUDY_ACTION_ID,
        [s.value for s in TERMINAL_STEP_PROGRESS_STATES],
    )
    eligible = []
    for row in rows:
        if row["state"] in _SUPERSEDABLE_STATES:
            latest = await fetch_pool_download_ticket(
                pool, sequenced_pool_idx=row["sequenced_pool_idx"]
            )
            if latest is None or latest["work_ticket_idx"] <= row["work_ticket_idx"]:
                continue
        eligible.append(row)
    return eligible


def _measured(ticket_root: Path, idx: int, action_id: str | None, state: str | None) -> TicketPlan:
    try:
        return TicketPlan(idx, action_id, state, measure_workspace(ticket_root, idx))
    except (OSError, ValueError) as exc:
        return TicketPlan(idx, action_id, state, _NO_USAGE, error=str(exc))


async def _orphan_idxs(pool: asyncpg.Pool, ticket_root: Path) -> list[int]:
    named = sorted(
        int(e.name)
        for e in os.scandir(ticket_root)
        if e.name.isascii()
        and e.name.isdigit()
        and str(int(e.name)) == e.name
        and int(e.name) <= _MAX_BIGINT
        and (e.is_dir(follow_symlinks=False) or e.is_symlink())
    )
    known = await pool.fetch(
        "SELECT work_ticket_idx FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])",
        named,
    )
    known_idxs = {r["work_ticket_idx"] for r in known}
    return [i for i in named if i not in known_idxs]


async def plan_reclaim(
    pool: asyncpg.Pool, *, ticket_root: Path, cutoff: datetime, action_id: str | None = None
) -> ReclaimPlan:
    """Read-only: the eligible workspaces, the SynDNA-backfill skips, and the orphans."""
    root = ticket_root.resolve()
    backfill = await plan_syndna_backfill(pool, ticket_root=root)
    needed = {p.work_ticket_idx for p in backfill.writable()}
    tickets, skipped = [], []
    for row in await _eligible(pool, cutoff=cutoff, action_id=action_id):
        idx = row["work_ticket_idx"]
        if not os.path.lexists(ticket_workspace(root, idx)):
            continue
        if idx in needed:
            skipped.append(idx)
            continue
        tickets.append(_measured(root, idx, row["action_id"], row["state"]))
    orphans = [_measured(root, idx, None, None) for idx in await _orphan_idxs(pool, root)]
    return ReclaimPlan(root, cutoff, action_id, tickets, orphans, skipped)


async def apply_reclaim(
    pool: asyncpg.Pool, plan: ReclaimPlan, *, include_orphans: bool = False
) -> ApplyResult:
    """Reclaim each planned workspace after re-checking it is still eligible (or still an
    orphan); one ticket's failure does not stop the rest."""
    result = ApplyResult()
    targets = [(t, False) for t in plan.tickets]
    if include_orphans:
        targets += [(o, True) for o in plan.orphans]
    for target, orphan in targets:
        idx = target.work_ticket_idx
        if target.error:
            result.failures.append((idx, target.error))
        elif orphan and await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM qiita.work_ticket WHERE work_ticket_idx = $1)", idx
        ):
            result.skipped.append((idx, "now has a work_ticket row"))
        elif not orphan and not await _eligible(
            pool, cutoff=plan.cutoff, action_id=plan.action_id, work_ticket_idx=idx
        ):
            result.skipped.append((idx, "no longer eligible"))
        elif errors := reclaim_workspace(plan.ticket_root, idx):
            result.failures.append((idx, f"{len(errors)} error(s); first: {errors[0]}"))
        else:
            result.reclaimed.append(target)
    return result
