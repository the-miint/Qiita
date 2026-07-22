"""Batch multi-study ENA import driver (TASK-06).

`create_ena_import_batch` INSERTs one `qiita.ena_import_batch` row plus one
`qiita.ena_import_batch_item` row per accession (state `pending`) and
returns immediately -- the route it backs (`routes.ena_import`) responds
202 with the handle. `schedule_ena_import_batch` then fires ONE background
`asyncio.Task` (this module's OWN tracked task set,
`app.state.running_ena_import_batches`, mirroring `dispatch.py`'s
`running_dispatches` / `drain_running_dispatches` -- this task drives
`register_ena_study` + `submit_work_ticket_core` directly, not a
work_ticket/`ComputeBackendClient` workflow run, so it does not belong in
`dispatch.py`'s task set).

The background task (`_run_batch`) processes every item with BOUNDED
concurrency (`asyncio.Semaphore(_STUDY_CONCURRENCY)`) -- both to respect
miint's ENAClient rate limit (~3 req/s outbound to ENA) and to bound total
concurrent DB writers. Each item is wrapped in its own try/except
(`_process_one_study`): resolve (blocking DuckDB+miint calls run under
`asyncio.to_thread`) -> `register_ena_study` -> one `download-ena-study`
work-ticket submission per pool the study created
(`submit.build_download_ena_study_ticket` +
`routes.work_ticket.submit_work_ticket_core`, in-process, propagating the
BATCH's submitting principal so that ticket's own audience gate is
enforced against a real principal, never bypassed). One accession's
failure marks only that item `failed` -- the batch itself never fails
(T06-3).

`reconcile_inflight_batches` (called from `main.py`'s lifespan startup,
alongside `dispatch.reconcile_inflight_tickets`) re-drives every item still
`pending`/`resolving` after a CP restart -- `register_ena_study` is
idempotent (T02-5) so re-driving from `pending` is always safe, even if a
prior resolve partially ran.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import asyncpg
from fastapi import FastAPI
from qiita_common.models import NON_TERMINAL_WORK_TICKET_STATES, WorkTicketState
from qiita_common.models.ena import ResolverKind, SourceArchive
from qiita_common.models.ena_import import BatchImportItem, BatchImportStatus, BatchItemState

from ..auth.principal import HumanUser
from ..auth.scopes import role_ceiling
from .accession import validate_study_accession
from .factory import get_resolver
from .registration import register_ena_study
from .submit import build_download_ena_study_ticket

_log = logging.getLogger(__name__)

# Bounded concurrency for the resolve+register phase. Small on purpose:
# duckdb-miint's ENAClient rate-limits outbound ENA Portal/Browser calls to
# ~3 req/s (duckdb-miint/src/ena_client.cpp); a handful of concurrently
# in-flight studies keeps the batch well under that ceiling without
# serializing the whole run.
_STUDY_CONCURRENCY = 4

# Shutdown-drain bound for this module's background task set, mirroring
# dispatch.py's _DISPATCH_DRAIN_TIMEOUT_SECONDS.
_BATCH_DRAIN_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True)
class BatchImportItemHandle:
    """One item's identity, as returned by `create_ena_import_batch` and
    consumed by `_run_batch` / `reconcile_inflight_batches`. Deliberately
    thin -- just enough to drive `_process_one_study`, not the full
    `BatchImportItem` wire shape."""

    idx: int
    ena_study_accession: str


async def _load_principal(pool: asyncpg.Pool, principal_idx: int) -> HumanUser:
    """Reconstruct the submitting `HumanUser` from a principal_idx.

    The background task and startup reconcile run with no live HTTP
    request, so there is no request-bound `Principal` to reuse. This is a
    small, local re-implementation of `auth.principal._build_human_user`'s
    query (kept local rather than importing that module-private helper
    across modules) -- `role_ceiling` (public, `auth.scopes`) supplies the
    same scope set an OIDC-resolved session would carry.
    """
    row = await pool.fetchrow(
        "SELECT p.idx, p.system_role, p.disabled, p.retired, u.email, u.profile_complete"
        " FROM qiita.principal p JOIN qiita.user u ON u.principal_idx = p.idx"
        " WHERE p.idx = $1",
        principal_idx,
    )
    if row is None:
        raise RuntimeError(
            f"principal {principal_idx} not found (or not a human user);"
            " cannot submit/re-drive ena_import_batch work on its behalf"
        )
    return HumanUser(
        principal_idx=row["idx"],
        email=row["email"],
        system_role=row["system_role"],
        scopes=role_ceiling(row["system_role"]),
        profile_complete=row["profile_complete"],
        disabled=row["disabled"],
        retired=row["retired"],
    )


async def create_ena_import_batch(
    pool: asyncpg.Pool,
    *,
    accessions: list[str],
    principal: HumanUser,
    resolver_backend: str,
    source_archive: SourceArchive,
    download_method: str,
) -> tuple[int, list[BatchImportItemHandle]]:
    """INSERT the batch row + one `pending` item per accession, synchronously.

    Validates every accession's shape up front (`ena_import.accession`,
    fail-loud on a malformed accession) BEFORE writing anything -- a
    batch containing one garbage accession never partially lands.
    `resolver_backend` is validated the same way (`get_resolver` raises
    `ValueError` on an unrecognized name; the resolver instance itself is
    discarded here -- each item builds its own via `get_resolver` again
    inside `_process_one_study`). Returns the new batch idx and the
    created item handles in submitted order; the caller (the route) fires
    the background processing task next via `schedule_ena_import_batch`.
    """
    validated = [validate_study_accession(a) for a in accessions]
    get_resolver(resolver_backend)  # fail loud on an unrecognized backend

    async with pool.acquire() as conn, conn.transaction():
        batch_idx = await conn.fetchval(
            "INSERT INTO qiita.ena_import_batch"
            " (submitted_by_principal_idx, resolver_backend, source_archive, download_method)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            principal.principal_idx,
            resolver_backend,
            source_archive.value,
            download_method,
        )
        items: list[BatchImportItemHandle] = []
        for accession in validated:
            item_idx = await conn.fetchval(
                "INSERT INTO qiita.ena_import_batch_item (batch_idx, ena_study_accession)"
                " VALUES ($1, $2) RETURNING idx",
                batch_idx,
                accession,
            )
            items.append(BatchImportItemHandle(idx=item_idx, ena_study_accession=accession))
    return batch_idx, items


async def _set_item_state(
    pool: asyncpg.Pool, item_idx: int, state: BatchItemState, *, failure_reason: str | None = None
) -> None:
    await pool.execute(
        "UPDATE qiita.ena_import_batch_item SET state = $2, failure_reason = $3 WHERE idx = $1",
        item_idx,
        state.value,
        failure_reason,
    )


async def _set_item_registered(pool: asyncpg.Pool, item_idx: int, *, study_idx: int) -> None:
    await pool.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET state = $2, study_idx = $3, failure_reason = NULL"
        " WHERE idx = $1",
        item_idx,
        BatchItemState.REGISTERED.value,
        study_idx,
    )


async def _set_item_downloading(
    pool: asyncpg.Pool, item_idx: int, *, work_ticket_idxs: list[int]
) -> None:
    await pool.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET state = $2, download_work_ticket_idxs = $3"
        " WHERE idx = $1",
        item_idx,
        BatchItemState.DOWNLOADING.value,
        work_ticket_idxs,
    )


async def _process_one_study(
    app: FastAPI,
    pool: asyncpg.Pool,
    *,
    item: BatchImportItemHandle,
    principal: HumanUser,
    resolver_backend: str,
    source_archive: SourceArchive,
    resolver_kind: ResolverKind,
) -> None:
    """Resolve + register ONE study, then submit one download-ena-study
    ticket per pool it created. Never raises -- every failure mode
    (resolve, register, ticket submission) is caught and recorded as this
    item's `failed` state, so one bad accession in a batch can never
    affect any other item or the batch as a whole (T06-3).

    Blocking DuckDB+miint resolver calls run under `asyncio.to_thread` so
    they don't stall the event loop the other concurrently-processing
    items share -- mirrors `cli.reference_load`'s and `actions.library`'s
    use of `asyncio.to_thread` around blocking DuckDB work.
    """
    try:
        await _set_item_state(pool, item.idx, BatchItemState.RESOLVING)
        resolver = get_resolver(resolver_backend)
        study_header = await asyncio.to_thread(
            resolver.resolve_study_header, item.ena_study_accession
        )
        runs = await asyncio.to_thread(resolver.resolve_runs, item.ena_study_accession)
        sample_attributes = await asyncio.to_thread(
            resolver.resolve_sample_attributes, item.ena_study_accession
        )

        result = await register_ena_study(
            pool,
            study_header=study_header,
            runs=runs,
            sample_attributes=sample_attributes,
            owner_idx=principal.principal_idx,
            caller_idx=principal.principal_idx,
            source_archive=source_archive,
            resolver_kind=resolver_kind,
        )
        await _set_item_registered(pool, item.idx, study_idx=result.study_idx)

        # Local import: `routes.work_ticket` is the HTTP-interface layer and
        # does not import `ena_import` (no cycle) -- but the direction is
        # deliberately unusual, so it's kept as an explicit, narrow import
        # rather than a module-level one. Reuse is the point: this MUST be
        # the exact same audience/scope/disallow-without-delete gate a real
        # `POST /work-ticket` submission goes through (see
        # `submit_work_ticket_core`'s own docstring).
        from ..routes.work_ticket import submit_work_ticket_core

        work_ticket_idxs: list[int] = []
        for created_pool in result.created_pools:
            body = build_download_ena_study_ticket(
                sequenced_pool_idx=created_pool.sequenced_pool_idx,
                sequencing_run_idx=created_pool.sequencing_run_idx,
                ena_study_accession=study_header.study_accession,
            )
            response = await submit_work_ticket_core(app=app, principal=principal, body=body)
            work_ticket_idxs.append(response.work_ticket_idx)

        await _set_item_downloading(pool, item.idx, work_ticket_idxs=work_ticket_idxs)
    except Exception as exc:  # noqa: BLE001 -- per-study isolation is the point
        # (T06-3): one accession's failure must never abort its siblings or
        # the batch as a whole. Recorded on this item, never swallowed
        # silently -- callers see it via GET /ena-import-batch/{idx}.
        _log.warning(
            "ena_import_batch item %d (%s) failed: %s",
            item.idx,
            item.ena_study_accession,
            exc,
        )
        await _set_item_state(pool, item.idx, BatchItemState.FAILED, failure_reason=str(exc))


async def _run_batch(
    app: FastAPI,
    pool: asyncpg.Pool,
    *,
    items: list[BatchImportItemHandle],
    principal: HumanUser,
    resolver_backend: str,
    source_archive: SourceArchive,
    resolver_kind: ResolverKind,
) -> None:
    """Process every item with bounded concurrency. Never raises -- each
    item's own try/except in `_process_one_study` absorbs its failure."""
    semaphore = asyncio.Semaphore(_STUDY_CONCURRENCY)

    async def _bounded(item: BatchImportItemHandle) -> None:
        async with semaphore:
            await _process_one_study(
                app,
                pool,
                item=item,
                principal=principal,
                resolver_backend=resolver_backend,
                source_archive=source_archive,
                resolver_kind=resolver_kind,
            )

    await asyncio.gather(*[_bounded(item) for item in items])


def schedule_ena_import_batch(
    app: FastAPI,
    *,
    items: list[BatchImportItemHandle],
    principal: HumanUser,
    resolver_backend: str,
    source_archive: SourceArchive,
    resolver_kind: ResolverKind,
) -> asyncio.Task:
    """Fire-and-forget the batch's resolve+register+submit background task.

    Registered on this module's OWN tracked task set
    (`app.state.running_ena_import_batches`) -- separate from
    `dispatch.schedule_dispatch`'s `running_dispatches` because this task
    drives `register_ena_study` + `submit_work_ticket_core` directly, not a
    work_ticket/`ComputeBackendClient` run; it does not belong in (or need)
    `dispatch.py`'s task set or drain.
    """
    task = asyncio.create_task(
        _run_batch(
            app,
            app.state.pool,
            items=items,
            principal=principal,
            resolver_backend=resolver_backend,
            source_archive=source_archive,
            resolver_kind=resolver_kind,
        ),
        name="ena_import_batch",
    )
    app.state.running_ena_import_batches.add(task)
    task.add_done_callback(app.state.running_ena_import_batches.discard)
    return task


async def drain_running_ena_import_batches(
    running: set[asyncio.Task], *, timeout_seconds: float = _BATCH_DRAIN_TIMEOUT_SECONDS
) -> None:
    """Shutdown-drain twin of `dispatch.drain_running_dispatches`, scoped
    to this module's own task set. Anything still running past the
    deadline is cancelled; its items stay `pending`/`resolving`/whatever
    they were and are re-driven by `reconcile_inflight_batches` on the
    next startup."""
    if not running:
        return
    pending = list(running)
    _log.info(
        "draining %d in-flight ena_import_batch task(s) (timeout=%.0fs)",
        len(pending),
        timeout_seconds,
    )
    _, still_pending = await asyncio.wait(pending, timeout=timeout_seconds)
    for task in still_pending:
        task.cancel()
    if still_pending:
        _log.warning(
            "cancelled %d ena_import_batch task(s) that did not drain in time;"
            " their in-flight items will be re-driven by"
            " reconcile_inflight_batches on next startup",
            len(still_pending),
        )


async def reconcile_inflight_batches(app: FastAPI) -> int:
    """Re-drive every batch item still `pending`/`resolving` at startup.

    Mirrors `dispatch.reconcile_inflight_tickets`: a CP restart leaves any
    item that hadn't reached `registered` with no live owner.
    `register_ena_study` is idempotent (T02-5), so re-driving from
    `pending` is safe even if a prior resolve partially ran. Items are
    grouped by batch so each batch's in-flight items share one background
    task + one bounded-concurrency semaphore, same as a fresh submission.
    Returns the number of items scheduled for re-drive, for logging.
    """
    pool = app.state.pool
    rows = await pool.fetch(
        "SELECT bi.idx, bi.ena_study_accession, bi.batch_idx,"
        "       b.submitted_by_principal_idx, b.resolver_backend, b.source_archive"
        " FROM qiita.ena_import_batch_item bi"
        " JOIN qiita.ena_import_batch b ON b.idx = bi.batch_idx"
        " WHERE bi.state = ANY($1::text[])"
        " ORDER BY bi.batch_idx, bi.idx",
        [BatchItemState.PENDING.value, BatchItemState.RESOLVING.value],
    )
    if not rows:
        return 0

    by_batch: dict[int, list[asyncpg.Record]] = {}
    for row in rows:
        by_batch.setdefault(row["batch_idx"], []).append(row)

    total = 0
    for batch_idx, batch_rows in by_batch.items():
        principal_idx = batch_rows[0]["submitted_by_principal_idx"]
        resolver_backend = batch_rows[0]["resolver_backend"]
        source_archive = SourceArchive(batch_rows[0]["source_archive"])
        try:
            principal = await _load_principal(pool, principal_idx)
        except RuntimeError:
            _log.exception(
                "cannot re-drive ena_import_batch %d -- submitting principal %d unresolvable",
                batch_idx,
                principal_idx,
            )
            continue
        items = [
            BatchImportItemHandle(idx=r["idx"], ena_study_accession=r["ena_study_accession"])
            for r in batch_rows
        ]
        _log.warning(
            "re-driving %d in-flight ena_import_batch_item row(s) for batch %d at startup",
            len(items),
            batch_idx,
        )
        schedule_ena_import_batch(
            app,
            items=items,
            principal=principal,
            resolver_backend=resolver_backend,
            source_archive=source_archive,
            resolver_kind=ResolverKind(resolver_backend),
        )
        total += len(items)
    return total


async def fetch_batch_status(pool: asyncpg.Pool, *, batch_idx: int) -> BatchImportStatus | None:
    """Read a batch's current, rolled-up per-item status. Returns None if
    `batch_idx` names no row.

    For an item in `downloading`, its `download_work_ticket_idxs`' current
    `qiita.work_ticket.state` are rolled up ON DEMAND (never persisted
    back onto the item row -- this is a pure read):
      - any ticket `failed`     -> reported as `failed` (download), with a
                                    reason naming the failed ticket(s);
                                    the BATCH itself is never marked failed.
      - any ticket non-terminal -> stays `downloading`.
      - every ticket terminal-success (`completed`/`no_data`) -> `done`.
    Every other persisted state (`pending`/`resolving`/`registered`/
    `failed`) passes through unchanged.
    """
    exists = await pool.fetchval("SELECT 1 FROM qiita.ena_import_batch WHERE idx = $1", batch_idx)
    if exists is None:
        return None

    item_rows = await pool.fetch(
        "SELECT idx, ena_study_accession, state, failure_reason, study_idx,"
        "       download_work_ticket_idxs"
        " FROM qiita.ena_import_batch_item"
        " WHERE batch_idx = $1"
        " ORDER BY idx",
        batch_idx,
    )

    all_ticket_idxs = sorted({idx for row in item_rows for idx in row["download_work_ticket_idxs"]})
    ticket_states: dict[int, str] = {}
    if all_ticket_idxs:
        ticket_rows = await pool.fetch(
            "SELECT work_ticket_idx, state FROM qiita.work_ticket"
            " WHERE work_ticket_idx = ANY($1::bigint[])",
            all_ticket_idxs,
        )
        ticket_states = {r["work_ticket_idx"]: r["state"] for r in ticket_rows}

    items: list[BatchImportItem] = []
    for row in item_rows:
        state = BatchItemState(row["state"])
        failure_reason = row["failure_reason"]
        ticket_idxs = list(row["download_work_ticket_idxs"])
        if state == BatchItemState.DOWNLOADING and ticket_idxs:
            states = [ticket_states.get(idx) for idx in ticket_idxs]
            failed_idxs = [
                idx
                for idx, s in zip(ticket_idxs, states, strict=True)
                if s == WorkTicketState.FAILED.value
            ]
            if failed_idxs:
                state = BatchItemState.FAILED
                failure_reason = f"download work_ticket(s) failed: {failed_idxs}"
            elif any(s in NON_TERMINAL_WORK_TICKET_STATES for s in states):
                state = BatchItemState.DOWNLOADING
            else:
                state = BatchItemState.DONE
        items.append(
            BatchImportItem(
                ena_study_accession=row["ena_study_accession"],
                state=state,
                study_idx=row["study_idx"],
                failure_reason=failure_reason,
                download_work_ticket_idxs=ticket_idxs,
            )
        )
    return BatchImportStatus(ena_import_batch_idx=batch_idx, items=items)
