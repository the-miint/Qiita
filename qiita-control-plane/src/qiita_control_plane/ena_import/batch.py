"""Batch multi-study ENA import driver.

`create_ena_import_batch` INSERTs one `qiita.ena_import_batch` row plus one
`pending` `ena_import_batch_item` per accession and returns immediately (the
route responds 202). `schedule_ena_import_batch` fires ONE background task on
this module's own tracked set `app.state.running_ena_import_batches` (mirroring
`dispatch.py`'s `running_dispatches`; separate because this task drives
`register_ena_study` + `submit_work_ticket_core` directly, not a
`ComputeBackendClient` workflow run).

The task (`_run_batch`) processes every item with bounded concurrency
(`_STUDY_CONCURRENCY`) -- staying well under miint's ENAClient outbound rate
limit and bounding concurrent DB writers. Each item (`_process_one_study`): resolve
(blocking calls under `asyncio.to_thread`) -> `register_ena_study` -> one
`download-ena-study` ticket per created pool, submitted in-process through
`submit_work_ticket_core` with the BATCH's submitting principal (so the ticket's
audience gate is enforced against a real principal). One accession's failure
marks only that item `failed`.

`reconcile_inflight_batches` (from `main.py` lifespan startup) re-drives every
item still `pending`/`resolving`/`registered` after a CP restart --
`register_ena_study` is idempotent and the submit loop reuses any already-created
download ticket, so re-driving is safe even if a prior resolve or ticket-submit
partially ran.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

import asyncpg
from fastapi import FastAPI
from qiita_common.models import WorkTicketState
from qiita_common.models.ena_import import (
    BatchImportItem,
    BatchImportStatus,
    BatchItemState,
    RunImportOutcome,
)

from ..auth.principal import HumanUser, PrincipalUnusableError, load_human_user
from ..repositories.ena_import_batch import (
    append_ena_import_batch_item_download_ticket,
    ena_import_batch_exists,
    ena_import_created_study,
    fetch_download_work_ticket_idx_for_sequenced_pool,
    fetch_ena_import_batch_items,
    fetch_inflight_ena_import_batch_items,
    fetch_work_ticket_states_for_idxs,
    insert_ena_import_batch,
    insert_ena_import_batch_item,
    update_ena_import_batch_item_registered,
    update_ena_import_batch_item_state,
)
from ..repositories.study import get_or_create_study_by_ena_accessions
from .accession import validate_study_accession
from .miint_resolver import MiintEnaResolver
from .registration import (
    EnaStudyRegistrationResult,
    RunRegistrationStatus,
    register_ena_study,
)
from .submit import (
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    build_download_ena_study_ticket,
)

_log = logging.getLogger(__name__)

# Bounded concurrency for resolve+register. Deliberately small: it keeps one batch
# from opening many concurrent ENA connections and DB writers at once, and stays
# comfortably under miint's ENAClient outbound rate limit. The exact request rate
# is miint's to set and is not measured here, so this is a conservative constant,
# not a value derived from that limit.
_STUDY_CONCURRENCY = 4

# Terminal-success work-ticket states: an item's download is `done` only when
# every one of its tickets is explicitly one of these. Anything else (running,
# unrecognized, or a missing row) must not read as success.
_TERMINAL_SUCCESS_STATES = frozenset(
    {WorkTicketState.COMPLETED.value, WorkTicketState.NO_DATA.value}
)


@dataclass(frozen=True)
class BatchImportItemHandle:
    """One item's identity, threaded from `create_ena_import_batch` into
    `_process_one_study`. Deliberately thinner than the `BatchImportItem` wire
    shape."""

    idx: int
    ena_study_accession: str


async def create_ena_import_batch(
    pool: asyncpg.Pool,
    *,
    accessions: list[str],
    principal: HumanUser,
) -> tuple[int, list[BatchImportItemHandle]]:
    """INSERT the batch row + one `pending` item per accession, synchronously.

    Validates every accession's shape up front (fail-loud, before any write) so
    a batch with one garbage accession never partially lands. Returns the batch
    idx and item handles in submitted order; the route fires the background task
    next.
    """
    # De-duplicate accessions, order-preserving: a repeated accession in one
    # request would otherwise fan out concurrent items registering the same study.
    validated = list(dict.fromkeys(validate_study_accession(a) for a in accessions))

    async with pool.acquire() as conn, conn.transaction():
        batch_idx = await insert_ena_import_batch(
            conn, submitted_by_principal_idx=principal.principal_idx
        )
        items: list[BatchImportItemHandle] = []
        for accession in validated:
            item_idx = await insert_ena_import_batch_item(
                conn, batch_idx=batch_idx, ena_study_accession=accession
            )
            items.append(BatchImportItemHandle(idx=item_idx, ena_study_accession=accession))
    return batch_idx, items


async def _set_item_state(
    pool: asyncpg.Pool, item_idx: int, state: BatchItemState, *, failure_reason: str | None = None
) -> None:
    async with pool.acquire() as conn:
        await update_ena_import_batch_item_state(
            conn, item_idx=item_idx, state=state.value, failure_reason=failure_reason
        )


def _run_outcomes(result: EnaStudyRegistrationResult) -> list[dict[str, Any]]:
    """Per-run outcomes for the item's `run_outcomes` JSONB column."""
    return [
        {
            "run_accession": o.run_accession,
            "status": o.status.value,
            "failure_reason": o.failure_reason,
        }
        for o in result.runs
    ]


async def _study_created_by_an_import(pool: asyncpg.Pool, study_idx: int) -> bool:
    """Whether some batch item created *study_idx*.

    An import may only add to a study an import created. A study Qiita created
    natively and later deposited carries a `bioproject_accession` too, so the
    accession lookup alone would match it and merge foreign ENA samples into
    curated data. Note a batch deleted after its import (CASCADE) takes this
    record with it, and a later re-import of that accession is then refused.
    """
    return await ena_import_created_study(pool, study_idx)


async def _set_item_registered(
    pool: asyncpg.Pool,
    item_idx: int,
    *,
    study_idx: int,
    study_created: bool,
    run_outcomes: list[dict[str, Any]],
) -> None:
    async with pool.acquire() as conn:
        await update_ena_import_batch_item_registered(
            conn,
            item_idx=item_idx,
            study_idx=study_idx,
            study_created=study_created,
            run_outcomes=run_outcomes,
        )


async def _append_item_download_ticket(pool: asyncpg.Pool, item_idx: int, ticket_idx: int) -> None:
    """Append one submitted download-ticket idx to the item's array the moment
    it is submitted, so a crash mid-loop leaves the tickets already sent
    recorded (nothing orphaned)."""
    async with pool.acquire() as conn:
        await append_ena_import_batch_item_download_ticket(
            conn, item_idx=item_idx, ticket_idx=ticket_idx
        )


async def _mark_item_downloading(pool: asyncpg.Pool, item_idx: int) -> None:
    """Flip a fully-submitted item to `downloading`. Its ticket idxs are already
    persisted by `_append_item_download_ticket` as each was submitted, so this
    only advances the state."""
    async with pool.acquire() as conn:
        await update_ena_import_batch_item_state(
            conn, item_idx=item_idx, state=BatchItemState.DOWNLOADING.value
        )


async def _existing_download_ticket_idx(pool: asyncpg.Pool, sequenced_pool_idx: int) -> int | None:
    """Lets a re-driven `registered` item reuse a ticket a prior run already
    submitted instead of re-submitting -- see the call site in
    `_process_one_study` for why (409-avoidance on re-drive)."""
    return await fetch_download_work_ticket_idx_for_sequenced_pool(
        pool,
        action_id=DOWNLOAD_ENA_STUDY_ACTION_ID,
        action_version=DOWNLOAD_ENA_STUDY_ACTION_VERSION,
        sequenced_pool_idx=sequenced_pool_idx,
    )


async def _process_one_study(
    app: FastAPI,
    pool: asyncpg.Pool,
    *,
    item: BatchImportItemHandle,
    principal: HumanUser,
) -> None:
    """Resolve + register ONE study, then submit one download-ena-study ticket
    per pool it created. Never raises -- every failure mode is caught and
    recorded as this item's `failed` state, so one bad accession can't affect
    any sibling or the batch. Blocking resolver calls run under
    `asyncio.to_thread` so they don't stall the shared event loop.
    """
    try:
        await _set_item_state(pool, item.idx, BatchItemState.RESOLVING)
        resolver = MiintEnaResolver()
        study_header = await asyncio.to_thread(
            resolver.resolve_study_header, item.ena_study_accession
        )
        runs = await asyncio.to_thread(resolver.resolve_runs, item.ena_study_accession)
        sample_attributes = await asyncio.to_thread(
            resolver.resolve_sample_attributes, item.ena_study_accession
        )

        # Resolve the study BEFORE registering anything, so an import into a
        # study we did not create fails with nothing written.
        async with pool.acquire() as conn:
            study_row, study_created = await get_or_create_study_by_ena_accessions(
                conn,
                bioproject_accession=study_header.study_accession,
                ena_study_accession=study_header.secondary_study_accession,
                owner_idx=principal.principal_idx,
                created_by_idx=principal.principal_idx,
                # study.title is NOT NULL but ENA's study_title is optional; a
                # title is cosmetic, not identity, so fall back to the accession.
                title=study_header.study_title or study_header.study_accession,
            )
        study_idx = study_row["idx"]
        if not study_created and not await _study_created_by_an_import(pool, study_idx):
            await _set_item_state(
                pool,
                item.idx,
                BatchItemState.FAILED,
                failure_reason=(
                    f"{item.ena_study_accession} maps to existing study {study_idx}, which was"
                    " not created by an ENA import; importing into it would merge ENA samples"
                    " into a natively-created study"
                ),
            )
            return

        result = await register_ena_study(
            pool,
            study_idx=study_idx,
            study_header=study_header,
            runs=runs,
            sample_attributes=sample_attributes,
            owner_idx=principal.principal_idx,
            caller_idx=principal.principal_idx,
        )
        await _set_item_registered(
            pool,
            item.idx,
            study_idx=result.study_idx,
            study_created=study_created,
            run_outcomes=_run_outcomes(result),
        )

        if not result.created_pools:
            # Registration succeeded (study + biosamples), but no run mapped to a
            # downloadable pool -- e.g. every run hit an unmappable platform. There
            # is nothing to download, so the item must reach a terminal state rather
            # than sit in `downloading` forever with an empty ticket list.
            await _set_item_state(
                pool,
                item.idx,
                BatchItemState.FAILED,
                failure_reason=(
                    "study registered but no run mapped to a downloadable pool"
                    " (no download tickets created)"
                ),
            )
            return

        if not any(o.status is not RunRegistrationStatus.FAILED for o in result.runs):
            # Pools exist (a platform mapped), but every run then failed inside
            # register_ena_study (protocol mapping, harmonization, or a DB error),
            # so the pools hold no sequenced_sample rows. Submitting downloads
            # against them would report success over an all-failed study. Terminal
            # `failed`; the per-run reasons are already persisted on `run_outcomes`.
            reasons = "; ".join(
                f"{o.run_accession}: {o.failure_reason}"
                for o in result.runs
                if o.status is RunRegistrationStatus.FAILED
            )
            await _set_item_state(
                pool,
                item.idx,
                BatchItemState.FAILED,
                failure_reason=f"study registered but every run failed to register ({reasons})",
            )
            return

        # Local import (narrow, deliberately unusual direction): reuse the exact
        # same audience/scope/disallow-without-delete gate a real
        # `POST /work-ticket` goes through, not a parallel copy.
        from ..routes.work_ticket import submit_work_ticket_core

        for created_pool in result.created_pools:
            # Re-drive safety: a prior run may have already submitted this pool's
            # ticket before crashing. Reuse it rather than re-submit (a
            # sequenced_pool re-submit 409s and would fail the whole item);
            # otherwise submit fresh. Each idx is persisted as it lands so a crash
            # mid-loop orphans nothing.
            ticket_idx = await _existing_download_ticket_idx(pool, created_pool.sequenced_pool_idx)
            if ticket_idx is None:
                body = build_download_ena_study_ticket(
                    sequenced_pool_idx=created_pool.sequenced_pool_idx,
                    sequencing_run_idx=created_pool.sequencing_run_idx,
                    ena_study_accession=study_header.study_accession,
                )
                response = await submit_work_ticket_core(app=app, principal=principal, body=body)
                ticket_idx = response.work_ticket_idx
            await _append_item_download_ticket(pool, item.idx, ticket_idx)

        await _mark_item_downloading(pool, item.idx)
    except Exception as exc:  # noqa: BLE001 -- per-study isolation: one
        # accession's failure must never abort siblings; recorded on this item,
        # visible via GET /ena-import-batch/{idx}, never swallowed silently.
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
            )

    await asyncio.gather(*[_bounded(item) for item in items])


def schedule_ena_import_batch(
    app: FastAPI,
    *,
    items: list[BatchImportItemHandle],
    principal: HumanUser,
) -> asyncio.Task:
    """Fire-and-forget the batch's resolve+register+submit background task on
    this module's own tracked set (see module docstring for why it's separate
    from `dispatch.py`'s)."""
    task = asyncio.create_task(
        _run_batch(
            app,
            app.state.pool,
            items=items,
            principal=principal,
        ),
        name="ena_import_batch",
    )
    app.state.running_ena_import_batches.add(task)
    task.add_done_callback(app.state.running_ena_import_batches.discard)
    return task


async def reconcile_inflight_batches(app: FastAPI) -> int:
    """Re-drive every batch item still `pending`/`resolving`/`registered` at startup.

    Mirrors `dispatch.reconcile_inflight_tickets`: a CP restart (or a
    drain-cancellation) leaves any non-terminal item with no live owner.
    `registered` is included because the `registered` -> `downloading` window
    still owes its download-ticket submissions -- a crash there would otherwise
    strand the item forever. Re-driving is safe: `register_ena_study` is
    idempotent, and the submit loop reuses any download ticket a prior run
    already created rather than re-submitting. Items are grouped by batch so each
    shares one task + semaphore, same as a fresh submission. Returns the count
    scheduled, for logging.
    """
    pool = app.state.pool
    rows = await fetch_inflight_ena_import_batch_items(pool)
    if not rows:
        return 0

    by_batch: dict[int, list[asyncpg.Record]] = {}
    for row in rows:
        by_batch.setdefault(row["batch_idx"], []).append(row)

    total = 0
    for batch_idx, batch_rows in by_batch.items():
        principal_idx = batch_rows[0]["submitted_by_principal_idx"]
        try:
            # Inside the guard: an unresolvable principal must fail only this
            # batch, not raise out of the lifespan reconcile and keep the whole
            # control plane down -- the same per-accession isolation this module
            # promises everywhere else.
            principal = await load_human_user(pool, principal_idx)
        except PrincipalUnusableError:
            _log.exception(
                "cannot re-drive ena_import_batch %d -- unresolvable submitting principal %d",
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
        )
        total += len(items)
    return total


async def fetch_batch_status(pool: asyncpg.Pool, *, batch_idx: int) -> BatchImportStatus | None:
    """Read a batch's current, rolled-up per-item status. Returns None if
    `batch_idx` names no row.

    A `downloading` item's `download_work_ticket_idxs`' `work_ticket.state` are
    rolled up ON DEMAND (never persisted back -- a pure read): any ticket failed
    -> `failed` (naming the ticket(s); the batch itself is never failed); any
    non-terminal -> stays `downloading`; all terminal-success -> `done`. Every
    other persisted state passes through unchanged.
    """
    if not await ena_import_batch_exists(pool, batch_idx):
        return None

    item_rows = await fetch_ena_import_batch_items(pool, batch_idx)

    all_ticket_idxs = sorted({idx for row in item_rows for idx in row["download_work_ticket_idxs"]})
    ticket_states: dict[int, str] = {}
    if all_ticket_idxs:
        ticket_rows = await fetch_work_ticket_states_for_idxs(pool, all_ticket_idxs)
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
            elif all(s in _TERMINAL_SUCCESS_STATES for s in states):
                state = BatchItemState.DONE
            else:
                # Any ticket not explicitly terminal-success -- still running, an
                # unrecognized state, or a missing work_ticket row (state None) --
                # must not read as success.
                state = BatchItemState.DOWNLOADING
        # run_outcomes is a JSONB column; asyncpg has no default jsonb codec, so
        # it comes back as a JSON string. Empty ('[]') until register_ena_study ran.
        runs = [RunImportOutcome(**o) for o in json.loads(row["run_outcomes"])]
        items.append(
            BatchImportItem(
                ena_study_accession=row["ena_study_accession"],
                state=state,
                study_idx=row["study_idx"],
                failure_reason=failure_reason,
                download_work_ticket_idxs=ticket_idxs,
                runs=runs,
            )
        )
    return BatchImportStatus(ena_import_batch_idx=batch_idx, items=items)
