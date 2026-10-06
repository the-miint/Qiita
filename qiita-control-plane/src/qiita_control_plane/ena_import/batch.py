"""Batch multi-study ENA import driver.

`create_ena_import_batch` INSERTs one `qiita.ena_import_batch` row plus one
`pending` `ena_import_batch_item` per accession and returns immediately (the
route responds 202). `schedule_ena_import_batch` fires ONE background task on
this module's own tracked set `app.state.running_ena_import_batches` (mirroring
`dispatch.py`'s `running_dispatches`; separate because this task drives
`register_ena_study` + `submit_work_ticket_core` directly, not a
`ComputeBackendClient` workflow run).

The task (`_run_batch`) processes every item under the process-wide bound
`_STUDY_CONCURRENCY`. Each item (`_process_one_study`): resolve
(blocking calls under `asyncio.to_thread`) -> `_reconcile_held_run_availability`
-> `register_ena_study` -> one `download-ena-study` ticket per pool holding
the study's runs, reused when one already covers the pool and otherwise
submitted in-process through `submit_work_ticket_core` with the BATCH's
submitting principal (so the ticket's audience gate is enforced against a
real principal). One accession's failure marks only that item `failed`.

`reconcile_inflight_batches` (from `main.py` lifespan startup) re-drives every
item still `pending`/`resolving`/`registered` after a CP restart --
`register_ena_study` is idempotent and the submit loop reuses any covering
download ticket, so re-driving is safe even if a prior resolve or ticket-submit
partially ran.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, replace
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException, status
from qiita_common.ena_accession import validate_study_accession
from qiita_common.models import WorkTicketState
from qiita_common.models.ena import EnaStatus
from qiita_common.models.ena_import import (
    BatchImportItem,
    BatchImportStatus,
    BatchItemState,
    EnaRunImportOutcome,
)

from ..auth.principal import HumanUser, PrincipalUnusableError, load_human_user
from ..repositories.ena_import_batch import (
    append_ena_import_batch_item_download_ticket,
    ena_import_batch_exists,
    ena_import_created_study,
    fetch_ena_import_batch_items,
    fetch_inflight_ena_import_batch_items,
    fetch_sequenced_samples_missed_by_completed_download,
    fetch_work_ticket_states_for_idxs,
    insert_ena_import_batch,
    insert_ena_import_batch_item,
    update_ena_import_batch_item_registered,
    update_ena_import_batch_item_run_outcomes,
    update_ena_import_batch_item_state,
    update_ena_import_batch_item_study_created,
)
from ..repositories.sequenced_sample import (
    fetch_held_ena_run_accessions_for_study,
    fetch_non_terminal_work_tickets_for_sequenced_sample,
    update_sequenced_sample_ena_status,
)
from ..repositories.study import (
    fetch_study_idx_by_either_ena_accession,
    get_or_create_study_by_ena_accessions,
)
from .availability import EnaAvailabilityClient
from .miint_resolver import MiintEnaResolver
from .registration import (
    EnaRunRegistrationOutcome,
    EnaRunRegistrationStatus,
    download_ticket_covers_pool,
    fetch_download_pool_states,
    fetch_pool_download_ticket,
    register_ena_study,
)
from .resolver import EnaAccessionNotFoundError
from .submit import (
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    build_download_ena_study_ticket,
)

_log = logging.getLogger(__name__)

# Process-wide bound on concurrent resolve+register, shared by every in-flight
# batch. Each permit holds at most one pool connection, so this must stay well
# below db.get_pool's max_size or the batch driver alone can starve every other
# caller of a connection. It also bounds the ENA request rate: miint's docs
# (https://the-miint.github.io/duckdb-miint/insdc_ena/) say only "rate-limited
# to ~3 requests/second", with no stated scope -- duckdb-miint#276 asks miint to
# document whether that cap is per ENAClient instance (today's per-query client
# construction would then let N concurrent studies reach ~3N req/s) or global.
# The dispatch each item's submit fires runs outside this permit, under
# dispatch's own process-wide cap — see dispatch._DISPATCH_CONCURRENCY.
_STUDY_CONCURRENCY = 4


def build_ena_import_study_semaphore() -> asyncio.Semaphore:
    """The process-wide permit pool `schedule_ena_import_batch` binds every batch to."""
    return asyncio.Semaphore(_STUDY_CONCURRENCY)


# Terminal-success work-ticket states: an item's download is `done` only when
# every one of its tickets is explicitly one of these. Anything else (running,
# unrecognized, or a missing row) must not read as success.
_TERMINAL_SUCCESS_STATES = frozenset(
    {WorkTicketState.COMPLETED.value, WorkTicketState.NO_DATA.value}
)
_TERMINAL_UNSUCCESSFUL_STATES = frozenset(
    {WorkTicketState.FAILED.value, WorkTicketState.CANCELLED.value}
)

# A run counts toward "this study registered something downloadable" only in
# these two states -- EXCLUDED (non-public) and FAILED do not.
_SUCCESSFUL_RUN_REGISTRATION_STATUSES = frozenset(
    {
        EnaRunRegistrationStatus.REGISTERED,
        EnaRunRegistrationStatus.SKIPPED_ALREADY_PRESENT,
        EnaRunRegistrationStatus.HELD_NOT_DOWNLOADED,
    }
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


def _ena_run_outcomes(outcomes: list[EnaRunRegistrationOutcome]) -> list[dict[str, Any]]:
    """Per-run outcomes for the item's `ena_run_outcomes` JSONB column."""
    return [
        {
            "run_accession": o.run_accession,
            "status": o.status.value,
            "failure_reason": o.failure_reason,
        }
        for o in outcomes
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
    ena_run_outcomes: list[dict[str, Any]],
) -> None:
    async with pool.acquire() as conn:
        await update_ena_import_batch_item_registered(
            conn,
            item_idx=item_idx,
            study_idx=study_idx,
            ena_run_outcomes=ena_run_outcomes,
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


async def _log_non_terminal_tickets_for_flagged_run(
    pool: asyncpg.Pool,
    *,
    ena_run_accession: str,
    prep_sample_idx: int,
    sequenced_pool_idx: int | None,
) -> None:
    """Log (never cancel) every non-terminal work_ticket touching a run this
    call just flagged, so an operator can act through
    `POST /work-ticket/cancel` -- the flag alone does not stop work already
    in flight against the run (see `ena_import.batch` module docstring)."""
    tickets = await fetch_non_terminal_work_tickets_for_sequenced_sample(
        pool, prep_sample_idx=prep_sample_idx, sequenced_pool_idx=sequenced_pool_idx
    )
    for t in tickets:
        _log.warning(
            "ena run %s flagged unavailable with work_ticket %d (action=%s, state=%s) still"
            " non-terminal; not auto-cancelled -- cancel via POST /work-ticket/cancel"
            " {sequenced_pool_idx=%s}",
            ena_run_accession,
            t["work_ticket_idx"],
            t["action_id"],
            t["state"],
            sequenced_pool_idx,
        )


async def _reconcile_held_run_availability(
    pool: asyncpg.Pool, *, study_idx: int, portal_run_accessions: frozenset[str]
) -> list[EnaRunRegistrationOutcome]:
    """Re-check every run this study already holds that the Portal's fresh
    response no longer names as public (`portal_run_accessions`), and clear the
    flag on any held run it does.

    The Portal `/search` endpoint never reports a non-public run at all (see
    `qiita_common.models.ena`), so a held run missing from a fresh response is
    a CANDIDATE -- it may be suppressed, withdrawn, or simply not retrievable
    -- and the Browser API (`EnaAvailabilityClient`) is the only source that
    says which. Every candidate is looked up before any write: if the client
    raises (an HTTP error, an unrecognized status, or an unparseable
    body), this function raises too and writes nothing, so the
    caller's failed item leaves every flag exactly as it found them. Once
    every lookup succeeds, every set/clear lands in one transaction.

    Returns one `EnaRunRegistrationOutcome` (`FLAGGED_UNAVAILABLE`) per run
    newly flagged or re-confirmed unavailable -- not per run merely checked or
    cleared, so a report distinguishes "still fine" from "flagged".
    """
    held = await fetch_held_ena_run_accessions_for_study(pool, study_idx=study_idx)
    candidates = [r for r in held if r["ena_run_accession"] not in portal_run_accessions]
    reappeared = [
        r
        for r in held
        if r["ena_run_accession"] in portal_run_accessions and r["ena_status"] is not None
    ]
    if not candidates and not reappeared:
        return []

    new_status_by_accession: dict[str, str | None] = {}
    if candidates:
        async with EnaAvailabilityClient() as client:
            new_status_by_accession = await client.check_runs(
                [row["ena_run_accession"] for row in candidates]
            )

    outcomes: list[EnaRunRegistrationOutcome] = []
    async with pool.acquire() as conn, conn.transaction():
        for row in candidates:
            new_status = new_status_by_accession[row["ena_run_accession"]]
            await update_sequenced_sample_ena_status(
                conn, ena_run_accession=row["ena_run_accession"], ena_status=new_status
            )
            if new_status is not None:
                outcomes.append(
                    EnaRunRegistrationOutcome(
                        run_accession=row["ena_run_accession"],
                        status=EnaRunRegistrationStatus.FLAGGED_UNAVAILABLE,
                        failure_reason=new_status,
                    )
                )
        for row in reappeared:
            await update_sequenced_sample_ena_status(
                conn, ena_run_accession=row["ena_run_accession"], ena_status=None
            )

    # Logged after commit, outside the write transaction: a cancel decision is
    # the operator's, not blocking on it here.
    for row in candidates:
        if new_status_by_accession[row["ena_run_accession"]] is not None:
            await _log_non_terminal_tickets_for_flagged_run(
                pool,
                ena_run_accession=row["ena_run_accession"],
                prep_sample_idx=row["prep_sample_idx"],
                sequenced_pool_idx=row["sequenced_pool_idx"],
            )
    return outcomes


async def _reconcile_and_record(
    pool: asyncpg.Pool,
    item: BatchImportItemHandle,
    *,
    study_idx: int,
    portal_run_accessions: frozenset[str],
) -> list[EnaRunRegistrationOutcome]:
    """`_reconcile_held_run_availability`, then record its outcomes on the item
    at once: the flags are already committed, so the item must show them even
    if a later step fails it."""
    flagged = await _reconcile_held_run_availability(
        pool, study_idx=study_idx, portal_run_accessions=portal_run_accessions
    )
    async with pool.acquire() as conn:
        await update_ena_import_batch_item_run_outcomes(
            conn,
            item_idx=item.idx,
            study_idx=study_idx,
            ena_run_outcomes=_ena_run_outcomes(flagged),
        )
    return flagged


async def _fail_after_held_run_check(
    pool: asyncpg.Pool,
    item: BatchImportItemHandle,
    failure_reason: str,
    *,
    portal_run_accessions: frozenset[str],
) -> None:
    """Fail an item with nothing new to register: the study is absent, not
    public, or has no public runs. The Portal drops non-public runs, so "every
    run suppressed" arrives as one of these; an import-created study's held
    runs are re-checked first, and the failure names any flagged."""
    study_idx = await fetch_study_idx_by_either_ena_accession(pool, item.ena_study_accession)
    if study_idx is not None and await _study_created_by_an_import(pool, study_idx):
        flagged = await _reconcile_and_record(
            pool, item, study_idx=study_idx, portal_run_accessions=portal_run_accessions
        )
        if flagged:
            reasons = "; ".join(f"{o.run_accession}: {o.failure_reason}" for o in flagged)
            failure_reason = f"{failure_reason} (held runs re-checked: {reasons})"
    await _set_item_state(pool, item.idx, BatchItemState.FAILED, failure_reason=failure_reason)


async def _mark_runs_held_not_downloaded(
    pool: asyncpg.Pool, outcomes: list[EnaRunRegistrationOutcome]
) -> list[EnaRunRegistrationOutcome]:
    """Report a held run its pool's completed download never fetched as
    `HELD_NOT_DOWNLOADED` rather than `SKIPPED_ALREADY_PRESENT`."""
    held = [
        o.sequenced_sample_idx
        for o in outcomes
        if o.status is EnaRunRegistrationStatus.SKIPPED_ALREADY_PRESENT
        and o.sequenced_sample_idx is not None
    ]
    if not held:
        return outcomes
    missed = await fetch_sequenced_samples_missed_by_completed_download(
        pool,
        sequenced_sample_idxs=held,
        action_id=DOWNLOAD_ENA_STUDY_ACTION_ID,
        action_version=DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    )
    return [
        replace(
            o,
            status=EnaRunRegistrationStatus.HELD_NOT_DOWNLOADED,
            failure_reason="held with no reads; its pool's download completed without it",
        )
        if o.sequenced_sample_idx in missed
        else o
        for o in outcomes
    ]


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
        try:
            study_header = await asyncio.to_thread(
                resolver.resolve_study_header, item.ena_study_accession
            )
        except EnaAccessionNotFoundError as exc:
            await _fail_after_held_run_check(
                pool, item, str(exc), portal_run_accessions=frozenset()
            )
            return
        try:
            ena_runs = await asyncio.to_thread(resolver.resolve_ena_runs, item.ena_study_accession)
            no_runs_reason = None
        except EnaAccessionNotFoundError as exc:
            ena_runs, no_runs_reason = [], str(exc)
        public_run_accessions = frozenset(
            run.run_accession for run in ena_runs if run.status is EnaStatus.PUBLIC
        )

        if study_header.status is not EnaStatus.PUBLIC:
            await _fail_after_held_run_check(
                pool,
                item,
                f"study {item.ena_study_accession} is {study_header.status.value}",
                portal_run_accessions=public_run_accessions,
            )
            return
        if not public_run_accessions:
            reasons = "; ".join(f"{run.run_accession} is {run.status.value}" for run in ena_runs)
            await _fail_after_held_run_check(
                pool,
                item,
                no_runs_reason or f"no public runs: {reasons}",
                portal_run_accessions=public_run_accessions,
            )
            return
        sample_attributes = await asyncio.to_thread(
            resolver.resolve_sample_attributes, item.ena_study_accession
        )

        # Resolve the study BEFORE registering anything, so an import into a
        # study we did not create fails with nothing written.
        async with pool.acquire() as conn, conn.transaction():
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
            if study_created:
                await update_ena_import_batch_item_study_created(
                    conn, item_idx=item.idx, study_idx=study_row["idx"]
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

        # A re-import: runs this study already holds that the Portal's fresh
        # response above no longer names are candidates for the ENA
        # availability flag; a held run the Portal DOES still return has any
        # existing flag cleared. Runs before register_ena_study so a Browser
        # API failure here fails the item before anything new is written --
        # held runs are rows Qiita already has, so this does not depend on
        # register_ena_study having run. Disjoint from register_ena_study's
        # outcomes below (which only cover runs the Portal returned), so the
        # two lists concatenate with no overlap.
        flagged = await _reconcile_and_record(
            pool, item, study_idx=study_idx, portal_run_accessions=public_run_accessions
        )
        result = await register_ena_study(
            pool,
            study_idx=study_idx,
            study_header=study_header,
            ena_runs=ena_runs,
            sample_attributes=sample_attributes,
            owner_idx=principal.principal_idx,
            caller_idx=principal.principal_idx,
        )
        run_outcomes = await _mark_runs_held_not_downloaded(pool, result.ena_runs)
        await _set_item_registered(
            pool,
            item.idx,
            study_idx=result.study_idx,
            ena_run_outcomes=_ena_run_outcomes(flagged) + _ena_run_outcomes(run_outcomes),
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

        if not any(o.status in _SUCCESSFUL_RUN_REGISTRATION_STATUSES for o in result.ena_runs):
            # Pools exist (a platform mapped), but every run then failed or was
            # excluded (non-public) inside register_ena_study, so the pools hold
            # no sequenced_sample rows. Submitting downloads against them would
            # report success over an all-failed study. Terminal `failed`; the
            # per-run reasons are already persisted on `ena_run_outcomes`.
            reasons = "; ".join(
                f"{o.run_accession}: {o.failure_reason}"
                for o in result.ena_runs
                if o.status not in _SUCCESSFUL_RUN_REGISTRATION_STATUSES
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

        # Each idx is persisted as it lands so a crash mid-loop orphans nothing.
        any_ticket = False
        for sequencing_run_idx in dict.fromkeys(p.sequencing_run_idx for p in result.created_pools):
            for pool_state in await fetch_download_pool_states(pool, sequencing_run_idx):
                if not pool_state["has_sequenced_sample"]:
                    continue
                if download_ticket_covers_pool(pool_state["work_ticket_state"]):
                    # Safe to reuse either way: a pool already covered when we
                    # resolved excluded our runs from it entirely, and one
                    # covered after our resolve read under the same lock and
                    # therefore sees every run we registered.
                    ticket_idx = pool_state["work_ticket_idx"]
                else:
                    body = build_download_ena_study_ticket(
                        sequenced_pool_idx=pool_state["sequenced_pool_idx"],
                        sequencing_run_idx=sequencing_run_idx,
                        ena_study_accession=study_header.study_accession,
                    )
                    try:
                        response = await submit_work_ticket_core(
                            app=app, principal=principal, body=body
                        )
                        ticket_idx = response.work_ticket_idx
                    except HTTPException as exc:
                        if exc.status_code != status.HTTP_409_CONFLICT:
                            raise
                        # A concurrent batch submitted this pool's ticket after our read.
                        ticket_idx = await _covering_download_ticket_idx(
                            pool, pool_state["sequenced_pool_idx"]
                        )
                        if ticket_idx is None:
                            raise
                await _append_item_download_ticket(pool, item.idx, ticket_idx)
                any_ticket = True

        if not any_ticket:
            await _set_item_state(
                pool,
                item.idx,
                BatchItemState.FAILED,
                failure_reason="study registered but no pool holds an active sequenced_sample",
            )
            return
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


async def _covering_download_ticket_idx(pool: asyncpg.Pool, sequenced_pool_idx: int) -> int | None:
    ticket = await fetch_pool_download_ticket(pool, sequenced_pool_idx=sequenced_pool_idx)
    if ticket is not None and download_ticket_covers_pool(ticket["work_ticket_state"]):
        return ticket["work_ticket_idx"]
    return None


async def _run_batch(
    app: FastAPI,
    pool: asyncpg.Pool,
    *,
    items: list[BatchImportItemHandle],
    principal: HumanUser,
    semaphore: asyncio.Semaphore,
) -> None:
    """Process every item against the process-wide `semaphore`. Never raises --
    each item's own try/except in `_process_one_study` absorbs its failure."""

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
    from `dispatch.py`'s).

    Reads `app.state.ena_import_study_semaphore` synchronously, before the task
    is created, so a missing semaphore raises here rather than inside the task.
    """
    semaphore = app.state.ena_import_study_semaphore
    task = asyncio.create_task(
        _run_batch(
            app,
            app.state.pool,
            items=items,
            principal=principal,
            semaphore=semaphore,
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
    shares one task, same as a fresh submission, all bound by the same
    process-wide semaphore. Returns the count scheduled, for logging.
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
        except PrincipalUnusableError as exc:
            _log.warning("cannot re-drive ena_import_batch %d: %s", batch_idx, exc)
            for r in batch_rows:
                await _set_item_state(
                    pool,
                    r["idx"],
                    BatchItemState.FAILED,
                    failure_reason=f"not re-driven, submitting principal unusable: {exc.detail}",
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
    rolled up ON DEMAND (never persisted back -- a pure read): any ticket failed or
    cancelled -> `failed` (naming the ticket(s); the batch itself is never failed);
    any non-terminal -> stays `downloading`; all terminal-success -> `done`. Every
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
            unsuccessful = [
                f"{idx} ({s})"
                for idx, s in zip(ticket_idxs, states, strict=True)
                if s in _TERMINAL_UNSUCCESSFUL_STATES
            ]
            if unsuccessful:
                state = BatchItemState.FAILED
                failure_reason = f"download work_ticket(s) did not complete: {unsuccessful}"
            elif all(s in _TERMINAL_SUCCESS_STATES for s in states):
                state = BatchItemState.DONE
            else:
                # Any ticket not explicitly terminal-success -- still running, an
                # unrecognized state, or a missing work_ticket row (state None) --
                # must not read as success.
                state = BatchItemState.DOWNLOADING
        # ena_run_outcomes is a JSONB column; asyncpg has no default jsonb codec, so
        # it comes back as a JSON string. Empty ('[]') until register_ena_study ran.
        ena_runs = [EnaRunImportOutcome(**o) for o in json.loads(row["ena_run_outcomes"])]
        items.append(
            BatchImportItem(
                ena_study_accession=row["ena_study_accession"],
                state=state,
                study_idx=row["study_idx"],
                failure_reason=failure_reason,
                download_work_ticket_idxs=ticket_idxs,
                ena_runs=ena_runs,
            )
        )
    return BatchImportStatus(ena_import_batch_idx=batch_idx, items=items)
