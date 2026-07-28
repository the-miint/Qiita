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
audience gate is enforced against a real principal) and the batch's own persisted
`download_method`. One accession's failure marks only that item `failed`.

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
from qiita_common.auth_constants import MSG_PRINCIPAL_DISABLED_OR_RETIRED
from qiita_common.models import WorkTicketState
from qiita_common.models.ena_import import (
    BatchImportItem,
    BatchImportStatus,
    BatchItemState,
    RunImportOutcome,
)

from ..auth.principal import HumanUser, _human_user_from_row
from ..auth.scopes import role_ceiling
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


async def _load_principal(pool: asyncpg.Pool, principal_idx: int) -> HumanUser:
    """Reconstruct the submitting `HumanUser` from a principal_idx.

    The background task and startup reconcile have no request-bound `Principal`
    to reuse, so this locally re-implements `auth.principal._build_human_user`'s
    query (`role_ceiling` supplies the same scope set an OIDC session would).
    Same guard: a disabled/retired principal is refused, not just a missing one
    -- an admin disabled/retired AFTER submission must not be re-driven on their
    behalf across a CP restart.
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
    if row["disabled"] or row["retired"]:
        raise RuntimeError(
            f"principal {principal_idx}: {MSG_PRINCIPAL_DISABLED_OR_RETIRED};"
            " cannot submit/re-drive ena_import_batch work on its behalf"
        )
    # Same construction the OIDC/token loaders use; the guard above is this
    # loader's addition (a disabled/retired principal must not be re-driven).
    return _human_user_from_row(row, scopes=role_ceiling(row["system_role"]))


async def create_ena_import_batch(
    pool: asyncpg.Pool,
    *,
    accessions: list[str],
    principal: HumanUser,
    download_method: str,
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
        batch_idx = await conn.fetchval(
            "INSERT INTO qiita.ena_import_batch"
            " (submitted_by_principal_idx, download_method)"
            " VALUES ($1, $2) RETURNING idx",
            principal.principal_idx,
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


def _run_outcomes(result: EnaStudyRegistrationResult) -> list[dict[str, Any]]:
    """Per-run outcomes for the item's `run_outcomes` JSONB column. Carries each
    run's status, failure reason, and harmonization gap (`missing_required`) so
    `GET /ena-import-batch/{idx}` can surface them."""
    return [
        {
            "run_accession": o.run_accession,
            "status": o.status.value,
            "failure_reason": o.failure_reason,
            "missing_required": (list(o.harmonization.missing_required) if o.harmonization else []),
        }
        for o in result.runs
    ]


def _preserve_missing_required(
    fresh: list[dict[str, Any]], stored: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Carry a previously recorded harmonization gap forward onto *fresh*.

    A re-drive re-registers an already-imported study, so `register_ena_study`
    finds every biosample already present and harmonizes none of them
    (`biosample_created` is False). The fresh outcomes then carry an empty
    `missing_required` not because the gap closed but because nothing recomputed
    it -- writing that over the stored list would drop the very gap this surface
    exists to report, and the runbook promises these are never silently dropped.

    Per run, so a re-drive that *does* create some biosamples keeps their newly
    computed values and preserves only the runs it skipped. A run absent from
    the stored outcomes (new to this pass) simply keeps its fresh value.
    """
    prior = {o["run_accession"]: o.get("missing_required") or [] for o in stored}
    return [
        o if o["missing_required"] else {**o, "missing_required": prior.get(o["run_accession"], [])}
        for o in fresh
    ]


async def _set_item_registered(
    pool: asyncpg.Pool, item_idx: int, *, study_idx: int, run_outcomes: list[dict[str, Any]]
) -> None:
    """Record a study's registration on its batch item.

    Reads the stored outcomes under the same transaction before writing so a
    re-drive cannot erase a harmonization gap it did not recompute (see
    `_preserve_missing_required`). asyncpg has no default jsonb codec, so the
    column is read as a string and written back as one + `::jsonb`.
    """
    async with pool.acquire() as conn, conn.transaction():
        stored = await conn.fetchval(
            "SELECT run_outcomes FROM qiita.ena_import_batch_item WHERE idx = $1 FOR UPDATE",
            item_idx,
        )
        merged = _preserve_missing_required(run_outcomes, json.loads(stored))
        await conn.execute(
            "UPDATE qiita.ena_import_batch_item"
            " SET state = $2, study_idx = $3, run_outcomes = $4::jsonb, failure_reason = NULL"
            " WHERE idx = $1",
            item_idx,
            BatchItemState.REGISTERED.value,
            study_idx,
            json.dumps(merged),
        )


async def _append_item_download_ticket(pool: asyncpg.Pool, item_idx: int, ticket_idx: int) -> None:
    """Append one submitted download-ticket idx to the item's array the moment
    it is submitted, so a crash mid-loop leaves the tickets already sent
    recorded (nothing orphaned). Idempotent: a re-drive that reuses an existing
    ticket does not duplicate the idx."""
    await pool.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET download_work_ticket_idxs = CASE"
        "   WHEN $2 = ANY(download_work_ticket_idxs) THEN download_work_ticket_idxs"
        "   ELSE array_append(download_work_ticket_idxs, $2) END"
        " WHERE idx = $1",
        item_idx,
        ticket_idx,
    )


async def _mark_item_downloading(pool: asyncpg.Pool, item_idx: int) -> None:
    """Flip a fully-submitted item to `downloading`. Its ticket idxs are already
    persisted by `_append_item_download_ticket` as each was submitted, so this
    only advances the state."""
    await pool.execute(
        "UPDATE qiita.ena_import_batch_item SET state = $2 WHERE idx = $1",
        item_idx,
        BatchItemState.DOWNLOADING.value,
    )


async def _existing_download_ticket_idx(pool: asyncpg.Pool, sequenced_pool_idx: int) -> int | None:
    """The idx of an existing download-ena-study work_ticket for this pool, if
    any (any state). Lets a re-driven `registered` item reuse a ticket a prior
    run already submitted instead of re-submitting -- a sequenced_pool re-submit
    would 409 (disallow-without-delete / the COMPLETED-pool gate), which would
    wrongly fail the whole item."""
    return await pool.fetchval(
        "SELECT work_ticket_idx FROM qiita.work_ticket"
        " WHERE action_id = $1 AND action_version = $2 AND sequenced_pool_idx = $3"
        " ORDER BY work_ticket_idx LIMIT 1",
        DOWNLOAD_ENA_STUDY_ACTION_ID,
        DOWNLOAD_ENA_STUDY_ACTION_VERSION,
        sequenced_pool_idx,
    )


async def _process_one_study(
    app: FastAPI,
    pool: asyncpg.Pool,
    *,
    item: BatchImportItemHandle,
    principal: HumanUser,
    download_method: str,
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

        result = await register_ena_study(
            pool,
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
                    download_method=download_method,
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
    download_method: str,
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
                download_method=download_method,
            )

    await asyncio.gather(*[_bounded(item) for item in items])


def schedule_ena_import_batch(
    app: FastAPI,
    *,
    items: list[BatchImportItemHandle],
    principal: HumanUser,
    download_method: str,
) -> asyncio.Task:
    """Fire-and-forget the batch's resolve+register+submit background task on
    this module's own tracked set (see module docstring for why it's separate
    from `dispatch.py`'s). `download_method` is the batch's own persisted value,
    threaded verbatim so a future transport value never silently drifts to the
    wrong one for tickets this batch submits.
    """
    task = asyncio.create_task(
        _run_batch(
            app,
            app.state.pool,
            items=items,
            principal=principal,
            download_method=download_method,
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
    rows = await pool.fetch(
        "SELECT bi.idx, bi.ena_study_accession, bi.batch_idx,"
        "       b.submitted_by_principal_idx, b.download_method"
        " FROM qiita.ena_import_batch_item bi"
        " JOIN qiita.ena_import_batch b ON b.idx = bi.batch_idx"
        " WHERE bi.state = ANY($1::text[])"
        " ORDER BY bi.batch_idx, bi.idx",
        [
            BatchItemState.PENDING.value,
            BatchItemState.RESOLVING.value,
            BatchItemState.REGISTERED.value,
        ],
    )
    if not rows:
        return 0

    by_batch: dict[int, list[asyncpg.Record]] = {}
    for row in rows:
        by_batch.setdefault(row["batch_idx"], []).append(row)

    total = 0
    for batch_idx, batch_rows in by_batch.items():
        principal_idx = batch_rows[0]["submitted_by_principal_idx"]
        download_method = batch_rows[0]["download_method"]
        try:
            # Inside the guard: an unresolvable principal must fail only this
            # batch, not raise out of the lifespan reconcile and keep the whole
            # control plane down -- the same per-accession isolation this module
            # promises everywhere else.
            principal = await _load_principal(pool, principal_idx)
        except RuntimeError:
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
            download_method=download_method,
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
    exists = await pool.fetchval("SELECT 1 FROM qiita.ena_import_batch WHERE idx = $1", batch_idx)
    if exists is None:
        return None

    item_rows = await pool.fetch(
        "SELECT idx, ena_study_accession, state, failure_reason, study_idx,"
        "       download_work_ticket_idxs, run_outcomes"
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
