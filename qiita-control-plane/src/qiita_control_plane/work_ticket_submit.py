"""Submit a work ticket (gate, INSERT, dispatch), independent of the HTTP layer.

`submit_work_ticket_core` and its submission gates live here rather than in
`routes/work_ticket.py` so in-process callers — the follow-on completion hook
and the ENA import batch driver — reach the submit path without importing a
routes module. The `POST /work-ticket` route is a thin caller that imports from
here.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path, PurePosixPath
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException, status
from qiita_common.actions import (
    FASTQ_PATH_CONTEXT_KEYS,
    PATH_SUFFIX,
    UPLOAD_IDX_SUFFIX,
    Audience,
)
from qiita_common.auth_constants import SystemRole
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    ReferenceStatus,
    ScopeTargetKind,
    WorkTicketCreateRequest,
    WorkTicketFollowOn,
    WorkTicketResponse,
    WorkTicketState,
)
from qiita_common.work_ticket_constants import FORCE_RESUBMIT_EXPLANATION

from .actions.context_validator import validate_context
from .actions.reference import (
    IllegalStatusTransition,
    ReferenceNotFound,
    transition_reference_status,
)
from .auth.guards import require_caller_has_admin_on_all_studies
from .auth.principal import HumanUser, Principal, ServiceAccount
from .config import Settings
from .dispatch import schedule_dispatch
from .ingest_path import IngestPathError, named_host_paths, resolve_ingest_path
from .repositories.prep_sample import fetch_active_study_idxs_for_prep_sample

_log = logging.getLogger(__name__)


# Names of the unique partial indexes that enforce
# disallow-without-delete atomically. Defined in migration
# 20260508000000_work_ticket_disallow_without_delete_indexes.sql; renaming
# either there must light up the catch site below at type-check time.
_DISALLOW_WITHOUT_DELETE_INDEXES = frozenset(
    {
        "work_ticket_one_in_flight_per_reference",
        "work_ticket_one_in_flight_per_study_prep",
        "work_ticket_one_in_flight_per_prep_sample",
        "work_ticket_one_in_flight_per_sequenced_pool",
        "work_ticket_one_in_flight_per_block",
        "work_ticket_one_in_flight_per_shard",
    }
)


async def _fetch_action_for_submission(
    pool: asyncpg.Pool, action_id: str, action_version: str
) -> dict[str, Any] | None:
    """Read just the columns the submission gate needs. Returns None if
    the action does not exist; returns a dict including `enabled` if it
    does (so the caller can distinguish "not found" from "deprecated"
    and respond with the right HTTP status — 404 vs 410).

    `audience` is parsed via the `Audience` Pydantic model — JSONB drift
    (renamed/removed field) becomes a loud ValidationError at submission
    rather than a silent default in the audience check below."""
    row = await pool.fetchrow(
        "SELECT target_kind, target_processing_kinds,"
        "       scopes, audience, context_schema, enabled, mem_ceiling_gb"
        " FROM qiita.action"
        " WHERE action_id = $1 AND version = $2",
        action_id,
        action_version,
    )
    if row is None:
        return None
    return {
        "target_kind": row["target_kind"],
        # asyncpg yields qiita.processing_kind[] as a list of strings;
        # the route compares them against prep_sample.processing_kind
        # (also a string) so no enum coercion is needed.
        "target_processing_kinds": list(row["target_processing_kinds"]),
        "scopes": list(row["scopes"]),
        # `audience` and `context_schema` are JSONB; asyncpg returns each
        # as a string by default.
        "audience": Audience.model_validate_json(row["audience"]),
        "context_schema": json.loads(row["context_schema"]),
        "enabled": row["enabled"],
        # Mem axis of the action ceiling — the upper bound a resource_override
        # is validated against at submission (the runner re-clamps at dispatch).
        "mem_ceiling_gb": row["mem_ceiling_gb"],
    }


def _check_audience(principal: Principal, audience: Audience) -> None:
    """403 unless the caller is in the action's audience.

    `audience.service=True` permits service-account principals.
    `audience.human_roles` is the set of system_role values that may
    submit. An action with `service=False` and an empty `human_roles`
    is unsubmittable by anyone — by design (e.g. a workflow that only
    the runner itself invokes via an internal call path)."""
    if isinstance(principal, ServiceAccount):
        if not audience.service:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="this action is not in your audience (service accounts not permitted)",
            )
        return
    if isinstance(principal, HumanUser):
        if principal.system_role not in audience.human_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="this action is not in your audience (system_role not permitted)",
            )
        return
    # Anonymous and any future kinds.
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="authentication required",
    )


def _check_scopes(principal: Principal, required_scopes: list[str]) -> None:
    """403 unless the caller's token carries every required scope.

    AND-composition: every scope in `action.scopes` must be present.
    OIDC-resolved principals carry their role's full ceiling; PAT-resolved
    principals carry the token's own scope set. `Principal.scopes` reads
    from whichever source applies."""
    missing = [s for s in required_scopes if not principal.has_scope(s)]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "missing required action scopes",
                "missing_scopes": missing,
            },
        )


def _scope_target_columns(
    scope_target: dict[str, Any],
) -> tuple[int | None, int | None, int | None, int | None, int | None, int | None]:
    """Map a ScopeTarget union member to the
    (study_idx, prep_idx, reference_idx, prep_sample_idx, sequenced_pool_idx,
    block_idx) tuple the work_ticket table expects.

    The SEQUENCED_POOL arm carries both sequenced_pool_idx and
    sequencing_run_idx; the run idx is consumed by the orchestrator
    framework (SCOPE_SCALARS_BY_KIND) but is not a work_ticket column —
    it's derivable from the pool row's FK back to qiita.sequencing_run.
    The BLOCK arm carries only block_idx (the filtering identity rides on
    work_ticket.mask_idx, not the scope target).
    """
    kind = scope_target["kind"]
    if kind == ScopeTargetKind.REFERENCE.value:
        return (None, None, scope_target["reference_idx"], None, None, None)
    if kind == ScopeTargetKind.STUDY_PREP.value:
        return (scope_target["study_idx"], scope_target["prep_idx"], None, None, None, None)
    if kind == ScopeTargetKind.PREP_SAMPLE.value:
        return (None, None, None, scope_target["prep_sample_idx"], None, None)
    if kind == ScopeTargetKind.SEQUENCED_POOL.value:
        return (None, None, None, None, scope_target["sequenced_pool_idx"], None)
    if kind == ScopeTargetKind.BLOCK.value:
        return (None, None, None, None, None, scope_target["block_idx"])
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"unknown scope_target.kind={kind!r}",
    )


async def _check_disallow_without_delete(
    pool: asyncpg.Pool,
    action_id: str,
    action_version: str,
    scope_target: dict[str, Any],
    *,
    force: bool = False,
) -> None:
    """409 if any existing ticket for this `(scope_target, action_id,
    action_version)` triple is in a non-terminal state. For most scopes
    COMPLETED tickets are tolerated here; resubmission is gated elsewhere
    until the result is DELETEd.

    The `sequenced_pool` scope is the exception: a COMPLETED ticket means the
    pool's reads are already registered in the lake, and DuckLake has no
    uniqueness — a naive re-run double-registers them. There is no
    result-deletion gate for a pool (the result is lake rows, not a single
    minted row), so this check itself refuses a re-submit over a COMPLETED pool
    ticket unless `force=True` (gated to wet_lab_admin+ at the route). What
    forcing costs, and the recovery that avoids it, are stated once in
    `FORCE_RESUBMIT_EXPLANATION`, which the 409 below carries.

    Best-effort fast path. The atomic gate is the unique partial indexes
    `work_ticket_one_in_flight_per_{reference,study_prep,prep_sample,sequenced_pool}`;
    we still SELECT first so the common (non-racing) case returns a 409
    carrying the blocking ticket idx, which is more useful to clients
    than the bare unique-violation that fires when two submissions race
    past this check. The COMPLETED-pool gate is SELECT-only (no unique-index
    backstop): a forced re-run legitimately creates a second ticket that itself
    reaches COMPLETED, so a `WHERE state='completed'` unique index would be
    wrong. Two concurrent non-forced submits racing past it is the same
    accepted TOCTOU class as the rest of this check."""
    (
        study_idx,
        prep_idx,
        reference_idx,
        prep_sample_idx,
        sequenced_pool_idx,
        block_idx,
    ) = _scope_target_columns(scope_target)
    if scope_target["kind"] == ScopeTargetKind.REFERENCE.value:
        existing = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE action_id = $1 AND action_version = $2"
            "   AND reference_idx = $3"
            "   AND state = ANY($4::qiita.work_ticket_state[])"
            " LIMIT 1",
            action_id,
            action_version,
            reference_idx,
            list(NON_TERMINAL_WORK_TICKET_STATES),
        )
    elif scope_target["kind"] == ScopeTargetKind.PREP_SAMPLE.value:
        existing = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE action_id = $1 AND action_version = $2"
            "   AND prep_sample_idx = $3"
            "   AND state = ANY($4::qiita.work_ticket_state[])"
            " LIMIT 1",
            action_id,
            action_version,
            prep_sample_idx,
            list(NON_TERMINAL_WORK_TICKET_STATES),
        )
    elif scope_target["kind"] == ScopeTargetKind.SEQUENCED_POOL.value:
        existing = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE action_id = $1 AND action_version = $2"
            "   AND sequenced_pool_idx = $3"
            "   AND state = ANY($4::qiita.work_ticket_state[])"
            " LIMIT 1",
            action_id,
            action_version,
            sequenced_pool_idx,
            list(NON_TERMINAL_WORK_TICKET_STATES),
        )
    elif scope_target["kind"] == ScopeTargetKind.BLOCK.value:
        # Block scope: only the in-flight (non-terminal) gate applies here.
        # A COMPLETED block does not block resubmission at the ticket level —
        # the per-(prep_sample, mask_idx) COMPLETED gate on qiita.mask_sample
        # is what a re-run must clear (DELETE-gated), not the block ticket.
        existing = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE action_id = $1 AND action_version = $2"
            "   AND block_idx = $3"
            "   AND state = ANY($4::qiita.work_ticket_state[])"
            " LIMIT 1",
            action_id,
            action_version,
            block_idx,
            list(NON_TERMINAL_WORK_TICKET_STATES),
        )
    else:
        existing = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE action_id = $1 AND action_version = $2"
            "   AND study_idx = $3 AND prep_idx = $4"
            "   AND state = ANY($5::qiita.work_ticket_state[])"
            " LIMIT 1",
            action_id,
            action_version,
            study_idx,
            prep_idx,
            list(NON_TERMINAL_WORK_TICKET_STATES),
        )
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "another ticket for this (scope_target, action) is in flight",
                "blocking_work_ticket_idx": existing,
            },
        )

    # Sequenced-pool only: refuse a re-submit over an already-COMPLETED pool
    # ticket unless forced. The pool's reads are registered in the lake; a
    # re-run re-registers them (DuckLake has no uniqueness → duplicate rows).
    if scope_target["kind"] == ScopeTargetKind.SEQUENCED_POOL.value and not force:
        completed = await pool.fetchval(
            "SELECT work_ticket_idx FROM qiita.work_ticket"
            " WHERE action_id = $1 AND action_version = $2"
            "   AND sequenced_pool_idx = $3"
            "   AND state = $4::qiita.work_ticket_state"
            " LIMIT 1",
            action_id,
            action_version,
            sequenced_pool_idx,
            WorkTicketState.COMPLETED.value,
        )
        if completed is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "reason": (
                        "a ticket for this pool and action has already COMPLETED, so "
                        "the pool's reads are already stored. Pass force=true "
                        "(`qiita submit-bcl-convert --force`) to submit anyway. "
                        f"{FORCE_RESUBMIT_EXPLANATION}"
                    ),
                    "blocking_work_ticket_idx": completed,
                },
            )


async def _check_prep_sample_study_access(
    pool: asyncpg.Pool, *, prep_sample_idx: int, caller: Principal
) -> None:
    """Require Tier.ADMIN (or owner / wet_lab_admin+) on every non-retired
    study link of this prep_sample. Bypass-role callers skip the DB read.
    An orphaned prep_sample (all links retired) passes — downstream
    lookups fail elsewhere.

    No service-account special-case: a service account holds no
    study_access rows, so `require_caller_has_admin_on_all_studies`
    rejects it on the first linked study with a 403. For every
    prep_sample-scoped action today the action's audience already
    excludes service accounts, so this gate never sees one — but the
    natural rejection means an action that later opens itself to
    service accounts does not silently bypass the per-study check.

    Policy is "caller had access at submit time"; this read runs on the
    pool, not the work_ticket INSERT transaction, so a study_access row
    racing with the gate decides arbitrarily. Future per-resource gates
    on other scope kinds are expected to follow the same shape.
    """
    if caller.has_role_at_least(SystemRole.WET_LAB_ADMIN):
        return
    study_idxs = await fetch_active_study_idxs_for_prep_sample(pool, prep_sample_idx)
    if not study_idxs:
        return
    await require_caller_has_admin_on_all_studies(
        pool,
        caller=caller,
        study_idxs=study_idxs,
    )


def _basename_carries_prefix(basename: str, prefix: str) -> bool:
    """True iff `basename` is `prefix` immediately followed by a `_` or
    `.` separator — segment-anchored, not a bare substring match.

    A plain `str.startswith` would admit `ITEM-12_R1.fastq` for a
    sequenced_pool_item_id of `ITEM-1`; requiring the separator pins the
    match to the documented `<pool_item_id>_R1.fastq` (paired-end) and
    `<pool_item_id>.fastq` (single-end) filename convention exactly. A
    basename equal to `prefix` with nothing after it is rejected — a
    fastq file always carries an extension.
    """
    if not basename.startswith(prefix):
        return False
    return basename[len(prefix) : len(prefix) + 1] in ("_", ".")


def _check_ingest_paths(
    principal: Principal,
    *,
    context_schema: dict[str, Any],
    action_context: dict[str, Any],
    roots: tuple[Path, ...],
) -> None:
    """Gate every host path the submitted `action_context` names.

    Two rules, in order:

    1. Naming a host path at all is wet_lab_admin-or-higher. A path is
       re-opened later on a compute node under the job account, so a submitter
       who can name one reaches every file that account can read — a wider
       reach than the action's own audience implies. A `user` submits the file
       as an upload instead and names the `{prefix}_upload_idx` handle, which
       the runner resolves to a path the submitter never chose.
    2. The path resolves under one of `PATH_INGEST_ROOTS` and, where the
       control plane can tell, exists (`ingest_path.resolve_ingest_path`).

    Which keys are host paths comes from the action's own `context_schema` and
    from the `*_path` / `*_dir` naming convention (`named_host_paths`), so an
    action that adds one is covered without a change here.

    Every offending path is reported in one 422 body, so a submission naming
    two bad paths takes one round-trip to fix rather than two.
    """
    named = named_host_paths(context_schema, action_context)
    if not named:
        return

    if not principal.has_role_at_least(SystemRole.WET_LAB_ADMIN):
        upload_keys = sorted(
            key.removesuffix(PATH_SUFFIX) + UPLOAD_IDX_SUFFIX
            for key in named
            if key.endswith(PATH_SUFFIX)
        )
        detail: dict[str, Any] = {
            "reason": (
                "naming a host path in action_context requires wet_lab_admin or system_admin"
            ),
            "context_keys": sorted(named),
        }
        if upload_keys:
            detail["upload_instead"] = upload_keys
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail)

    errors = []
    for key, raw in sorted(named.items()):
        try:
            resolve_ingest_path(raw, roots=roots)
        except IngestPathError as exc:
            errors.append({"context_key": key, "path": exc.path, "reason": exc.reason})
    if errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": "action_context names a host path the compute cluster cannot use",
                "errors": errors,
                "ingest_roots": [str(root) for root in roots],
            },
        )


async def _fastq_upload_filenames(
    pool: asyncpg.Pool, *, action_context: dict[str, Any], principal_idx: int
) -> dict[str, str]:
    """`{context_key: source_filename}` for every fastq key submitted as an
    upload handle rather than a path.

    An upload-fed submission carries `fastq_upload_idx` instead of
    `fastq_path`, and the staging path the runner resolves it to
    (`uploads/{idx}/upload.parquet`) is not a name the submitter chose — so
    the filename-prefix rule has to read what the client said it sent
    (`upload.source_filename`). One batched fetch, keyed back to the context
    key so the 422 names the field the submitter wrote.

    Scoped to uploads the caller created. The runner refuses another
    principal's upload anyway (`runner._upload`), but the 422 this feeds quotes
    the filename back, and a filename here carries a `sequenced_pool_item_id` —
    so an unscoped lookup would answer "what is upload N called" for any N to
    anyone holding a prep_sample of their own.

    An upload with no `source_filename` (it predates the column, or the client
    did not send one) is omitted: the rule is vacuous without a name, the same
    way it is vacuous without a `sequenced_pool_item_id`.
    """
    by_idx: dict[int, str] = {}
    for key in FASTQ_PATH_CONTEXT_KEYS:
        # Report against the key the submitter actually wrote
        # (`fastq_upload_idx`), not its resolved `fastq_path` twin.
        upload_key = key.removesuffix(PATH_SUFFIX) + UPLOAD_IDX_SUFFIX
        handle = action_context.get(upload_key)
        if isinstance(handle, int) and not isinstance(handle, bool) and handle > 0:
            by_idx[handle] = upload_key
    if not by_idx:
        return {}
    rows = await pool.fetch(
        "SELECT upload_idx, source_filename FROM qiita.upload"
        " WHERE upload_idx = ANY($1::bigint[]) AND created_by_idx = $2",
        list(by_idx),
        principal_idx,
    )
    return {
        by_idx[row["upload_idx"]]: row["source_filename"]
        for row in rows
        if row["source_filename"] is not None
    }


async def _check_fastq_filename_prefix(
    pool: asyncpg.Pool,
    *,
    prep_sample_idx: int,
    action_context: dict[str, Any],
    principal_idx: int,
) -> None:
    """422 when a fastq path in `action_context` has a basename that is
    not the prep_sample's `sequenced_pool_item_id` followed by a `_` or
    `.` separator (see `_basename_carries_prefix`).

    The rule: the `--pool-item-id` chosen at `sequenced-sample create`
    time is the filename prefix of every fastq the work-ticket
    processes, so the R1/R2 pair and the sequenced_sample row stay
    mechanically tied. `action_context` and the sequenced_sample row are
    minted in two separate calls and nothing else couples them, so the
    check lives here. Keyed on the context keys (FASTQ_PATH_CONTEXT_KEYS),
    not the action_id, so the route stays generic over actions.

    Covers both routes a fastq arrives by. A path-fed submission is checked on
    the basename of the path; an upload-fed one (`fastq_upload_idx`, the route
    a regular user takes) on the client-claimed `upload.source_filename`, since
    the staging path the runner resolves the handle to is not a name the
    submitter chose. See `_fastq_upload_filenames`.

    Skipped when the resolved `sequenced_pool_item_id` is NULL. Two
    shapes reach that branch:

    - a sequenced_sample row exists but is pool-less — sequenced_pool_idx
      and sequenced_pool_item_id are both NULL — which is legitimate; the
      rule then has nothing to anchor against.
    - no sequenced_sample subtype row exists for this
      processing_kind='sequenced' prep_sample — anomalous, since the
      sequenced-sample create composer writes supertype and subtype
      atomically. Policing that integrity gap is not this gate's job
      (the processing_kind check above and the create composer own it);
      the gate just declines to invent a constraint it cannot evaluate.

    Either way the rule constrains a relationship between two values and
    is vacuous when one does not exist, so the submission proceeds.

    Like `_check_prep_sample_study_access`, the read runs on the pool,
    not the work_ticket INSERT transaction: a sequenced_sample row
    created concurrently with the gate decides arbitrarily.
    `sequenced_pool_item_id` is not PATCH-editable, so a value already
    set cannot change under the gate.
    """
    fastq_paths = {
        key: action_context[key]
        for key in FASTQ_PATH_CONTEXT_KEYS
        # Defense-in-depth: for fastq-to-parquet the context_schema pins
        # each fastq key to a string, so validate_context already 422'd a
        # non-string upstream. The guard still covers actions with a
        # permissive schema — a non-string value is not a fastq path, so
        # it is skipped here rather than rejected.
        if isinstance(action_context.get(key), str)
    }
    fastq_uploads = await _fastq_upload_filenames(
        pool, action_context=action_context, principal_idx=principal_idx
    )
    if not fastq_paths and not fastq_uploads:
        return
    pool_item_id = await pool.fetchval(
        "SELECT sequenced_pool_item_id FROM qiita.sequenced_sample WHERE prep_sample_idx = $1",
        prep_sample_idx,
    )
    if pool_item_id is None:
        return
    mismatched = [
        {
            "context_key": key,
            "fastq_path": path,
            "basename": PurePosixPath(path).name,
        }
        for key, path in sorted(fastq_paths.items())
        if not _basename_carries_prefix(PurePosixPath(path).name, pool_item_id)
    ]
    mismatched += [
        {"context_key": key, "source_filename": name, "basename": name}
        for key, name in sorted(fastq_uploads.items())
        if not _basename_carries_prefix(name, pool_item_id)
    ]
    if mismatched:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": (
                    "fastq filename must be the prep_sample's sequenced_pool_item_id"
                    " followed by '_' or '.'"
                ),
                "sequenced_pool_item_id": pool_item_id,
                "mismatched": mismatched,
            },
        )


def require_compute_backend_client(app: FastAPI) -> None:
    """Guard that 503s if the orchestrator dispatch path is not configured.
    Prevents creating tickets that can never run."""
    if app.state.compute_backend_client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="compute orchestrator not configured (COMPUTE_ORCHESTRATOR_URL unset)",
        )


async def _gate_submission(
    *,
    pool: asyncpg.Pool,
    settings: Settings,
    principal: Principal,
    body: WorkTicketCreateRequest,
) -> dict[str, Any]:
    """Every submission gate that reads only the request and stored state, not
    the scope's in-flight tickets: action, audience, scopes, privileged knobs,
    target kind, prep_sample access, context schema, host paths. Returns the
    scope target as a JSON dict.

    The in-flight / completed dedupe is left to the caller because a follow-on
    is gated here at its parent's submission, when that dedupe would be asking
    the wrong question.
    """

    action = await _fetch_action_for_submission(pool, body.action_id, body.action_version)
    if action is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"action ({body.action_id!r}, {body.action_version!r}) not found",
        )
    if not action["enabled"]:
        # 410 Gone — the action_id/version pair exists in the catalog
        # but has been deprecated (sync replaced it with a newer version,
        # or an operator disabled it). Distinct from 404 so clients that
        # auto-retry on 404 don't keep trying a permanently-gone version.
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=f"action ({body.action_id!r}, {body.action_version!r}) is deprecated",
        )

    _check_audience(principal, action["audience"])
    _check_scopes(principal, action["scopes"])

    # force bypasses the COMPLETED-pool resubmit gate (a re-run re-registers the
    # pool's reads — a data-integrity foot-gun), so it is privileged regardless
    # of the action's own audience, like resource_override below.
    if body.force and not principal.has_role_at_least(SystemRole.WET_LAB_ADMIN):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"reason": "force requires wet_lab_admin or system_admin"},
        )

    # resource_override is a privileged, ceiling-bounded per-run resource bump.
    # Gate it to wet_lab_admin / system_admin regardless of the action's own
    # (possibly broader) audience — a regular user who can submit a workflow
    # still cannot inflate its SLURM footprint — and reject a mem_gb above the
    # action's ceiling here so the caller gets a clean 422 instead of a ticket
    # that fails at dispatch.
    if body.resource_override is not None:
        if not principal.has_role_at_least(SystemRole.WET_LAB_ADMIN):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"reason": "resource_override requires wet_lab_admin or system_admin"},
            )
        override_mem_gb = body.resource_override.mem_gb
        if override_mem_gb is not None and override_mem_gb > action["mem_ceiling_gb"]:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail={
                    "reason": "resource_override.mem_gb exceeds the action's mem ceiling",
                    "resource_override_mem_gb": override_mem_gb,
                    "action_mem_ceiling_gb": action["mem_ceiling_gb"],
                },
            )

    scope_target = body.scope_target.model_dump(mode="json")
    if scope_target["kind"] != action["target_kind"]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": "scope_target.kind does not match action.target_kind",
                "scope_target_kind": scope_target["kind"],
                "action_target_kind": action["target_kind"],
            },
        )

    # Kind-specific gate for prep_sample-scoped actions: when the action
    # declares a nonempty target_processing_kinds list, the prep_sample's
    # actual processing_kind must be in it. Empty list = "any kind"
    # (cross-kind admin actions). For other scope kinds the list must
    # be empty per the DB CHECK; nothing to do here.
    if (
        scope_target["kind"] == ScopeTargetKind.PREP_SAMPLE.value
        and action["target_processing_kinds"]
    ):
        actual_kind = await pool.fetchval(
            "SELECT processing_kind FROM qiita.prep_sample WHERE idx = $1",
            scope_target["prep_sample_idx"],
        )
        if actual_kind is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(f"prep_sample {scope_target['prep_sample_idx']!r} not found"),
            )
        if actual_kind not in action["target_processing_kinds"]:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail={
                    "reason": (
                        "prep_sample.processing_kind does not match action.target_processing_kinds"
                    ),
                    "prep_sample_processing_kind": actual_kind,
                    "action_target_processing_kinds": action["target_processing_kinds"],
                },
            )

    if scope_target["kind"] == ScopeTargetKind.PREP_SAMPLE.value:
        await _check_prep_sample_study_access(
            pool,
            prep_sample_idx=scope_target["prep_sample_idx"],
            caller=principal,
        )

    context_errors = validate_context(action["context_schema"], body.action_context)
    if context_errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": "action_context does not match action.context_schema",
                "errors": context_errors,
            },
        )

    # Every host path in the (now schema-valid) action_context must be one the
    # caller is allowed to name and the compute cluster can reach.
    _check_ingest_paths(
        principal,
        context_schema=action["context_schema"],
        action_context=body.action_context,
        roots=settings.path_ingest_roots,
    )

    # A fastq path in the (now schema-valid) action_context must carry a
    # basename prefixed by the prep_sample's sequenced_pool_item_id.
    if scope_target["kind"] == ScopeTargetKind.PREP_SAMPLE.value:
        await _check_fastq_filename_prefix(
            pool,
            prep_sample_idx=scope_target["prep_sample_idx"],
            action_context=body.action_context,
            principal_idx=principal.principal_idx,
        )

    return scope_target


async def _gate_follow_on(
    *,
    pool: asyncpg.Pool,
    settings: Settings,
    principal: Principal,
    parent: WorkTicketCreateRequest,
) -> None:
    """Run `_gate_submission` over every link of `parent.on_success` on the
    parent's scope target, so a follow-on these gates refuse is refused with its
    parent rather than at a later stage's completion. State that changes in
    between, and the in-flight dedupe these gates leave out, can still refuse a
    link when its own stage completes.

    Human callers only: the completion hook re-loads the originator with
    `load_human_user`, and there is no equivalent for a service account."""
    assert parent.on_success is not None
    if not isinstance(principal, HumanUser):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"reason": "on_success is only accepted from a human caller"},
        )
    node: WorkTicketFollowOn | None = parent.on_success
    while node is not None:
        link = node.as_request(parent.scope_target)
        try:
            await _gate_submission(pool=pool, settings=settings, principal=principal, body=link)
        except HTTPException as exc:
            raise HTTPException(
                status_code=exc.status_code,
                detail={"reason": "on_success was refused", "on_success_detail": exc.detail},
            ) from exc
        node = node.on_success


class FollowOnAlreadyRecordedError(Exception):
    """The parent already records a follow-on outcome; the ticket was not created."""


async def submit_work_ticket_core(
    *,
    app: FastAPI,
    principal: Principal,
    body: WorkTicketCreateRequest,
    follow_on_of: int | None = None,
) -> WorkTicketResponse:
    """Gate, INSERT, and dispatch one work ticket for `principal`.

    Takes `app` rather than a `Request` so in-process callers get the same gates
    as `POST /work-ticket`, including the action's audience check.

    `follow_on_of` names the parent whose follow-on this is. The parent's
    `follow_on_work_ticket_idx` is written in the INSERT's transaction, and only
    while the parent records no outcome; otherwise the INSERT rolls back and this
    raises `FollowOnAlreadyRecordedError`. That is what makes a follow-on created
    at most once and never left unrecorded.
    """
    require_compute_backend_client(app)
    pool: asyncpg.Pool = app.state.pool
    settings: Settings = app.state.settings

    scope_target = await _gate_submission(
        pool=pool, settings=settings, principal=principal, body=body
    )
    if body.on_success is not None:
        await _gate_follow_on(pool=pool, settings=settings, principal=principal, parent=body)

    await _check_disallow_without_delete(
        pool, body.action_id, body.action_version, scope_target, force=body.force
    )

    (
        study_idx,
        prep_idx,
        reference_idx,
        prep_sample_idx,
        sequenced_pool_idx,
        block_idx,
    ) = _scope_target_columns(scope_target)
    # INSERT and the failed→pending scope reset are ONE transaction so a fresh
    # submit never half-applies — mirrors the `/run` redrive path's single-
    # transaction guarantee. A reference this new ticket binds to may sit at
    # `failed` from a prior ticket (a `reference load --reference-idx N` retry);
    # the reference FSM's only legal exit from `failed` is `→ pending`, so
    # without the reset the run's first status PATCH (`failed → hashing`) is
    # illegal and the ticket dies immediately. Binding them atomically means an
    # unexpected reset failure rolls the ticket back too — it never leaves a
    # committed PENDING ticket undispatched, which `_check_disallow_without_delete`
    # would then block from resubmission. The reset only touches a `failed`
    # reference (any other state is a clean zero-row UPDATE the helper turns into
    # a no-op or a logged warning — it never poisons this transaction).
    # `schedule_dispatch` stays OUTSIDE: it is fire-and-forget, post-commit.
    try:
        async with pool.acquire() as conn, conn.transaction():
            work_ticket_idx = await conn.fetchval(
                "INSERT INTO qiita.work_ticket ("
                "  action_id, action_version, originator_principal_idx,"
                "  scope_target_kind, study_idx, prep_idx, reference_idx,"
                "  prep_sample_idx, sequenced_pool_idx, block_idx, action_context,"
                "  resource_override, on_success"
                ") VALUES ($1, $2, $3, $4::qiita.scope_target_kind,"
                "          $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb, $13::jsonb)"
                " RETURNING work_ticket_idx",
                body.action_id,
                body.action_version,
                principal.principal_idx,
                scope_target["kind"],
                study_idx,
                prep_idx,
                reference_idx,
                prep_sample_idx,
                sequenced_pool_idx,
                block_idx,
                json.dumps(body.action_context),
                json.dumps(body.resource_override.model_dump())
                if body.resource_override is not None
                else None,
                body.on_success.model_dump_json() if body.on_success is not None else None,
            )
            await _reset_failed_reference_scope_for_dispatch(
                conn,
                scope_target_kind=scope_target["kind"],
                reference_idx=reference_idx,
                context="submit",
            )
            if follow_on_of is not None:
                recorded = await conn.fetchval(
                    "UPDATE qiita.work_ticket SET follow_on_work_ticket_idx = $2"
                    " WHERE work_ticket_idx = $1"
                    "   AND follow_on_work_ticket_idx IS NULL AND follow_on_error IS NULL"
                    " RETURNING work_ticket_idx",
                    follow_on_of,
                    work_ticket_idx,
                )
                if recorded is None:
                    raise FollowOnAlreadyRecordedError(follow_on_of)
    except asyncpg.exceptions.UniqueViolationError as exc:
        # Unique partial index fired — a concurrent submission won the
        # race past `_check_disallow_without_delete`. Map to the same
        # 409 shape the SELECT-side check returns; the blocking idx is
        # not cheap to recover here (would require a second query) and
        # the client only needs to know to retry against the in-flight
        # ticket they will discover via /work-ticket listing.
        if exc.constraint_name in _DISALLOW_WITHOUT_DELETE_INDEXES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "reason": "another ticket for this (scope_target, action) is in flight",
                },
            ) from exc
        raise

    # Fire-and-forget dispatch in the background. The route returns 202
    # immediately; the workflow runs in-process via asyncio.
    schedule_dispatch(app, work_ticket_idx)

    _log.info(
        "submitted work_ticket %d for action %s/%s by principal %d",
        work_ticket_idx,
        body.action_id,
        body.action_version,
        principal.principal_idx,
    )
    return WorkTicketResponse(work_ticket_idx=work_ticket_idx, state=WorkTicketState.PENDING)


async def _reset_failed_reference_scope_for_dispatch(
    executor: asyncpg.Pool | asyncpg.Connection,
    *,
    scope_target_kind: str,
    reference_idx: int | None,
    context: str,
) -> None:
    """Reset a `failed` `reference` scope_target back to `pending` when
    (re)dispatching a workflow against it, so the run's first status PATCH
    can proceed.

    A failed reference workflow leaves its reference at `failed` (the
    failure_status PATCH put it there), and the reference FSM's only legal
    exit from `failed` is `→ pending`. Without this reset the (re)dispatched
    workflow's first status PATCH (`pending → hashing`, attempted as
    `failed → hashing`) is illegal and the run dies immediately. BOTH
    dispatch paths need it: a `/run` redrive of the SAME failed ticket, and a
    fresh `POST /work-ticket` bound to an existing reference that a prior
    ticket left at `failed` (the `reference load --reference-idx N` retry).

    Only references at `failed` are touched: `transition_reference_status`
    is the single source of truth for the matrix, and `→ pending` is legal
    *only* from `failed`. A reference in any other state raises
    IllegalStatusTransition, which we swallow rather than abort the
    operator's (re)dispatch — but the two non-`failed` cases differ sharply:
      * already `pending` (e.g. the workflow died before any status PATCH):
        benign — the run's first PATCH (`pending → hashing`) is legal
        and the run proceeds normally.
      * an in-progress state (`hashing`/`minting`/`loading`/`indexing`) or
        `active`: the FSM can't rewind it, so the run will fail cleanly
        later in the runner. We log it at WARNING with the actual state so
        that not-yet-supported multi-step case is visible, not silent.
    Non-reference scopes carry no status and are skipped.

    `context` is a short label woven into the log lines ("redrive" / "submit").
    """
    if scope_target_kind != ScopeTargetKind.REFERENCE.value:
        return
    if reference_idx is None:
        return
    try:
        await transition_reference_status(executor, reference_idx, ReferenceStatus.PENDING)
    except IllegalStatusTransition as exc:
        _log.warning(
            "%s of work_ticket on reference %d: reference is %r, not 'failed' —"
            " not resetting; the run proceeds and re-walks from there"
            " (a non-'pending' state means the FSM can't rewind and the run"
            " will fail cleanly downstream)",
            context,
            reference_idx,
            exc.current,
        )
    except ReferenceNotFound:
        # A work_ticket carries a reference_idx its FK (ON DELETE RESTRICT)
        # guarantees exists — hitting this means the row is gone out from
        # under a live ticket, i.e. DB corruption, not a recoverable edge.
        _log.error(
            "%s references missing reference %d; skipping scope-target reset"
            " (referential integrity violation — investigate)",
            context,
            reference_idx,
        )
