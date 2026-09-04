"""Runner upload-handle resolution."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg
from qiita_common.api_paths import (
    compute_upload_staging_path,
)
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import (
    UploadStatus,
    WorkTicketFailureStage,
)

from ._base import _PATH_SUFFIX, _UPLOAD_IDX_SUFFIX

# =============================================================================
# Upload-handle resolution
# =============================================================================
#
# Source-of-truth for the upload domain — what a `qiita.upload` row means
# and the consume contract — lives in db/migrations/20260521000000_upload.sql.
# These helpers tie that domain to the workflow runner: pre-step resolution
# (find the file the step will read) and post-success consumption (mark the
# slot terminal).


def _submission_bad_input(reason: str) -> BackendFailure:
    """A BAD_INPUT failure attributed to workflow SUBMISSION (not any one step).

    The shared shape every pre-step resolution pass raises — `_resolve_upload_handles`
    and `_resolve_host_filter_indexes` — so the outer `except BackendFailure`
    block in `run_workflow` translates each into a FAILED work_ticket
    identically (step_name=None ⇒ attributed to the workflow's submission)."""
    return BackendFailure(
        kind=FailureKind.BAD_INPUT,
        stage=WorkTicketFailureStage.SUBMISSION,
        step_name=None,
        reason=reason,
    )


# Postgres SQLSTATE 40001 signature. The data plane's DuckLake catalog writes run
# under serializable isolation, so a concurrent-attach race surfaces this exact
# text — but stringified through the pyarrow FlightError the DP returns, so there
# is no typed asyncpg exception to isinstance against at this layer (unlike
# `_is_transient_db_error`, which matches the CP's OWN asyncpg errors). Match the
# message. See FailureKind.DATA_PLANE_TRANSIENT.
_DP_SERIALIZATION_SIGNATURE = "could not serialize access due to concurrent update"

# gRPC UNAVAILABLE signatures. A data-plane fetch that can't reach the DP surfaces
# as a pyarrow FlightUnavailableError — the DP momentarily saturated by a fan-out,
# restarting during a deploy, or briefly unreachable. This is transient by
# definition (gRPC's own retriable status): a redrive self-heals once the DP is
# back, so it must NOT be filed as a permanent bad-input. Matched by message for
# the same reason as the serialization signature (no typed exception at this
# layer); any one substring is sufficient.
#
# pyarrow renders a Flight error TWO ways, and the difference decides which
# substrings can fire. A client-side failure (nothing listening) gives
# "Flight returned unavailable error, with message: failed to connect to all
# addresses…" — the first three match that. A status the server actually returned
# gives "<message>. Detail: Unavailable. gRPC client debug context: …", with the
# class name absent, so only the fourth matches. That second form is the
# saturated / restarting DP this list names first, and without "detail: unavailable"
# it classified permanent. Both forms are pinned in test_dp_fetch_classifier.py.
_DP_UNAVAILABLE_SIGNATURES = (
    "flightunavailableerror",
    "flight returned unavailable",
    "failed to connect to all addresses",
    "detail: unavailable",
)

# Expired-token signatures, BOTH required. A Flight token carries a TTL
# (`auth/tickets.py` DEFAULT_TTL_SECONDS) that the data plane checks when the call
# arrives, so a token that ages out in transit comes back as the data plane's
# `AuthError::Expired` text under a gRPC unauthenticated status. The next attempt
# mints a fresh token, so a redrive self-heals it exactly as the two signatures
# above do.
#
# The expiry text alone is too loose — "ticket" names both a Flight ticket and a
# work_ticket here, and "work ticket expired" contains it. The unauthenticated
# marker is what makes it a Flight auth failure, and it survives both of pyarrow's
# renderings: the message-prefix form spells "unauthenticated error", the
# server-returned form appends "Detail: Unauthenticated". The class name alone is too loose
# in the other direction: every AuthError variant maps to unauthenticated, and
# `invalid signature` / `malformed payload` never self-heal. A test parses the data
# plane's `AuthError` Display impl so a reword there fails rather than silently
# reverting every expiry to permanent.
_DP_TICKET_EXPIRED_SIGNATURES = ("unauthenticated", "ticket expired")


def _is_dp_serialization_conflict(exc: BaseException) -> bool:
    """True if a data-plane Flight failure is a transient, retriable serialization
    conflict (SQLSTATE 40001) rather than a permanent bad-input / DP-down error."""
    return _DP_SERIALIZATION_SIGNATURE in str(exc)


def _is_dp_unavailable(exc: BaseException) -> bool:
    """True if a data-plane Flight failure is a transient gRPC UNAVAILABLE (the DP
    could not be reached), rather than a permanent bad-input error."""
    text = str(exc).lower()
    return any(sig in text for sig in _DP_UNAVAILABLE_SIGNATURES)


def _is_dp_ticket_expired(exc: BaseException) -> bool:
    """True if a data-plane Flight failure is an expired signing token, rather than
    a permanent auth failure (bad signature, malformed payload) or a bad input."""
    text = str(exc).lower()
    return all(sig in text for sig in _DP_TICKET_EXPIRED_SIGNATURES)


def _is_retriable_dp_error(exc: BaseException) -> bool:
    """True if a data-plane Flight fetch failed for a transient, retriable reason —
    a serialization conflict (concurrent DuckLake attach), the DP being briefly
    unreachable, or a signing token that expired before the call reached the DP —
    any of which a redrive self-heals. Everything else is a permanent bad-input an
    operator must resolve."""
    return (
        _is_dp_serialization_conflict(exc) or _is_dp_unavailable(exc) or _is_dp_ticket_expired(exc)
    )


def _submission_dp_fetch_failure(reason: str, exc: BaseException) -> BackendFailure:
    """A SUBMISSION failure for a data-plane Flight fetch (adapters, reads).

    Wrapping buys a clean FAILED transition: a raw pyarrow FlightError reaching
    `run_workflow`'s bare `except Exception` records stage=STEP_RUN with
    step_name=None, which violates the work_ticket_failure_step_name_consistent
    CHECK — the failure transition itself throws and strands the ticket in
    PROCESSING. Same SUBMISSION/step_name=None shape whatever the cause, so it
    stays a drop-in for the existing `except` translation.

    Classifies by cause: a transient serialization conflict (concurrent DuckLake
    attach), a transient DP-unreachable (gRPC UNAVAILABLE), or an expired signing
    token is DATA_PLANE_TRANSIENT; anything else keeps the BAD_INPUT/permanent
    shape of `_submission_bad_input` (a genuine bad reference or missing data an
    operator must resolve).

    **Retriable here does not mean retried.** Every caller is a pre-loop resolver,
    which runs before the step loop and so never reaches `_run_entry_with_retry`:
    nothing re-runs it, and `retry_count` stays 0. What the label changes is where
    the ticket lands — `notify.sweeper`'s owed set is
    `failure_type IS DISTINCT FROM 'retriable'`, so a retriable failure is held for
    an operator redrive instead of reported as a settled outcome in the
    originator's digest."""
    kind = (
        FailureKind.DATA_PLANE_TRANSIENT if _is_retriable_dp_error(exc) else FailureKind.BAD_INPUT
    )
    return BackendFailure(
        kind=kind,
        stage=WorkTicketFailureStage.SUBMISSION,
        step_name=None,
        reason=reason,
    )


async def _resolve_upload_handles(
    pool: asyncpg.Pool,
    *,
    action_context: dict[str, Any],
    originator_principal_idx: int,
    upload_staging_root: Path,
) -> tuple[dict[str, Path], list[int]]:
    """For every `{prefix}_upload_idx` key in `action_context`, resolve
    to a `{prefix}_path` Path binding pointing at the canonical staging
    file (`{staging_root}/uploads/{idx}/upload.parquet`).

    Validates four invariants per upload, in this order:
      1. The upload row exists.
      2. status='ready' — the DoPut completed and /done was called.
      3. created_by_idx == originator_principal_idx — uploaders can only
         feed their own work tickets. Matches the same per-row ownership
         gate `POST /upload/{idx}/done` and `GET /upload/{idx}` enforce
         (see routes/upload.py); the runner double-checks here because
         the originator on a work_ticket isn't necessarily the same as
         the principal that created each referenced upload.
      4. The on-disk file exists at the canonical staging path. Catches
         a CP↔DP layout drift (or a deleted scratch) before the workflow
         hands the path to a step that would 404 on it.

    Any violation raises a typed `BackendFailure(BAD_INPUT)` at
    stage=SUBMISSION — the work_ticket goes to FAILED with the failure
    attributed to the workflow's submission, not any one step.
    Non-`_upload_idx` keys (e.g. legacy `fasta_path` literals) flow
    through untouched in the caller's binding map.

    Returns `(resolved_paths, upload_idxs_to_consume)`. The consume list
    is held until workflow success; mid-flight failures leave the
    referenced uploads in `ready` so the operator can decide whether to
    redrive against the same handles.
    """

    # Shared SUBMISSION-attributed BAD_INPUT shape (see _submission_bad_input).
    _bad = _submission_bad_input

    # First pass: validate keys + value shape, collect (key, prefix, upload_idx).
    pending: list[tuple[str, str, int]] = []
    for key, value in sorted(action_context.items()):
        if not key.endswith(_UPLOAD_IDX_SUFFIX):
            continue
        # Bare suffix as the full key — `"_upload_idx": N` — would
        # resolve to `_path`, clobbering any unrelated binding under the
        # same name. Reject the empty-prefix case so the convention's
        # `{prefix}_path` injection is always meaningful.
        prefix = key.removesuffix(_UPLOAD_IDX_SUFFIX)
        if not prefix:
            raise _bad(
                f"action_context key {key!r} has no name prefix before "
                f"{_UPLOAD_IDX_SUFFIX!r}; use e.g. fasta_upload_idx, not _upload_idx"
            )
        if not isinstance(value, int) or value <= 0:
            raise _bad(f"action_context.{key} must be a positive integer, got {value!r}")
        pending.append((key, prefix, value))

    if not pending:
        return {}, []

    # Second pass: single batched fetch keyed by upload_idx → row.
    upload_idxs = [p[2] for p in pending]
    rows = await pool.fetch(
        "SELECT upload_idx, status, created_by_idx FROM qiita.upload"
        " WHERE upload_idx = ANY($1::bigint[])",
        upload_idxs,
    )
    by_idx = {r["upload_idx"]: r for r in rows}

    resolved: dict[str, Path] = {}
    to_consume: list[int] = []
    for key, prefix, upload_idx in pending:
        row = by_idx.get(upload_idx)
        if row is None:
            raise _bad(f"action_context.{key}={upload_idx} references unknown upload")
        if row["status"] != UploadStatus.READY.value:
            raise _bad(
                f"action_context.{key}={upload_idx} expected status "
                f"{UploadStatus.READY.value!r}, got {row['status']!r}"
            )
        if row["created_by_idx"] != originator_principal_idx:
            raise _bad(
                f"action_context.{key}={upload_idx} was created by principal "
                f"{row['created_by_idx']}, work_ticket originator is "
                f"{originator_principal_idx}"
            )
        staging_path = compute_upload_staging_path(upload_staging_root, upload_idx)
        if not staging_path.exists():
            raise _bad(
                f"action_context.{key}={upload_idx} resolves to {staging_path} "
                "but the staged file is missing — CP and DP "
                "upload_staging_root disagree, or scratch was wiped"
            )
        resolved[prefix + _PATH_SUFFIX] = staging_path
        to_consume.append(upload_idx)
    return resolved, to_consume


async def _consume_upload_handles(
    pool: asyncpg.Pool | asyncpg.Connection, *, upload_idxs: list[int]
) -> None:
    """Bulk-transition `ready → consumed` for the listed upload rows.
    Mismatches (count of rows updated != len(upload_idxs)) raise a
    FINALIZE-stage BackendFailure so a stolen handle surfaces loudly
    instead of silently completing the workflow.

    Accepts either a pool or a live Connection so the success-path
    finalize block can run this inside the same transaction as the
    work_ticket COMPLETED transition."""
    if not upload_idxs:
        return
    # completed_at is pinned at the first terminal transition (the
    # pending→ready UPDATE in POST /upload/{idx}/done) per the migration
    # comment on `upload_terminal_has_completed_at`. Any other path that
    # mutates `status` off `pending` must populate `completed_at`; paths
    # that move between non-pending states (ready→consumed here, a future
    # consumed→archived, etc.) must NOT overwrite it.
    rows = await pool.fetch(
        "UPDATE qiita.upload"
        " SET status = $1"
        " WHERE upload_idx = ANY($2::bigint[])"
        "   AND status = $3"
        " RETURNING upload_idx",
        UploadStatus.CONSUMED.value,
        upload_idxs,
        UploadStatus.READY.value,
    )
    if len(rows) != len(upload_idxs):
        consumed = {r["upload_idx"] for r in rows}
        missing = sorted(set(upload_idxs) - consumed)
        raise BackendFailure(
            kind=FailureKind.UNKNOWN_PERMANENT,
            stage=WorkTicketFailureStage.FINALIZE,
            step_name=None,
            reason=(
                f"could not transition uploads {missing} from "
                f"{UploadStatus.READY.value!r} to {UploadStatus.CONSUMED.value!r}: "
                "concurrent state change"
            ),
        )
