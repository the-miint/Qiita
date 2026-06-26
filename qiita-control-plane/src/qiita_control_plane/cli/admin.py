"""qiita-admin — operator CLI for host-side and direct-DB tasks.

Placement rule (qiita vs qiita-admin) — the deciding test is how a command
reaches the system and whether the auth model can gate it:

  qiita        — credentialed API calls over HTTP+PAT; the server's
                 role/scope guards decide what's allowed. A command only a
                 system_admin can use still belongs in `qiita` if it's a
                 normal authenticated API call (the server 403s everyone
                 else) — the binary is not the security boundary.
  qiita-admin  — operator-on-the-host actions that run *outside* the
                 API/auth model: direct Postgres writes (gated by
                 DATABASE_URL) or host/cluster operations, for moments the
                 auth system can't help (no admin exists yet, the API is
                 down, or you're recovering state).

`token revoke-all` is HTTP+PAT and by the rule could live in `qiita`; it
stays here for operator discoverability, not because the split forces it.

Subcommands:
  set-system-role  — direct DB UPDATE of qiita.principal.system_role.
                     Used for the bootstrap path (first system_admin) and
                     when the operator has DB access but no PAT yet. Refuses
                     to operate on the system principal (idx=1).
  whoami           — calls GET /api/v1/auth/whoami via the configured PAT.
  token revoke-all — calls POST /api/v1/admin/principal/{idx}/revoke-all-tokens.
  login            — drives the AuthRocket LoginRocket Web flow end-to-end.
                     Spawns a localhost loopback HTTP server, opens a
                     browser to /api/v1/auth/login?cli=1&port=N, waits for
                     the handoff to redirect back with a one-time code,
                     exchanges the code at /api/v1/auth/cli-exchange for
                     a PAT, and writes the PAT to ~/.qiita/token (0600).
  actions sync     — read every action YAML under --workflows-dir and upsert
                     YAML-authoritative columns into qiita.action. Direct DB
                     write; reads DATABASE_URL from env. Idempotent: re-runs
                     converge to the YAML state without touching operational
                     columns (enabled / first_seen_at / disabled_*).
  ticket force-fail — direct-DB transition of a non-terminal work_ticket
                     to state=failed with a captured failure_type /
                     stage / step_name / reason. Replaces the previous
                     "operator writes UPDATE qiita.work_ticket by hand"
                     recovery pattern with a single command that
                     respects the schema's CHECK constraints. Refuses
                     to operate on already-terminal tickets.
  owner-biosample-id — HTTP+PAT export of the owner-submitted original
                     sample names for a study, written as a TSV to
                     --output (0600, never stdout). Maps biosample_idx +
                     biosample_accession back to the PII-pinned owner name
                     that is otherwise masked everywhere. With
                     --sequenced-pool-idx, restricts to that pool's samples
                     in the study and adds prep_sample_idx + ENA
                     experiment/run accessions. Server-gated by system_admin
                     + the admin:biosample_owner_id_read scope.
  work-ticket backfill-mask-idx — one-time idempotent backfill of
                     work_ticket.mask_idx for existing read-mask /
                     fastq-to-parquet tickets, by re-deriving each
                     ticket's mask params hash and LOOKING IT UP in
                     qiita.mask_definition (never minting). Dry-run by
                     default; --apply writes. Scoped to mask_idx IS NULL
                     so re-runs are no-ops.
  compute-readiness — exercise the path qiita-job needs end-to-end and
                     report per-check status (JWT, CP /healthz,
                     SLURM_NATIVE_PYTHON on host, plus an optional
                     SLURM probe-job that verifies the same env from
                     a compute node). Subprocess-execs into the
                     orchestrator's venv since the diagnostic uses the
                     orchestrator's Settings.from_env() and
                     SlurmrestdClient surfaces.
  mask delete      — calls DELETE /api/v1/mask-definition/{mask_idx} as
                     the configured PAT (system_admin via the
                     mask_definition:delete scope). The route does the
                     lake-first teardown (DuckLake read_mask rows then
                     the Postgres mask_definition row) and detaches any
                     referencing work_ticket via ON DELETE SET NULL.
                     Prints the rows_deleted count.
  mask purge-failed — bulk recovery for the read_mask move-then-read
                     bug: failed read-mask / fastq-to-parquet tickets
                     whose failure_reason carries "read_mask parquet not
                     found" (the mask IS registered in DuckLake; only the
                     metrics step failed). For each candidate it captures
                     the resubmit params, deletes the now-stale mask (so
                     the re-run won't duplicate read_mask rows), optionally
                     deletes the FAILED ticket (--with-tickets), then
                     RESUBMITS a fresh work_ticket against the fixed
                     workflow. Guards: a mask referenced by ANY non-failed
                     work_ticket is NEVER deleted (skipped + reported).
                     Dry-run by default; --execute required to mutate.
                     --limit caps, --rate throttles SLURM resubmits,
                     --wait polls each resubmit to a terminal state.
                     Mixes direct-DB reads (selector + shared-mask guard +
                     ticket delete) with PAT'd REST calls (mask delete +
                     resubmit), so it needs BOTH DATABASE_URL and a PAT.

Authentication for HTTP subcommands: read PAT from QIITA_TOKEN env var or
from ~/.qiita/token (mode 0600 expected). Loopback login flow, token I/O,
and the generic HTTP runner live in `cli._common`.
"""

import argparse
import asyncio
import base64
import contextlib
import csv
import itertools
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import asyncpg
import httpx
from pydantic import ValidationError
from qiita_common.api_paths import (
    PATH_ADMIN_MASKED_READ_EXPORT_TICKET,
    PATH_ADMIN_PREFIX,
    PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT,
    PATH_ADMIN_STUDY_OWNER_BIOSAMPLE_ID,
    PATH_MASK_DEFINITION_PREFIX,
    PATH_WORK_TICKET_PREFIX,
    PATH_WORK_TICKET_ROOT,
)
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole
from qiita_common.parquet import ROW_GROUP_SIZE_BYTES

from qiita_control_plane.actions import (
    DuplicateActionError,
    load_actions,
    sync_actions,
)
from qiita_control_plane.miint import connect_with_miint
from qiita_control_plane.runner import backfill_work_ticket_mask_idx

from . import _common

# Direct-DB connection timeout for the bootstrap subcommand. Short because the
# DB is expected to be reachable on the operator's network; a multi-second
# stall here masks misconfiguration.
_DB_CONNECT_TIMEOUT_SECONDS = 5

# Production install location for the orchestrator's venv. Same path
# the deploy script writes to and the systemd unit launches from —
# this constant is a default for the operator-side wrapper, not the
# source of truth; --orchestrator-venv overrides for dev hosts or
# unusual layouts. The wrapper subprocess-execs `<venv>/bin/python -m
# qiita_compute_orchestrator.cli.compute_readiness`.
_DEFAULT_ORCHESTRATOR_VENV = Path("/opt/qiita/compute-orchestrator/.venv")

# Derived from SystemRole so the role list isn't repeated anywhere in this
# file — adding `SystemRole.X` widens validation, error message, and `--help`
# automatically.
_VALID_ROLE_VALUES = tuple(r.value for r in SystemRole)


# ---------------------------------------------------------------------------
# Bootstrap subcommand: set-system-role (direct DB)
# ---------------------------------------------------------------------------


async def _set_system_role(database_url: str, email: str, role: str) -> int:
    """Update the principal's system_role by email lookup.

    Returns the principal_idx that was updated. Refuses to operate on
    idx=1 (the system principal). Raises with a clear message if the
    email is not found (the operator probably hasn't logged in via OIDC
    yet, which is what creates the principal+user pair).
    """
    if role not in _VALID_ROLE_VALUES:
        raise ValueError(f"role must be one of {' / '.join(_VALID_ROLE_VALUES)} (got {role!r})")
    try:
        conn = await asyncpg.connect(database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        idx = await conn.fetchval(
            "SELECT u.principal_idx FROM qiita.user u WHERE u.email = $1",
            email,
        )
        if idx is None:
            raise RuntimeError(
                f"no user with email {email!r} — has this user logged in"
                " via OIDC at least once? First login creates the principal+user"
                " rows; only then can their role be set."
            )
        if idx == SYSTEM_PRINCIPAL_IDX:
            raise RuntimeError(
                f"refusing to modify the system principal (idx={SYSTEM_PRINCIPAL_IDX})"
            )
        await conn.execute(
            "UPDATE qiita.principal SET system_role = $1 WHERE idx = $2",
            role,
            idx,
        )
        return idx
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# actions sync — direct-DB upsert of YAML-authoritative columns
# ---------------------------------------------------------------------------


async def _sync_actions(database_url: str, workflows_dir: Path) -> dict:
    """Load every action YAML under workflows_dir, then upsert into
    qiita.action inside one transaction. Returns a dict with counts of
    inserted, updated, and total actions found."""
    actions = load_actions(workflows_dir)
    if not actions:
        return {"found": 0, "inserted": 0, "updated": 0}
    try:
        conn = await asyncpg.connect(database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        result = await sync_actions(conn, actions)
    finally:
        await conn.close()
    return {"found": len(actions), **result}


# ---------------------------------------------------------------------------
# ticket force-fail — direct-DB transition of a non-terminal work_ticket
# ---------------------------------------------------------------------------

# work_ticket_failure_step_name_consistent in db/migrations/20260504000001
# requires failure_step_name IS NOT NULL iff failure_stage='step_run'.
# Mirrored here so the CLI fails before the DB does, with a clearer message.
_FAILURE_STAGES_REQUIRING_STEP_NAME = ("step_run",)
_FAILURE_STAGES_REJECTING_STEP_NAME = ("submission", "finalize")
_FAILURE_STAGE_CHOICES = _FAILURE_STAGES_REQUIRING_STEP_NAME + _FAILURE_STAGES_REJECTING_STEP_NAME

# Tickets in these states are eligible for force-fail; anything terminal
# (failed / completed) is rejected so the CLI doesn't silently overwrite
# a captured failure or convert a real success into a fake failure.
_FORCE_FAIL_ELIGIBLE_STATES = ("pending", "queued", "processing")


def _validate_force_fail_args(stage: str, step_name: str | None) -> None:
    """Surface CHECK violations before sending UPDATE so the error
    message names the constraint directly. Stage / step-name
    interlock matches work_ticket_failure_step_name_consistent."""
    if stage in _FAILURE_STAGES_REQUIRING_STEP_NAME and not step_name:
        raise ValueError(
            f"--step-name is required when --stage={stage} (mirrors the"
            " work_ticket_failure_step_name_consistent CHECK constraint)"
        )
    if stage in _FAILURE_STAGES_REJECTING_STEP_NAME and step_name:
        raise ValueError(
            f"--step-name must not be set when --stage={stage} (mirrors the"
            " work_ticket_failure_step_name_consistent CHECK constraint)"
        )


async def _force_fail_ticket(
    database_url: str,
    *,
    work_ticket_idx: int,
    stage: str,
    step_name: str | None,
    reason: str,
) -> dict:
    """Transition a non-terminal work_ticket to state=failed with the
    captured failure_* columns set. Refuses to overwrite an already-
    terminal ticket so a real success or a captured prior failure isn't
    lost.

    The CHECK constraint shape (work_ticket_failure_consistent +
    work_ticket_failure_step_name_consistent) is enforced by the DB;
    we validate stage / step-name compatibility client-side first
    (_validate_force_fail_args) so the error message is more direct than
    asyncpg's CheckViolationError surface.

    failure_type is always 'permanent' for the force-fail path: an
    operator hand-failing a stuck ticket has already concluded retries
    won't help. Sites that need a retriable force-fail (rare —
    PROCESSING tickets already get retry semantics from the runner)
    can extend this later.
    """
    _validate_force_fail_args(stage, step_name)
    try:
        conn = await asyncpg.connect(database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        async with conn.transaction():
            current_state = await conn.fetchval(
                "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1 FOR UPDATE",
                work_ticket_idx,
            )
            if current_state is None:
                raise RuntimeError(f"no work_ticket with idx={work_ticket_idx}")
            if current_state not in _FORCE_FAIL_ELIGIBLE_STATES:
                raise RuntimeError(
                    f"work_ticket idx={work_ticket_idx} is in terminal state"
                    f" {current_state!r}; refusing to overwrite. Eligible states:"
                    f" {', '.join(_FORCE_FAIL_ELIGIBLE_STATES)}."
                )
            await conn.execute(
                """
                UPDATE qiita.work_ticket
                SET state             = 'failed',
                    failure_type      = 'permanent',
                    failure_stage     = $2,
                    failure_step_name = $3,
                    failure_reason    = $4,
                    -- Clear any in-place-retry marker the runner left so the
                    -- force-failed ticket shows only its real failure surface,
                    -- not a stale "stuck since T" reason (covers the case where
                    -- the runner died before it could clear the marker itself).
                    transient_reason  = NULL,
                    transient_since   = NULL
                WHERE work_ticket_idx  = $1
                """,
                work_ticket_idx,
                stage,
                step_name,
                reason,
            )
        return {
            "work_ticket_idx": work_ticket_idx,
            "previous_state": current_state,
            "state": "failed",
            "failure_type": "permanent",
            "failure_stage": stage,
            "failure_step_name": step_name,
            "failure_reason": reason,
        }
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# work-ticket backfill-mask-idx — one-time idempotent mask_idx backfill
# ---------------------------------------------------------------------------


def _decode_hmac_secret() -> bytes:
    """Decode HMAC_SECRET_KEY (base64) the same way Settings.from_env does — the
    backfill re-materializes the canonical adapter set via a signed Flight ticket,
    so it needs the same signing key the CP boots with. Mirror from_env's
    >=16-byte floor so a too-short key is rejected here too (it would otherwise
    sign tickets the data plane refuses)."""
    raw = os.environ.get("HMAC_SECRET_KEY")
    if not raw:
        raise RuntimeError("HMAC_SECRET_KEY not set")
    try:
        secret = base64.b64decode(raw)
    except Exception as exc:  # noqa: BLE001 — surface the decode reason
        raise RuntimeError("HMAC_SECRET_KEY must be valid base64") from exc
    if len(secret) < 16:
        raise RuntimeError("HMAC_SECRET_KEY must decode to at least 16 bytes")
    return secret


def _parse_optional_adapter_ref() -> int | None:
    """Read QIITA_DEFAULT_ADAPTER_REFERENCE_IDX (the canonical adapter set the
    mask hash covers). Optional: a deploy without it minted maskless configs, and
    the backfill then derives params with adapter_set_hash=None."""
    raw = os.environ.get("QIITA_DEFAULT_ADAPTER_REFERENCE_IDX")
    if raw is None or raw == "":
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"QIITA_DEFAULT_ADAPTER_REFERENCE_IDX must be an integer, got {raw!r}"
        ) from exc
    if value <= 0:
        raise RuntimeError(f"QIITA_DEFAULT_ADAPTER_REFERENCE_IDX must be positive, got {value}")
    return value


async def _backfill_mask_idx(database_url: str, *, apply: bool) -> dict:
    """Acquire a pool, re-derive each eligible ticket's mask params, look it up,
    and (when apply) populate work_ticket.mask_idx. The adapter set is
    re-materialized into a throwaway temp workspace (only its bytes are hashed)."""
    data_plane_url = os.environ.get("DATA_PLANE_URL", "grpc://localhost:50051")
    default_adapter_reference_idx = _parse_optional_adapter_ref()
    # The HMAC key only signs the adapter-fetch Flight ticket; a maskless deploy
    # (no adapter reference configured) never re-materializes adapters, so require
    # the key only when it would actually be used.
    hmac_secret = _decode_hmac_secret() if default_adapter_reference_idx is not None else b""
    try:
        pool = await asyncpg.create_pool(
            database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS, min_size=1, max_size=4
        )
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        with tempfile.TemporaryDirectory(prefix="qiita-backfill-mask-") as tmp:
            return await backfill_work_ticket_mask_idx(
                pool,
                workspace=Path(tmp),
                default_adapter_reference_idx=default_adapter_reference_idx,
                data_plane_url=data_plane_url,
                hmac_secret=hmac_secret,
                apply=apply,
            )
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# mask delete / mask purge-failed
# ---------------------------------------------------------------------------

# The two affected workflows. The move-then-read ordering bug lived in both
# read-mask/1.0.0 and fastq-to-parquet/1.3.0 (same register→persist shape), so
# the recovery covers both. The selector keys on failure_reason, not workflow,
# but we still scope the candidate set to these action_ids so an unrelated
# action that happens to log the same string is never swept up.
_PURGE_FAILED_ACTION_IDS = ("read-mask", "fastq-to-parquet")

# The failure_reason substring the move-then-read bug leaves behind: host_filter
# and register-files both succeeded (the mask IS registered in DuckLake), only
# persist-read-metrics failed re-opening the moved-away staging path.
_READ_MASK_PARQUET_NOT_FOUND = "read_mask parquet not found"

# Resubmit is faithful ONLY for prep_sample-scoped tickets — both affected
# actions are prep_sample-scoped, so this is the only kind we expect. A
# candidate of any other kind is reported (defensive) rather than guessed at.
_RESUBMITTABLE_SCOPE_KIND = "prep_sample"


def _mask_delete_via_route(base_url: str, token: str, mask_idx: int) -> dict:
    """DELETE /api/v1/mask-definition/{mask_idx} as the PAT.

    Going through the route (not a direct data-plane DoAction) exercises the
    mask_definition:delete scope check AND the route's lake-first ordering
    (DuckLake read_mask rows → Postgres mask_definition row), which is exactly
    what the bulk tool wants per delete."""
    # _common.call prepends API_PREFIX, so pass the post-prefix segment.
    return _common.call(
        "DELETE",
        base_url,
        token,
        f"{PATH_MASK_DEFINITION_PREFIX}/{mask_idx}",
    )


async def _select_purge_failed_candidates(
    pool: asyncpg.Pool, *, action_ids: tuple[str, ...], limit: int | None
) -> list[asyncpg.Record]:
    """Failed tickets for the chosen action(s) carrying the move-then-read
    failure signature, with everything needed to resubmit. Ordered by
    work_ticket_idx so a --limit slice is stable across runs."""
    query = (
        "SELECT work_ticket_idx, action_id, action_version, scope_target_kind,"
        "       prep_sample_idx, action_context, originator_principal_idx, mask_idx"
        "  FROM qiita.work_ticket"
        " WHERE state = 'failed'"
        "   AND action_id = ANY($1::text[])"
        "   AND failure_reason LIKE '%' || $2 || '%'"
        " ORDER BY work_ticket_idx"
    )
    args: list = [list(action_ids), _READ_MASK_PARQUET_NOT_FOUND]
    if limit is not None:
        query += " LIMIT $3"
        args.append(limit)
    return await pool.fetch(query, *args)


async def _count_non_failed_missing_mask_idx(
    pool: asyncpg.Pool, *, action_ids: tuple[str, ...]
) -> int:
    """Count non-failed tickets for these action(s) that have a NULL mask_idx.

    This is the backfill-completeness gate. The shared-mask guard
    (_mask_shared_with_non_failed) keys on `mask_idx = $1 AND state <> 'failed'`,
    so a non-failed ticket that genuinely shares a mask but whose mask_idx was
    never backfilled (still NULL) is INVISIBLE to the guard — we could then
    delete a mask a COMPLETED result depends on, silently dropping its read_mask
    rows. While ANY such ticket exists, the guard is unsound, so --execute must
    refuse. (Tickets in a *failed* state with NULL mask_idx are fine here: they
    are not the ones the guard protects — they land in skipped_no_mask_idx.)"""
    return await pool.fetchval(
        "SELECT COUNT(*) FROM qiita.work_ticket"
        " WHERE action_id = ANY($1::text[])"
        "   AND state <> 'failed'"
        "   AND mask_idx IS NULL",
        list(action_ids),
    )


async def _mask_shared_with_non_failed(pool: asyncpg.Pool, mask_idx: int) -> list[int]:
    """work_ticket_idxs in ANY non-failed state that reference this mask.

    The shared-mask guard: the config-hash dedup means one mask_idx can back
    many tickets across runs, including COMPLETED ones. Deleting a mask a
    non-failed ticket depends on would silently drop a live result's read_mask
    rows — so if this returns a non-empty list, we SKIP the mask entirely.
    Relies on backfill-mask-idx having populated mask_idx on non-failed tickets
    too (else they read NULL and the guard misses them)."""
    rows = await pool.fetch(
        "SELECT work_ticket_idx FROM qiita.work_ticket"
        " WHERE mask_idx = $1 AND state <> 'failed'"
        " ORDER BY work_ticket_idx",
        mask_idx,
    )
    return [r["work_ticket_idx"] for r in rows]


def _build_resubmit_body(row: asyncpg.Record) -> dict:
    """Reconstruct the WorkTicketCreateRequest body from a stored ticket row.

    Both affected actions are prep_sample-scoped, so the only scope_target we
    rebuild is the prep_sample form. action_context is stored as JSON text on the
    row; decode it back to the object the submit route validates against the
    action's context_schema. originator is NOT carried — the resubmit route sets
    originator_principal_idx server-side from the authenticated caller (the
    operator running this command), which is the intended provenance for a
    recovery re-run."""
    raw_context = row["action_context"]
    if isinstance(raw_context, str):
        action_context = json.loads(raw_context) if raw_context else {}
    elif raw_context is None:
        action_context = {}
    else:
        action_context = dict(raw_context)
    return {
        "action_id": row["action_id"],
        "action_version": row["action_version"],
        "scope_target": {
            "kind": _RESUBMITTABLE_SCOPE_KIND,
            "prep_sample_idx": row["prep_sample_idx"],
        },
        "action_context": action_context,
    }


def _resubmit_work_ticket(base_url: str, token: str, body: dict) -> dict:
    """POST /api/v1/work-ticket as the PAT, returning {work_ticket_idx, state}.

    Reuses the normal submission path so the resubmit goes through the same
    validation, disallow-without-delete gate (a FAILED original never blocks),
    and dispatch a fresh `qiita` submit would. With the stale mask purged first,
    the re-run mints a fresh mask_idx and runs clean on the reordered workflow."""
    return _common.call(
        "POST", base_url, token, f"{PATH_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_ROOT}", json=body
    )


# Terminal work-ticket states the --wait poll stops on.
_TERMINAL_TICKET_STATES = frozenset({"completed", "no_data", "failed"})
# Default poll cadence + ceiling for --wait. Generous ceiling because a real
# read-mask run is a SLURM job; the operator can Ctrl-C and re-check by hand.
_WAIT_POLL_INTERVAL_SECONDS = 10
_WAIT_TIMEOUT_SECONDS = 3600


def _poll_ticket_to_terminal(base_url: str, token: str, work_ticket_idx: int) -> str:
    """Poll GET /work-ticket/{idx} until a terminal state or the wait ceiling.

    Returns the final observed state (which may still be non-terminal if the
    ceiling is hit — the caller reports it as 'still running' rather than
    blocking the whole batch forever)."""
    deadline = time.monotonic() + _WAIT_TIMEOUT_SECONDS
    state = "unknown"
    while time.monotonic() < deadline:
        body = _common.call("GET", base_url, token, f"{PATH_WORK_TICKET_PREFIX}/{work_ticket_idx}")
        state = body.get("state", "unknown")
        if state in _TERMINAL_TICKET_STATES:
            return state
        time.sleep(_WAIT_POLL_INTERVAL_SECONDS)
    return state


async def _purge_failed(
    database_url: str,
    base_url: str,
    token: str,
    *,
    action_ids: tuple[str, ...],
    execute: bool,
    with_tickets: bool,
    limit: int | None,
    rate_seconds: float,
    wait: bool,
) -> dict:
    """Drive the bulk purge-and-resubmit recovery.

    Dry-run (default): select candidates, run the shared-mask guard, and report
    exactly what WOULD be purged/resubmitted — writes NOTHING.

    --execute, per ticket (capture-before-delete, with per-item isolation so one
    failure doesn't abort the batch):
      1. capture the resubmit body from the ticket row FIRST;
      2. delete the mask via the route (drops the registered read_mask rows so
         the resubmit won't duplicate them);
      3. if --with-tickets, DELETE the FAILED work_ticket row (steps CASCADE);
      4. resubmit a fresh ticket via POST /work-ticket;
      5. if --wait, poll the resubmit to a terminal state.

    Recovery: per-item failures are isolated and reported with everything
    needed to replay the submission by hand — work_ticket_idx, mask_idx, the
    captured resubmit_body, and what already happened (mask_deleted,
    ticket_deleted). If a resubmit fails AFTER its mask was deleted, the mask's
    read_mask rows are already gone, so a plain re-POST of the reported
    resubmit_body to POST /work-ticket is safe: the re-run mints a fresh
    mask_idx and cannot duplicate rows. The command exits non-zero whenever the
    failures list is non-empty.
    """
    try:
        pool = await asyncpg.create_pool(
            database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS, min_size=1, max_size=4
        )
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        # Backfill-completeness gate (computed up front so dry-run reports it and
        # --execute can refuse on it before any destructive work). The shared-mask
        # guard is only sound once every NON-failed ticket carries its mask_idx;
        # a non-failed sharer with a NULL mask_idx is invisible to the guard, so
        # the mask could be wrongly deleted out from under a live result.
        backfill_incomplete = await _count_non_failed_missing_mask_idx(pool, action_ids=action_ids)

        candidates = await _select_purge_failed_candidates(pool, action_ids=action_ids, limit=limit)

        # Classify candidates up front so the dry-run report and the execute
        # path see the same buckets. A candidate is:
        #   - skipped_no_mask_idx: mask_idx is NULL (backfill never matched it —
        #     can't safely purge a mask we can't name; resubmit alone would
        #     duplicate the existing read_mask rows). Report, never touch.
        #   - skipped_wrong_kind: not prep_sample-scoped (defensive; the two
        #     affected actions are always prep_sample). Report, never touch.
        #   - skipped_shared: mask_idx referenced by a non-failed ticket. The
        #     critical guard — report, never delete that mask.
        #   - eligible: safe to purge + resubmit.
        eligible: list[dict] = []
        skipped_no_mask_idx: list[int] = []
        skipped_wrong_kind: list[int] = []
        skipped_shared: list[dict] = []
        # One mask can back several failed candidates; guard each distinct mask
        # once and cache the verdict so the report counts a shared mask once.
        guard_cache: dict[int, list[int]] = {}

        for row in candidates:
            wt_idx = row["work_ticket_idx"]
            mask_idx = row["mask_idx"]
            if row["scope_target_kind"] != _RESUBMITTABLE_SCOPE_KIND:
                skipped_wrong_kind.append(wt_idx)
                continue
            if mask_idx is None:
                skipped_no_mask_idx.append(wt_idx)
                continue
            if mask_idx not in guard_cache:
                guard_cache[mask_idx] = await _mask_shared_with_non_failed(pool, mask_idx)
            non_failed = guard_cache[mask_idx]
            if non_failed:
                skipped_shared.append(
                    {
                        "work_ticket_idx": wt_idx,
                        "mask_idx": mask_idx,
                        "non_failed_work_ticket_idxs": non_failed,
                    }
                )
                continue
            eligible.append(
                {
                    "work_ticket_idx": wt_idx,
                    "mask_idx": mask_idx,
                    "prep_sample_idx": row["prep_sample_idx"],
                    "row": row,
                }
            )

        report: dict = {
            "executed": execute,
            "with_tickets": with_tickets,
            "action_ids": list(action_ids),
            "backfill_incomplete": backfill_incomplete,
            "candidates": len(candidates),
            "eligible": [
                {k: e[k] for k in ("work_ticket_idx", "mask_idx", "prep_sample_idx")}
                for e in eligible
            ],
            "skipped_shared": skipped_shared,
            "skipped_no_mask_idx": skipped_no_mask_idx,
            "skipped_wrong_kind": skipped_wrong_kind,
            "purged": [],
            "resubmitted": [],
            "failures": [],
        }

        if not execute:
            return report

        # Refuse to do any destructive work while the shared-mask guard is unsound
        # (some non-failed ticket still has a NULL mask_idx, invisible to the
        # guard). Fail loudly with the count and the exact fix-up command.
        if backfill_incomplete:
            raise RuntimeError(
                f"backfill incomplete: {backfill_incomplete} non-failed work_ticket(s)"
                f" for {list(action_ids)} have mask_idx IS NULL, so the shared-mask"
                " guard cannot see them and a shared mask could be wrongly deleted."
                " Run `qiita-admin work-ticket backfill-mask-idx --apply` first, then"
                " re-run this command."
            )

        # --execute: process each eligible candidate in isolation. Mask deletes
        # dedup across candidates that share a mask (only the first delete finds
        # rows; the route is idempotent for the rest).
        deleted_masks: set[int] = set()
        for i, e in enumerate(eligible):
            wt_idx = e["work_ticket_idx"]
            mask_idx = e["mask_idx"]
            # Capture the resubmit body BEFORE the try so it (and the
            # progress flags) are always available for a recoverable failure
            # report, even if mask-delete itself raises.
            resubmit_body = _build_resubmit_body(e["row"])
            mask_deleted = mask_idx in deleted_masks
            ticket_deleted = False
            try:
                # 1. Delete the mask via the route (lake-first; idempotent).
                if mask_idx not in deleted_masks:
                    del_result = _mask_delete_via_route(base_url, token, mask_idx)
                    deleted_masks.add(mask_idx)
                    mask_deleted = True
                    report["purged"].append(
                        {
                            "work_ticket_idx": wt_idx,
                            "mask_idx": mask_idx,
                            "rows_deleted": del_result.get("rows_deleted"),
                        }
                    )

                # 2. Optionally delete the FAILED ticket (work_ticket_step
                #    CASCADEs). Re-assert state='failed' in the WHERE so a ticket
                #    that somehow moved off 'failed' between select and now is
                #    never deleted.
                if with_tickets:
                    await pool.execute(
                        "DELETE FROM qiita.work_ticket"
                        " WHERE work_ticket_idx = $1 AND state = 'failed'",
                        wt_idx,
                    )
                    ticket_deleted = True

                # 3. Resubmit a fresh ticket via the normal submission path.
                submitted = _resubmit_work_ticket(base_url, token, resubmit_body)
                new_idx = submitted.get("work_ticket_idx")
                entry = {
                    "original_work_ticket_idx": wt_idx,
                    "new_work_ticket_idx": new_idx,
                    "prep_sample_idx": e["prep_sample_idx"],
                    "state": submitted.get("state"),
                }

                # 4. Optionally poll the resubmit to terminal.
                if wait and new_idx is not None:
                    poll_state = _poll_ticket_to_terminal(base_url, token, new_idx)
                    entry["observed_state"] = poll_state
                    if poll_state not in _TERMINAL_TICKET_STATES:
                        # The wait ceiling was hit before a terminal state; mark
                        # the entry honestly rather than asserting terminality.
                        entry["timed_out"] = True
                report["resubmitted"].append(entry)
            except (httpx.HTTPError, asyncpg.PostgresError, ValueError, RuntimeError) as exc:
                # Capture enough to REPLAY this submission by hand. If the mask
                # was already deleted, a plain re-POST of resubmit_body is safe —
                # the mask's read_mask rows are gone, so the re-run mints a fresh
                # mask_idx and cannot duplicate rows.
                report["failures"].append(
                    {
                        "work_ticket_idx": wt_idx,
                        "mask_idx": mask_idx,
                        "mask_deleted": mask_deleted,
                        "ticket_deleted": ticket_deleted,
                        "resubmit_body": resubmit_body,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

            # Throttle SLURM resubmits (skip the sleep after the final item).
            if rate_seconds > 0 and i < len(eligible) - 1:
                time.sleep(rate_seconds)

        return report
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# HTTP subcommand helpers
# ---------------------------------------------------------------------------


def _token_revoke_all(base_url: str, token: str, principal_idx: int) -> dict:
    return _common.call(
        "POST",
        base_url,
        token,
        f"/admin/principal/{principal_idx}/revoke-all-tokens",
    )


# Owner-biosample-id export. Column order is fixed so the TSV is stable across
# runs; the owner name goes last (the sensitive payload after the identifiers).
# The pool variant adds the sequencing-pathway columns the server only fills
# when filtered to a pool.
_OWNER_ID_BASE_COLUMNS = ("biosample_idx", "biosample_accession", "owner_biosample_id")
_OWNER_ID_POOL_COLUMNS = (
    "biosample_idx",
    "biosample_accession",
    "prep_sample_idx",
    "ena_experiment_accession",
    "ena_run_accession",
    "owner_biosample_id",
)


def _write_owner_biosample_id_tsv(body: dict, output: Path) -> int:
    """Write the export `body` (an OwnerBiosampleIdExportResponse) to `output`
    as a header + tab-separated rows. NULL JSON values become empty cells.

    Written to a temp file in the same directory (created mode 0600 — the rows
    hold the owner-submitted names, which are PII) then atomically `os.replace`d
    into place, so a mid-write failure (disk full, etc.) can never truncate an
    existing export: either the new file lands whole or the old one is untouched.
    The temp file is removed on any failure so a stray partial is never left.

    Returns the row count written (excluding the header).
    """
    columns = (
        _OWNER_ID_POOL_COLUMNS if body["sequenced_pool_idx"] is not None else _OWNER_ID_BASE_COLUMNS
    )
    rows = body["rows"]
    fd, tmp_name = tempfile.mkstemp(dir=output.parent, prefix=f".{output.name}.", suffix=".tmp")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", newline="") as fh:
            writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
            writer.writerow(columns)
            for row in rows:
                writer.writerow(["" if row.get(col) is None else row[col] for col in columns])
        os.replace(tmp_name, output)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise
    return len(rows)


# ---------------------------------------------------------------------------
# argparse entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qiita-admin", description="Qiita admin CLI")
    _common.add_base_url_arg(parser)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_role = sub.add_parser(
        "set-system-role",
        help="Direct-DB role update (bootstrap path)",
    )
    p_role.add_argument("--email", required=True)
    p_role.add_argument(
        "--role",
        required=True,
        choices=list(_VALID_ROLE_VALUES),
    )
    p_role.set_defaults(handler=_handle_set_system_role)

    p_whoami = sub.add_parser("whoami", help="Print the authenticated principal")
    p_whoami.set_defaults(handler=_handle_whoami)

    p_token = sub.add_parser("token", help="Token operations")
    p_token_sub = p_token.add_subparsers(dest="token_cmd", required=True)
    p_revoke = p_token_sub.add_parser("revoke-all", help="Bulk-revoke all of a principal's tokens")
    p_revoke.add_argument("--principal-idx", required=True, type=int)
    p_revoke.set_defaults(handler=_handle_token_revoke_all)

    p_login = sub.add_parser(
        "login",
        help="AuthRocket LoginRocket Web flow with localhost loopback",
    )
    _common.add_token_file_arg(p_login)
    p_login.set_defaults(handler=_handle_login)

    p_ticket = sub.add_parser("ticket", help="Work-ticket operations")
    p_ticket_sub = p_ticket.add_subparsers(dest="ticket_cmd", required=True)
    p_force_fail = p_ticket_sub.add_parser(
        "force-fail",
        help=(
            "Direct-DB transition of a non-terminal work_ticket to state=failed."
            " Replaces the previous 'operator runs UPDATE qiita.work_ticket by"
            " hand' recovery pattern."
        ),
    )
    p_force_fail.add_argument(
        "--idx", required=True, type=int, dest="work_ticket_idx", help="work_ticket_idx"
    )
    p_force_fail.add_argument("--reason", required=True, help="Operator-supplied failure_reason")
    p_force_fail.add_argument(
        "--stage",
        required=True,
        choices=list(_FAILURE_STAGE_CHOICES),
        help=(
            "failure_stage: submission / step_run / finalize."
            " --step-name is required when --stage=step_run and rejected otherwise."
        ),
    )
    p_force_fail.add_argument(
        "--step-name",
        dest="step_name",
        default=None,
        help="failure_step_name (required iff --stage=step_run)",
    )
    p_force_fail.set_defaults(handler=_handle_ticket_force_fail)

    p_work_ticket = sub.add_parser("work-ticket", help="Work-ticket maintenance operations")
    p_work_ticket_sub = p_work_ticket.add_subparsers(dest="work_ticket_cmd", required=True)
    p_backfill = p_work_ticket_sub.add_parser(
        "backfill-mask-idx",
        help=(
            "One-time idempotent backfill of work_ticket.mask_idx for existing"
            " read-mask / fastq-to-parquet tickets. Re-derives each ticket's mask"
            " params hash and LOOKS IT UP in mask_definition (never mints). Dry-run"
            " by default; pass --apply to write."
        ),
    )
    p_backfill.add_argument(
        "--apply",
        action="store_true",
        help="Write the populated mask_idx values (default: dry-run, report only).",
    )
    p_backfill.set_defaults(handler=_handle_work_ticket_backfill_mask_idx)

    p_mask = sub.add_parser("mask", help="Mask-definition maintenance operations")
    p_mask_sub = p_mask.add_subparsers(dest="mask_cmd", required=True)
    p_mask_delete = p_mask_sub.add_parser(
        "delete",
        help=(
            "Delete one mask via DELETE /mask-definition/{mask_idx} (system_admin,"
            " mask_definition:delete). Drops its DuckLake read_mask rows then the"
            " Postgres mask_definition row; referencing work_tickets detach"
            " (ON DELETE SET NULL). Prints rows_deleted."
        ),
    )
    p_mask_delete.add_argument("mask_idx", type=int, help="mask_idx to delete")
    p_mask_delete.set_defaults(handler=_handle_mask_delete)

    p_purge = p_mask_sub.add_parser(
        "purge-failed",
        help=(
            "Bulk purge-and-resubmit recovery for read-mask / fastq-to-parquet"
            " tickets that FAILED with 'read_mask parquet not found' (the mask is"
            " registered in DuckLake; only persist-read-metrics failed). Per"
            " ticket: capture resubmit params, delete the stale mask (so the"
            " re-run won't duplicate read_mask rows), optionally delete the FAILED"
            " ticket (--with-tickets), then RESUBMIT a fresh ticket. NEVER deletes"
            " a mask referenced by a non-failed work_ticket (shared-mask guard:"
            " skipped + reported). Dry-run by default; pass --execute to mutate."
            " Needs DATABASE_URL and a PAT."
        ),
    )
    p_purge.add_argument(
        "--action",
        required=True,
        choices=["read-mask", "fastq-to-parquet", "all"],
        help="Which action(s) to recover. 'all' covers both affected workflows.",
    )
    p_purge.add_argument(
        "--execute",
        action="store_true",
        help="Perform the purge + resubmit (default: dry-run, report only, no writes).",
    )
    p_purge.add_argument(
        "--with-tickets",
        action="store_true",
        dest="with_tickets",
        help=(
            "Also DELETE the FAILED work_ticket rows (work_ticket_step CASCADEs)."
            " Only ever deletes tickets in state='failed'."
        ),
    )
    p_purge.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap how many candidate tickets are processed (ordered by work_ticket_idx).",
    )
    p_purge.add_argument(
        "--rate",
        type=float,
        default=0.0,
        dest="rate_seconds",
        help="Seconds to sleep between resubmits so SLURM isn't flooded (default: 0).",
    )
    p_purge.add_argument(
        "--wait",
        action="store_true",
        help="After each resubmit, poll the new ticket to a terminal state and report it.",
    )
    p_purge.set_defaults(handler=_handle_mask_purge_failed)

    p_actions = sub.add_parser("actions", help="Action registry operations")
    p_actions_sub = p_actions.add_subparsers(dest="actions_cmd", required=True)
    p_actions_sync = p_actions_sub.add_parser(
        "sync",
        help="Upsert workflows YAMLs into qiita.action (YAML-authoritative columns only)",
    )
    p_actions_sync.add_argument(
        "--workflows-dir",
        type=Path,
        default=Path("workflows"),
        help="Directory to scan for action YAMLs (default: ./workflows)",
    )
    p_actions_sync.set_defaults(handler=_handle_actions_sync)

    p_owner_id = sub.add_parser(
        "owner-biosample-id",
        help=(
            "Export the owner-submitted original sample names for a study as a"
            " TSV (system_admin only). Maps biosample_idx + accession back to"
            " the PII-pinned owner name."
        ),
    )
    p_owner_id.add_argument(
        "--study-idx",
        required=True,
        type=int,
        dest="study_idx",
        help="study_idx to export (required).",
    )
    p_owner_id.add_argument(
        "--sequenced-pool-idx",
        type=int,
        default=None,
        dest="sequenced_pool_idx",
        help=(
            "Restrict to this sequenced_pool's samples within the study and add"
            " prep_sample_idx + ENA experiment/run accession columns."
        ),
    )
    p_owner_id.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Path to write the TSV to (created mode 0600; never printed to stdout).",
    )
    p_owner_id.set_defaults(handler=_handle_owner_biosample_id)

    p_export = sub.add_parser(
        "masked-read-export",
        help=(
            "Export masked sequence data for every sample on a sequenced_pool"
            " (system_admin only). Streams each sample's masked reads from the"
            " data plane and writes per-sample files named"
            " <biosample_accession>.<run>.<pool>.<prep>[.R1/.R2].<parquet|fastq>"
            " (paired fastq splits into R1/R2)."
        ),
    )
    p_export.add_argument(
        "--sequenced-pool-idx",
        required=True,
        type=int,
        dest="sequenced_pool_idx",
        help="sequenced_pool to export every (non-retired) sample of (required).",
    )
    p_export.add_argument(
        "--mask-idx",
        required=True,
        type=int,
        dest="mask_idx",
        help="mask_idx identifying which masked reads to export (required).",
    )
    p_export.add_argument(
        "--format",
        choices=("parquet", "fastq"),
        default="parquet",
        help=(
            "Output format. parquet → one <stem>.parquet per sample; fastq →"
            " one <stem>.fastq for a single-end sample, or split"
            " <stem>.R1.fastq + <stem>.R2.fastq for a paired sample."
        ),
    )
    p_export.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        dest="output_dir",
        help="Existing directory to write per-sample files into (created mode 0600).",
    )
    p_export.add_argument(
        "--data-plane-url",
        required=True,
        dest="data_plane_url",
        help="gRPC URL of the data plane (e.g. grpc://qiita-data.example.com:50051).",
    )
    p_export.set_defaults(handler=_handle_masked_read_export)

    p_readiness = sub.add_parser(
        "compute-readiness",
        help=(
            "Exercise the path qiita-job needs and report per-check status."
            " Local checks (JWT, CP /healthz, SLURM_NATIVE_PYTHON on host)"
            " plus an optional SLURM probe-job."
        ),
    )
    p_readiness.add_argument(
        "--orchestrator-venv",
        type=Path,
        default=_DEFAULT_ORCHESTRATOR_VENV,
        help=(
            "Path to the orchestrator's venv; the wrapper invokes"
            f" `<venv>/bin/python -m qiita_compute_orchestrator.cli.compute_readiness`."
            f" Default: {_DEFAULT_ORCHESTRATOR_VENV}"
        ),
    )
    p_readiness.add_argument(
        "--no-slurm-probe",
        action="store_true",
        dest="no_slurm_probe",
        help="Skip the SLURM submit phase; run local checks only.",
    )
    p_readiness.add_argument(
        "--json",
        action="store_true",
        dest="emit_json",
        help="Emit JSON instead of the human-readable report.",
    )
    p_readiness.add_argument(
        "--probe-timeout-seconds",
        type=float,
        default=None,
        help=(
            "Override the orchestrator-side wait for the SLURM probe-job"
            " (the probe itself also has a SLURM time_limit). Default: rely"
            " on the orchestrator-side default."
        ),
    )
    p_readiness.set_defaults(handler=_handle_compute_readiness)

    return parser


# ---------------------------------------------------------------------------
# Subcommand handlers (registered via parser.set_defaults(handler=...))
# ---------------------------------------------------------------------------


def _handle_set_system_role(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        idx = asyncio.run(_set_system_role(database_url, args.email, args.role))
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"updated principal idx={idx} system_role={args.role}")
    return 0


def _handle_whoami(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(lambda t: _common.whoami(args.base_url, t))


def _handle_token_revoke_all(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda t: _token_revoke_all(args.base_url, t, args.principal_idx)
    )


def _handle_owner_biosample_id(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """GET the owner-id export and write it to --output as a TSV.

    Not routed through run_http_subcommand because it writes a file rather than
    printing to stdout: it owns its own token-read, HTTP-error, and write-error
    handling so each failure surfaces as a clean stderr message + non-zero exit
    (not a traceback). The owner names are PII, so they go only to the (0600)
    output file — stdout gets a row-count summary, never the names themselves.
    """
    output: Path = args.output
    if not output.parent.is_dir():
        print(f"error: output directory does not exist: {output.parent}", file=sys.stderr)
        return 2

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    path = (
        f"{PATH_ADMIN_PREFIX}{PATH_ADMIN_STUDY_OWNER_BIOSAMPLE_ID.format(study_idx=args.study_idx)}"
    )
    params = (
        {"sequenced_pool_idx": args.sequenced_pool_idx}
        if args.sequenced_pool_idx is not None
        else None
    )
    try:
        body = _common.call("GET", args.base_url, token, path, params=params)
    except httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1

    try:
        n = _write_owner_biosample_id_tsv(body, output)
    except OSError as exc:
        print(f"error: could not write {output}: {exc}", file=sys.stderr)
        return 1
    print(f"wrote {n} rows to {output}")
    return 0


# Conservative accession charset: the accession is the leading filename
# component AND (via the output path) is interpolated into the DuckDB COPY SQL,
# so reject anything outside [A-Za-z0-9._-] — that excludes '/' (path traversal)
# and "'" (SQL-string break). ENA/NCBI accessions are alphanumeric in practice.
_SAFE_ACCESSION = re.compile(r"^[A-Za-z0-9._-]+$")


def _sql_str(path: Path) -> str:
    """Escape a filesystem path for inlining as a DuckDB SQL string literal."""
    return str(path).replace("'", "''")


# The read_masked view's columns, in the verbatim order the miint FORMAT FASTQ
# writer requires (read_id, sequence1, qual1, sequence2, qual2). Projected by the
# fastq COPY; aliasing any of these away raises a BinderException (pinned by the
# orchestrator's masked-export fastq contract test).
_READ_MASKED_COLUMNS = "read_id, sequence1, qual1, sequence2, qual2"


def _commit_partials(copy_fn, pairs: list[tuple[Path, Path]]) -> None:
    """Run `copy_fn` (which COPYs the masked rows into each pair's `.partial`),
    then move each partial into place. Each partial is chmod 0600 *before* the
    rename — the reads are privacy-masked sequence data, so the file is never
    visible at its final name under a looser umask, even for an instant.

    All-or-nothing across the pair: on any failure (COPY error, or a rename/chmod
    failing partway through a paired R1+R2 commit) every partial AND every
    already-committed final is removed, so a retry never finds a half-written
    file or a lone R1 without its R2. The partial paths are known up front so a
    failure *inside* the COPY (which may have already created some partials) is
    cleaned up too."""
    committed: list[Path] = []
    try:
        copy_fn()
        for partial, final in pairs:
            partial.chmod(0o600)
            os.replace(partial, final)
            committed.append(final)
    except BaseException:
        for partial, _ in pairs:
            with contextlib.suppress(FileNotFoundError):
                partial.unlink()
        for final in committed:
            with contextlib.suppress(FileNotFoundError):
                final.unlink()
        raise


def _peek_paired(reader):
    """Decide single-end vs paired from the Arrow `reader` WITHOUT draining it.

    A prep_sample is uniformly single- or paired-end — the mask filter drops
    reads but never changes R1/R2 layout — so the first non-empty batch is
    representative. Read leading batches until one carries rows, read pairing off
    that batch's `sequence2` null-ness, then return `(paired, stream)` where
    `stream` re-prepends the peeked batches in front of the still-unconsumed tail.
    This lets the fastq COPY stream straight through (bounded to one batch) rather
    than materializing the whole sample just to choose its output target. An empty
    stream (no rows at all) reports single-end."""
    import pyarrow as pa  # noqa: PLC0415

    schema = reader.schema
    sequence2_idx = schema.get_field_index("sequence2")
    peeked: list = []
    paired = False
    for batch in reader:
        peeked.append(batch)
        if batch.num_rows:
            paired = batch.column(sequence2_idx).null_count < batch.num_rows
            break
    stream = pa.RecordBatchReader.from_batches(schema, itertools.chain(peeked, reader))
    return paired, stream


def _write_masked_sample(reader, stem: str, output_dir: Path, fmt: str, con) -> None:
    """Write one sample's streamed masked reads under output_dir, atomically (via
    a `.partial` sibling renamed into place) and chmod 0600. Both formats stream
    the Arrow `reader` (bounded memory, no full materialization):

      parquet — stream straight to a `pyarrow.parquet.ParquetWriter` (zstd) into
                one `<stem>.parquet`. No DuckDB hop, so the bulk read bytes are
                never materialized into DuckDB vectors and the scan never touches
                Acero (which is why the parquet path needs no buffer realignment —
                see `_handle_masked_read_export`). `con` is unused (pass None). The
                writer is opened from `reader.schema`, so a zero-row stream still
                produces a valid empty `<stem>.parquet`.
      fastq   — stream through the caller's shared miint DuckDB `con` (the FORMAT
                FASTQ writer lives in DuckDB+miint; `con` is reused across all
                samples). Output is gzip-compressed (`<stem>.fastq.gz`). The
                manifest carries no paired flag, so pairing is read from the data
                (`sequence2` null-ness) by peeking the first batch (`_peek_paired`),
                without draining the single-pass reader. A single-end sample → one
                `<stem>.fastq.gz`; a paired sample → `<stem>.R1.fastq.gz` +
                `<stem>.R2.fastq.gz` via miint's `{ORIENTATION}` placeholder
                (paired rows into a single path are a hard error in the writer;
                should the per-sample SE/PE uniformity ever break, a misdetected
                single-end COPY hits that error and fails loudly)."""
    if fmt == "parquet":
        import pyarrow as pa  # noqa: PLC0415
        import pyarrow.parquet as pq  # noqa: PLC0415

        partial = output_dir / f"{stem}.parquet.partial"

        def _write_parquet() -> None:
            # The data plane streams ~2048-row DuckDB DataChunks, so writing each
            # incoming batch as its own row group would fragment the file into
            # hundreds of tiny row groups (worse compression + pruning). Buffer
            # batches up to one row group's worth and write them as a single row
            # group — reproducing the layout (and bounded peak memory) of the
            # DuckDB `COPY` this path replaced. Size the group by encoded bytes
            # (ROW_GROUP_SIZE_BYTES, the qiita-wide cap from PARQUET_OPTS) rather
            # than a fixed row count, so wide rows don't produce oversized groups;
            # batch.nbytes is the in-memory size DuckDB's byte cap also measures.
            writer = pq.ParquetWriter(partial, reader.schema, compression="zstd")
            try:
                buffer: list = []
                buffered_bytes = 0

                def flush() -> None:
                    nonlocal buffer, buffered_bytes
                    if buffer:
                        writer.write_table(pa.Table.from_batches(buffer, reader.schema))
                        buffer = []
                        buffered_bytes = 0

                for batch in reader:
                    buffer.append(batch)
                    buffered_bytes += batch.nbytes
                    if buffered_bytes >= ROW_GROUP_SIZE_BYTES:
                        flush()
                flush()
            finally:
                writer.close()

        _commit_partials(_write_parquet, [(partial, output_dir / f"{stem}.parquet")])
    elif fmt == "fastq":
        paired, stream = _peek_paired(reader)
        con.register("masked", stream)
        if paired:
            # `{ORIENTATION}` expands to R1/R2, so the one COPY emits both
            # `<stem>.R1.fastq.gz.partial` and `<stem>.R2.fastq.gz.partial`.
            target = output_dir / f"{stem}.{{ORIENTATION}}.fastq.gz.partial"
            pairs = [
                (
                    output_dir / f"{stem}.{o}.fastq.gz.partial",
                    output_dir / f"{stem}.{o}.fastq.gz",
                )
                for o in ("R1", "R2")
            ]
        else:
            target = output_dir / f"{stem}.fastq.gz.partial"
            pairs = [(target, output_dir / f"{stem}.fastq.gz")]
        _commit_partials(
            lambda: con.execute(
                f"COPY (SELECT {_READ_MASKED_COLUMNS} FROM masked) "
                f"TO '{_sql_str(target)}' (FORMAT FASTQ, COMPRESSION 'gzip')"
            ),
            pairs,
        )
    else:
        raise ValueError(f"unsupported export format: {fmt!r}")


def _handle_masked_read_export(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Export every (non-retired) sample on a sequenced_pool's masked reads to
    per-sample files. system_admin only (admin:masked_read_export).

    GETs the roster manifest, then for each sample mints a just-in-time DoGet
    ticket and streams its read_masked rows from the data plane straight to disk
    (parquet via a pyarrow ParquetWriter; fastq.gz via one shared miint DuckDB
    connection reused across every sample) — so a large pool never buffers in
    memory or on an intermediate disk hop. Per-sample writes are atomic and 0600.

    Fails loudly (exit 1, nothing written) if any sample lacks a usable
    biosample_accession (missing — not yet NCBI-submitted — or outside the safe
    charset), since the filename requires it; validated up front so one odd
    sample can't leave a partial export.
    """
    import pyarrow.flight as flight  # noqa: PLC0415
    import pyarrow.ipc as ipc  # noqa: PLC0415

    output_dir: Path = args.output_dir
    if not output_dir.is_dir():
        print(f"error: output directory does not exist: {output_dir}", file=sys.stderr)
        return 2

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    manifest_path = (
        f"{PATH_ADMIN_PREFIX}"
        f"{PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT.format(sequenced_pool_idx=args.sequenced_pool_idx)}"
    )
    ticket_path = f"{PATH_ADMIN_PREFIX}{PATH_ADMIN_MASKED_READ_EXPORT_TICKET}"
    try:
        manifest = _common.call(
            "GET", args.base_url, token, manifest_path, params={"mask_idx": args.mask_idx}
        )
    except httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1

    samples = manifest["samples"]
    run_idx = manifest["sequencing_run_idx"]
    pool_idx = manifest["sequenced_pool_idx"]
    if not samples:
        print(
            f"no samples on sequenced_pool {pool_idx} for mask_idx {args.mask_idx}; "
            "nothing to export"
        )
        return 0

    # Validate every accession up front so an unsubmitted/odd sample fails the
    # whole export before any download — never a partial output set.
    bad = sorted(
        s["prep_sample_idx"]
        for s in samples
        if not s["biosample_accession"] or not _SAFE_ACCESSION.match(s["biosample_accession"])
    )
    if bad:
        print(
            f"error: {len(bad)} sample(s) on sequenced_pool {pool_idx} have no usable "
            f"biosample_accession (missing or outside [A-Za-z0-9._-]): prep_sample_idx {bad}. "
            "The export filename requires the accession — submit/repair these samples first.",
            file=sys.stderr,
        )
        return 1

    # Only the fastq path feeds Flight batches into DuckDB (the miint FORMAT FASTQ
    # writer), which routes a registered pyarrow reader through pyarrow.dataset →
    # Acero. Flight hands us each RecordBatch by zero-copying the gRPC message
    # body, whose absolute base address carries no element-alignment guarantee, so
    # a uint64/int32 column buffer routinely lands off its natural alignment even
    # though the data plane writes 64-byte-aligned IPC (arrow-rs default), and
    # Acero then logs a "poorly aligned input buffer" warning per misaligned column
    # per batch (apache/arrow#37195). Ask the Flight reader to realign each buffer
    # to its type's required alignment on receive (DataTypeSpecific copies only the
    # small offset/validity/fixed-width buffers, leaving the bulk sequence/quality
    # byte buffers zero-copy). The parquet path streams straight to a ParquetWriter
    # (no Acero), so it needs no realignment and keeps those bulk buffers zero-copy.
    read_opts = (
        flight.FlightCallOptions(
            read_options=ipc.IpcReadOptions(ensure_alignment=ipc.Alignment.DataTypeSpecific)
        )
        if args.format == "fastq"
        else None
    )
    # The fastq writer needs a miint DuckDB connection; open it once and reuse it
    # across all samples (each sample re-registers the `masked` view) rather than
    # paying a fresh connect + extension LOAD per sample. Parquet needs no DuckDB.
    con = connect_with_miint() if args.format == "fastq" else None
    flight_client = flight.FlightClient(args.data_plane_url)
    try:
        for s in samples:
            prep = s["prep_sample_idx"]
            ticket_resp = _common.call(
                "POST",
                args.base_url,
                token,
                ticket_path,
                json={"prep_sample_idx": prep, "mask_idx": args.mask_idx},
            )
            ticket_bytes = base64.b64decode(ticket_resp["ticket"])
            reader = flight_client.do_get(flight.Ticket(ticket_bytes), read_opts).to_reader()
            stem = f"{s['biosample_accession']}.{run_idx}.{pool_idx}.{prep}"
            _write_masked_sample(reader, stem, output_dir, args.format, con)
    except httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1
    finally:
        flight_client.close()
        if con is not None:
            con.close()

    print(f"exported {len(samples)} sample(s) from sequenced_pool {pool_idx} to {output_dir}")
    return 0


def _handle_login(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.do_login(
        base_url=args.base_url,
        token_file=args.token_file,
        cli_command="qiita-admin login",
    )


def _handle_actions_sync(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        result = asyncio.run(_sync_actions(database_url, args.workflows_dir))
    except (FileNotFoundError, DuplicateActionError, ValidationError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


def _handle_compute_readiness(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Subprocess into the orchestrator's venv to run the compute-readiness
    diagnostic. The orchestrator owns the actual checks (it has the
    Settings.from_env() + SlurmrestdClient surface); this wrapper is a
    thin pass-through so operators have a single `qiita-admin` UX
    surface for cluster-side problems too.

    Returns the subprocess's exit code verbatim so non-zero from any
    check failure propagates up through `qiita-admin` cleanly.
    """
    venv: Path = args.orchestrator_venv
    python = venv / "bin" / "python"
    if not python.exists():
        print(
            f"error: orchestrator python not found at {python}."
            " Pass --orchestrator-venv if the venv is installed elsewhere.",
            file=sys.stderr,
        )
        return 2
    cmd = [str(python), "-m", "qiita_compute_orchestrator.cli.compute_readiness"]
    if args.no_slurm_probe:
        cmd.append("--no-slurm-probe")
    if args.emit_json:
        cmd.append("--json")
    if args.probe_timeout_seconds is not None:
        cmd += ["--probe-timeout-seconds", str(args.probe_timeout_seconds)]
    return subprocess.call(cmd)


def _handle_ticket_force_fail(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        result = asyncio.run(
            _force_fail_ticket(
                database_url,
                work_ticket_idx=args.work_ticket_idx,
                stage=args.stage,
                step_name=args.step_name,
                reason=args.reason,
            )
        )
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


def _handle_work_ticket_backfill_mask_idx(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        report = asyncio.run(_backfill_mask_idx(database_url, apply=args.apply))
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    mode = "APPLIED" if report["applied"] else "DRY-RUN (no writes; pass --apply to commit)"
    counted = report["counted"]
    populated = report["populated"]
    skipped_no_mask = len(report["skipped_no_mask"])
    print(f"backfill-mask-idx [{mode}]")
    print(f"  counted (mask_idx IS NULL, mask-bearing actions): {counted}")
    print(f"  populated:           {populated}")
    print(f"  skipped (no matching mask): {skipped_no_mask}")
    print(f"  skipped (not prep_sample):  {len(report['skipped_not_prep_sample'])}")
    if report["skipped_no_mask"]:
        print(f"  skipped-no-mask ticket idxs: {report['skipped_no_mask']}")
    if report["skipped_not_prep_sample"]:
        print(f"  skipped-not-prep-sample ticket idxs: {report['skipped_not_prep_sample']}")
    # The backfill matches a ticket only when its re-derived mask params hash to an
    # already-minted mask. A serialization / config / adapter-writer drift between
    # this run and the original mint would make EVERY real ticket miss the lookup
    # and land in skipped_no_mask instead of populated — a silent no-op that looks
    # like success. Before trusting an --apply, verify populated > 0 and that
    # skipped_no_mask is the small residue you expect (tickets that genuinely
    # failed before minting), not the bulk of the candidates.
    if counted > 0 and populated == 0:
        print(
            "  WARNING: candidate tickets exist but NONE matched a mask — this"
            " likely indicates a hash-repro drift (serialization / adapter writer /"
            " config), not 'nothing to do'. Do NOT --apply until resolved."
        )
    elif not report["applied"]:
        print(
            "  Before running --apply: confirm populated > 0 and skipped_no_mask is"
            " expected-small; an unexpected all-skipped result means a hash-repro"
            " drift, not real work to skip."
        )
    return 0


def _handle_mask_delete(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if args.mask_idx <= 0:
        print("error: mask_idx must be a positive integer", file=sys.stderr)
        return 2

    def _render(body: dict | list) -> None:
        # body is the MaskDefinitionDeleteResponse dict.
        print(json.dumps(body, indent=2))
        if isinstance(body, dict) and "rows_deleted" in body:
            print(
                f"deleted mask_idx={body.get('mask_idx')}:"
                f" {body['rows_deleted']} read_mask row(s) removed.",
                file=sys.stderr,
            )

    return _common.run_http_subcommand(
        lambda t: _mask_delete_via_route(args.base_url, t, args.mask_idx),
        render=_render,
    )


def _handle_mask_purge_failed(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.action == "all":
        action_ids = _PURGE_FAILED_ACTION_IDS
    else:
        action_ids = (args.action,)

    try:
        report = asyncio.run(
            _purge_failed(
                database_url,
                args.base_url,
                token,
                action_ids=action_ids,
                execute=args.execute,
                with_tickets=args.with_tickets,
                limit=args.limit,
                rate_seconds=args.rate_seconds,
                wait=args.wait,
            )
        )
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1

    mode = "EXECUTED" if report["executed"] else "DRY-RUN (no writes; pass --execute to commit)"
    print(f"mask purge-failed [{mode}]")
    print(f"  actions:    {report['action_ids']}")
    if report["backfill_incomplete"]:
        # Prominent banner so the operator sees this BEFORE attempting --execute
        # (which refuses outright while backfill is incomplete).
        print(
            f"  *** BACKFILL INCOMPLETE: {report['backfill_incomplete']} non-failed"
            f" work_ticket(s) for {report['action_ids']} have mask_idx IS NULL."
        )
        print(
            "      The shared-mask guard cannot see them; a shared mask could be wrongly deleted."
        )
        print(
            "      Run `qiita-admin work-ticket backfill-mask-idx --apply` first."
            " --execute will REFUSE until this is 0."
        )
    print(f"  candidates: {report['candidates']}")
    print(f"  eligible:   {len(report['eligible'])}")
    for e in report["eligible"]:
        print(
            f"    would purge+resubmit: work_ticket_idx={e['work_ticket_idx']}"
            f" mask_idx={e['mask_idx']} prep_sample_idx={e['prep_sample_idx']}"
        )
    if report["skipped_shared"]:
        print(f"  skipped (shared mask — NOT deleted): {len(report['skipped_shared'])}")
        for s in report["skipped_shared"]:
            print(
                f"    SKIP work_ticket_idx={s['work_ticket_idx']} mask_idx={s['mask_idx']}"
                f" — referenced by non-failed tickets {s['non_failed_work_ticket_idxs']}"
            )
    if report["skipped_no_mask_idx"]:
        print(
            f"  skipped (mask_idx IS NULL — run backfill-mask-idx first):"
            f" {report['skipped_no_mask_idx']}"
        )
    if report["skipped_wrong_kind"]:
        print(f"  skipped (not prep_sample-scoped): {report['skipped_wrong_kind']}")

    if report["executed"]:
        print(f"  purged masks:  {len(report['purged'])}")
        for p in report["purged"]:
            print(
                f"    purged mask_idx={p['mask_idx']} (rows_deleted={p['rows_deleted']})"
                f" for original work_ticket_idx={p['work_ticket_idx']}"
            )
        print(f"  resubmitted:   {len(report['resubmitted'])}")
        for r in report["resubmitted"]:
            if "observed_state" in r:
                marker = " (TIMED OUT)" if r.get("timed_out") else ""
                tail = f" observed_state={r['observed_state']}{marker}"
            else:
                tail = ""
            print(
                f"    original={r['original_work_ticket_idx']} ->"
                f" new={r['new_work_ticket_idx']} state={r['state']}{tail}"
            )
        if report["failures"]:
            print(f"  FAILURES (isolated; batch continued): {len(report['failures'])}")
            for f in report["failures"]:
                print(
                    f"    FAIL work_ticket_idx={f['work_ticket_idx']}"
                    f" mask_idx={f['mask_idx']}"
                    f" (mask_deleted={f['mask_deleted']} ticket_deleted={f['ticket_deleted']}):"
                    f" {f['error']}"
                )
                # Replay hint: with the mask already deleted, a plain re-POST of
                # this body is safe (no duplicate read_mask rows).
                print(f"      replay POST /work-ticket: {json.dumps(f['resubmit_body'])}")
            # A non-empty failures list is an operator-actionable signal.
            return 1
    else:
        # Mirror the backfill command's "verify before you commit" caveat.
        print(
            "  Before running --execute: eyeball the eligible list above and"
            " confirm the skipped-shared masks are genuinely shared (a non-failed"
            " ticket really depends on them). --execute purges each listed mask"
            " and resubmits a fresh ticket; nothing is written in this dry-run."
        )
        print(
            "  Recovery: if a resubmit fails mid-batch it is reported with its"
            " resubmit_body; the mask is already deleted by then, so a plain"
            " re-POST of that body to POST /work-ticket is safe (no duplicate"
            " read_mask rows)."
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _common.validate_base_url(args, parser)
    return args.handler(args, parser)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
