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

For the subcommand list and per-flag details, run `qiita-admin --help` (or
`qiita-admin <subcommand> --help`) — the argparse help is the ground truth, so
it can't drift from the actual commands the way a hand-maintained list would.

Authentication for HTTP subcommands: read PAT from QIITA_TOKEN env var or
from ~/.qiita/token (mode 0600 expected). Loopback login flow, token I/O,
and the generic HTTP runner live in `cli._common`.
"""

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import asyncpg
import httpx

from .. import _common
from ._helpers import _DB_CONNECT_TIMEOUT_SECONDS
from .actions_sync import _handle_actions_sync, _sync_actions
from .auth import _handle_login, _handle_token_revoke_all, _handle_whoami, _token_revoke_all
from .compute_readiness import _DEFAULT_ORCHESTRATOR_VENV, _handle_compute_readiness
from .force_fail import (
    _FAILURE_STAGE_CHOICES,
    _FAILURE_STAGES_REJECTING_STEP_NAME,
    _FAILURE_STAGES_REQUIRING_STEP_NAME,
    _FORCE_FAIL_ELIGIBLE_STATES,
    _force_fail_ticket,
    _handle_ticket_force_fail,
    _validate_force_fail_args,
)
from .mask import (
    _PURGE_FAILED_ACTION_IDS,
    _READ_MASK_PARQUET_NOT_FOUND,
    _RESUBMITTABLE_SCOPE_KIND,
    _TERMINAL_TICKET_STATES,
    _WAIT_POLL_INTERVAL_SECONDS,
    _WAIT_TIMEOUT_SECONDS,
    _build_resubmit_body,
    _count_non_failed_missing_mask_idx,
    _handle_mask_delete,
    _mask_delete_via_route,
    _mask_shared_with_non_failed,
    _poll_ticket_to_terminal,
    _resubmit_work_ticket,
    _select_purge_failed_candidates,
)
from .masked_export import (
    _READ_MASKED_COLUMNS,
    _SAFE_ACCESSION,
    _commit_partials,
    _count_masked,
    _export_stem,
    _handle_masked_read_export,
    _parquet_row_count,
    _peek_paired,
    _sql_str,
    _write_masked_sample,
)
from .owner_id import (
    _OWNER_ID_BASE_COLUMNS,
    _OWNER_ID_POOL_COLUMNS,
    _handle_owner_biosample_id,
    _write_owner_biosample_id_tsv,
)
from .role import _VALID_ROLE_VALUES, _handle_set_system_role, _set_system_role

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
        help=(
            "Directory to write per-sample files into (created, with parents, if"
            " missing; files are mode 0600). On re-export, an existing parquet"
            " sample is skipped when its count matches the data plane and"
            " overwritten when it differs; an existing fastq target is refused."
        ),
    )
    p_export.add_argument(
        "--data-plane-url",
        required=True,
        dest="data_plane_url",
        help=(
            "gRPC URL of the data plane. From off the deploy host use the public "
            "TLS edge (e.g. grpc+tls://qiita.example.com:443); grpc://<host>:50051 "
            "is the direct/on-host form and is not reachable off-host."
        ),
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
            f" Default: {_DEFAULT_ORCHESTRATOR_VENV} (set $QIITA_ORCHESTRATOR_VENV to"
            " change the default without this flag)"
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
                " These tickets predate mask_idx tracking and should have been"
                " populated at migration time; investigate and set their mask_idx"
                " before re-running this command."
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
            "      These tickets predate mask_idx tracking; populate their mask_idx"
            " before proceeding. --execute will REFUSE until this is 0."
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
            f"  skipped (mask_idx IS NULL — predates mask tracking):"
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
            # This branch exits nonzero, so its operator-actionable failure
            # lines go to stderr (distinct from the run report on stdout).
            print(
                f"  FAILURES (isolated; batch continued): {len(report['failures'])}",
                file=sys.stderr,
            )
            for f in report["failures"]:
                print(
                    f"    FAIL work_ticket_idx={f['work_ticket_idx']}"
                    f" mask_idx={f['mask_idx']}"
                    f" (mask_deleted={f['mask_deleted']} ticket_deleted={f['ticket_deleted']}):"
                    f" {f['error']}",
                    file=sys.stderr,
                )
                # Replay hint: with the mask already deleted, a plain re-POST of
                # this body is safe (no duplicate read_mask rows).
                print(
                    f"      replay POST /work-ticket: {json.dumps(f['resubmit_body'])}",
                    file=sys.stderr,
                )
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


__all__ = [
    "_DB_CONNECT_TIMEOUT_SECONDS",
    "_DEFAULT_ORCHESTRATOR_VENV",
    "_FAILURE_STAGES_REJECTING_STEP_NAME",
    "_FAILURE_STAGES_REQUIRING_STEP_NAME",
    "_FAILURE_STAGE_CHOICES",
    "_FORCE_FAIL_ELIGIBLE_STATES",
    "_OWNER_ID_BASE_COLUMNS",
    "_OWNER_ID_POOL_COLUMNS",
    "_PURGE_FAILED_ACTION_IDS",
    "_READ_MASKED_COLUMNS",
    "_READ_MASK_PARQUET_NOT_FOUND",
    "_RESUBMITTABLE_SCOPE_KIND",
    "_SAFE_ACCESSION",
    "_TERMINAL_TICKET_STATES",
    "_VALID_ROLE_VALUES",
    "_WAIT_POLL_INTERVAL_SECONDS",
    "_WAIT_TIMEOUT_SECONDS",
    "_build_parser",
    "_build_resubmit_body",
    "_commit_partials",
    "_count_masked",
    "_count_non_failed_missing_mask_idx",
    "_export_stem",
    "_force_fail_ticket",
    "_handle_actions_sync",
    "_handle_compute_readiness",
    "_handle_login",
    "_handle_mask_delete",
    "_handle_mask_purge_failed",
    "_handle_masked_read_export",
    "_handle_owner_biosample_id",
    "_handle_set_system_role",
    "_handle_ticket_force_fail",
    "_handle_token_revoke_all",
    "_handle_whoami",
    "_mask_delete_via_route",
    "_mask_shared_with_non_failed",
    "_parquet_row_count",
    "_peek_paired",
    "_poll_ticket_to_terminal",
    "_purge_failed",
    "_resubmit_work_ticket",
    "_select_purge_failed_candidates",
    "_set_system_role",
    "_sql_str",
    "_sync_actions",
    "_token_revoke_all",
    "_validate_force_fail_args",
    "_write_masked_sample",
    "_write_owner_biosample_id_tsv",
    "main",
]
