"""qiita-admin CLI — ticket force-fail subcommand (direct DB).

Split out of the former single-file ``cli.admin`` module; behavior unchanged.
"""

import argparse
import asyncio
import json
import os
import sys

import asyncpg
from qiita_common.models import NON_TERMINAL_WORK_TICKET_STATES

from ._helpers import _DB_CONNECT_TIMEOUT_SECONDS

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
# (completed / no_data / failed) is rejected so the CLI doesn't silently
# overwrite a captured failure or convert a real outcome into a fake failure.
_FORCE_FAIL_ELIGIBLE_STATES = NON_TERMINAL_WORK_TICKET_STATES


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
