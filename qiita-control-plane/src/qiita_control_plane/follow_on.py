"""Submit a completed ticket's follow-on (`qiita.work_ticket.on_success`).

`submit_follow_on` runs from dispatch's completion hook after every terminal
outcome and is a no-op unless the ticket COMPLETED with an `on_success` that has
no outcome yet. It submits the follow-on through `submit_work_ticket_core` as the
originator, on the parent's scope target, and the outcome lands on the parent:

  * created: `follow_on_work_ticket_idx`, written by the core in the same
    transaction as the new ticket's INSERT and only while the parent records no
    outcome — so a follow-on is created at most once however many callers race,
    and a created one is never unrecorded;
  * refused: `follow_on_error`, for a definitive refusal only (a 4xx from the
    gates, or an originator who can no longer submit);
  * anything else (a 5xx, a database error): nothing is recorded, and the next
    startup's `reconcile_follow_ons` submits it again.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException
from qiita_common.models import WorkTicketState

from .auth.principal import PrincipalUnusableError, load_human_user

if TYPE_CHECKING:
    import asyncpg
    from fastapi import FastAPI

_log = logging.getLogger(__name__)


async def _record_refusal(pool: asyncpg.Pool, work_ticket_idx: int, error: str) -> None:
    # Conditional, like the core's success write: whichever outcome lands first
    # stands (the one-outcome CHECK would refuse a second).
    await pool.execute(
        "UPDATE qiita.work_ticket SET follow_on_error = $2"
        " WHERE work_ticket_idx = $1"
        "   AND follow_on_work_ticket_idx IS NULL AND follow_on_error IS NULL",
        work_ticket_idx,
        error,
    )


def _describe_refusal(exc: HTTPException) -> str:
    detail: Any = exc.detail
    text = detail if isinstance(detail, str) else json.dumps(detail, sort_keys=True)
    return f"HTTP {exc.status_code}: {text}"


async def submit_follow_on(app: FastAPI, work_ticket_idx: int) -> None:
    """Submit `work_ticket_idx`'s follow-on if it COMPLETED with one that has no
    outcome yet. A failure that is not a refusal propagates, recording nothing."""
    # Imported here: routes.work_ticket imports dispatch, which imports this module.
    from .routes.work_ticket import (
        FollowOnAlreadyRecordedError,
        fetch_work_ticket,
        submit_work_ticket_core,
    )

    pool: asyncpg.Pool = app.state.pool
    parent = await fetch_work_ticket(pool, work_ticket_idx)
    if (
        parent is None
        or parent.state != WorkTicketState.COMPLETED
        or parent.on_success is None
        or parent.follow_on_work_ticket_idx is not None
        or parent.follow_on_error is not None
    ):
        return
    try:
        principal = await load_human_user(pool, parent.originator_principal_idx)
        created = await submit_work_ticket_core(
            app=app,
            principal=principal,
            body=parent.on_success.as_request(parent.scope_target),
            follow_on_of=work_ticket_idx,
        )
    except FollowOnAlreadyRecordedError:
        return  # another caller recorded the outcome first
    except PrincipalUnusableError as exc:
        await _record_refusal(
            pool, work_ticket_idx, f"the submitting user can no longer submit: {exc}"
        )
        return
    except HTTPException as exc:
        if exc.status_code >= 500:
            raise
        await _record_refusal(pool, work_ticket_idx, _describe_refusal(exc))
        return
    _log.info(
        "work_ticket %d completed; submitted follow-on work_ticket %d",
        work_ticket_idx,
        created.work_ticket_idx,
    )


async def reconcile_follow_ons(app: FastAPI) -> None:
    """Submit every completed ticket's follow-on that has no outcome yet: one that
    completed with no hook to see it, or whose submission failed transiently.

    Runs at startup after `reconcile_inflight_tickets`, so a follow-on created
    here is dispatched only by its own submission."""
    pool: asyncpg.Pool = app.state.pool
    rows = await pool.fetch(
        "SELECT work_ticket_idx FROM qiita.work_ticket"
        " WHERE state = $1::qiita.work_ticket_state AND on_success IS NOT NULL"
        "   AND follow_on_work_ticket_idx IS NULL AND follow_on_error IS NULL"
        " ORDER BY work_ticket_idx",
        WorkTicketState.COMPLETED.value,
    )
    for row in rows:
        try:
            await submit_follow_on(app, row["work_ticket_idx"])
        except Exception:
            _log.exception(
                "follow-on of work_ticket %d was not submitted; the next startup retries it",
                row["work_ticket_idx"],
            )
