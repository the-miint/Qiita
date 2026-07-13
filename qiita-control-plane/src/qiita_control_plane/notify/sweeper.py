"""In-process notify sweeper — best-effort time-window digest.

A terminal work_ticket with `notified_at IS NULL` is the "email owed" signal.
Once per `NOTIFY_SWEEP_INTERVAL_SECONDS` this sweeper:

1. takes a session-level advisory lock on its own dedicated single-connection
   pool (so a second CP process can never double-send — nothing enforces
   single-process today — and a slow relay can't starve the request pool);
2. SELECTs the owed set (byte-matching the partial index predicate, incl. the
   `failure_type IS DISTINCT FROM 'retriable'` carve-out), capped at
   `NOTIFY_MAX_ROWS_PER_SWEEP` so a backlog can't pin the lock unboundedly;
3. groups by originator and, per group, decides `flush_now` via a
   trailing-debounce with a max-wait cap (so a never-quiescing fanout still
   flushes and forward progress is guaranteed);
4. drains stale rows (older than `NOTIFY_MAX_AGE_SECONDS`) without emailing,
   gates on `qiita.user.receive_processing_emails`, dead-letters after
   `NOTIFY_MAX_ATTEMPTS`, else counts the originator's still-active and
   held-for-redrive tickets, renders one digest, writes a receipt, sends, and
   stamps the EXACT captured id set (send-then-stamp = at-least-once).

Between them those three buckets account for every ticket the recipient has —
what just finished (the owed set), what is still coming (`_ACTIVE_COUNT_SELECT`),
and what is stuck (`_HELD_COUNT_SELECT`). A digest that reported only the first
left a recipient mid-fanout unable to tell "2 failed, 24 still running" from
"2 failed, and that's the whole batch".

Every step is wrapped so one bad row / recipient can't wedge the long-lived
loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from qiita_common.models import EmailReceiptStatus

from ..dispatch import NON_TERMINAL_WORK_TICKET_STATES
from ..runner import _TERMINAL_WORK_TICKET_STATES
from .render import (
    WORK_TICKET_DIGEST_TEMPLATE,
    render_work_ticket_digest,
    summarize_active,
    template_sha,
)

if TYPE_CHECKING:
    import asyncpg

    from ..config import Settings
    from .transport import Transport

_log = logging.getLogger(__name__)

# Arbitrary fixed application-wide key for pg_try_advisory_lock. Only the notify
# sweeper uses it; a second CP process running the sweeper concurrently fails to
# acquire it and skips its pass, so a digest is never double-sent.
_NOTIFY_SWEEP_LOCK_KEY = 4_310_290_147

# Terminal-state literals shared by the owed-set SELECT and the partial index
# (qiita_work_ticket_email_owed_idx). Built from the runner's frozenset — the
# single source of truth — in sorted order so the SELECT predicate byte-matches
# the migration's index predicate and the planner can use it. A parity test
# pins the three sites together.
_TERMINAL_STATE_LITERALS = tuple(sorted(_TERMINAL_WORK_TICKET_STATES))
_TERMINAL_STATE_SQL = ", ".join(f"'{s}'" for s in _TERMINAL_STATE_LITERALS)

# The owed-set predicate. MUST byte-match the partial index predicate.
_OWED_SET_WHERE = (
    "notified_at IS NULL"
    f" AND state IN ({_TERMINAL_STATE_SQL})"
    " AND failure_type IS DISTINCT FROM 'retriable'"
)

_OWED_SET_SELECT = (
    "SELECT work_ticket_idx, originator_principal_idx, action_id, action_version,"
    "       state, failure_reason, updated_at, notify_attempts"
    "  FROM qiita.work_ticket"
    f" WHERE {_OWED_SET_WHERE}"
    " ORDER BY originator_principal_idx, updated_at"
)

# One originator's still-in-flight tickets, tallied per (action, state). Without
# this the digest says only what terminalized, and a recipient mid-fanout can't
# tell "2 failed, 24 still running" from "2 failed, and that's the batch". The
# active set is `NON_TERMINAL_WORK_TICKET_STATES` — the SAME predicate the
# `GET /work-ticket?active=true` route filters on, so the email answers exactly
# the question the operator would otherwise go run `qiita ticket list --active`
# to answer.
#
# Scoped to the originator, not the ticket's action or scope target: the
# recipient's question is "where am I in *my* queue", and a digest is already
# per-originator.
#
# Unlike the owed-set SELECT this has no dedicated partial index — it rides
# `work_ticket_originator_idx` plus a heap filter, so it costs an originator's
# LIFETIME ticket count for an answer that stays tiny. Fine at our size (a few
# thousand tickets, one query per digest, on the sweeper's own connection); if
# an originator's history ever makes it hurt, a partial index on
# (originator_principal_idx) WHERE state IN (<non-terminal>) serves this and the
# `?active=true` route both — and would want the owed set's inline-literal
# treatment so the planner can prove implication.
_ACTIVE_COUNT_SELECT = (
    "SELECT action_id, action_version, state, count(*) AS n"
    "  FROM qiita.work_ticket"
    " WHERE originator_principal_idx = $1"
    "   AND state = ANY($2::qiita.work_ticket_state[])"
    " GROUP BY action_id, action_version, state"
)

# The third bucket: FAILED-with-retriable, which the owed set carves out (it is
# withheld from email so that a redrive-and-complete reports the TRUE outcome).
# Terminal, so it is not in the active set either — which means without this it
# would appear in neither half of the digest, and the "nothing still active"
# line would tell a recipient everything is accounted for while N of their
# tickets sit dead on infra waiting for someone to redrive them. Exactly the
# blind spot the digest exists to close. `notified_at IS NULL` is what makes it
# the complement of the owed set's carve-out rather than all-time history.
_HELD_COUNT_SELECT = (
    "SELECT count(*)"
    "  FROM qiita.work_ticket"
    " WHERE originator_principal_idx = $1"
    "   AND notified_at IS NULL"
    "   AND state = 'failed'"
    "   AND failure_type = 'retriable'"
)


@dataclass(slots=True)
class SweepResult:
    """Per-pass tally, for the NoOp-visibility log and tests."""

    acquired: bool = False
    owed_rows: int = 0
    originators: int = 0
    digests_sent: int = 0
    stale_drained: int = 0
    gated_out: int = 0
    dead_lettered: int = 0
    send_failures: int = 0


async def _insert_receipt(
    conn: asyncpg.Connection,
    *,
    template_name: str,
    template_context: dict[str, Any],
    recipient_email: str,
    recipient_principal_idx: int,
    subject: str,
    body_text: str,
    body_html: str | None,
    status: str,
    transport: str,
    template_sha_value: str,
    attempts: int = 0,
    error: str | None = None,
    provider_message_id: str | None = None,
) -> int:
    return await conn.fetchval(
        "INSERT INTO qiita.email_receipt"
        " (template_name, template_context, recipient_email, recipient_principal_idx,"
        "  subject, body_text, body_html, status, transport, template_sha,"
        "  attempts, error, provider_message_id)"
        " VALUES ($1, $2::jsonb, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
        " RETURNING idx",
        template_name,
        json.dumps(template_context),
        recipient_email,
        recipient_principal_idx,
        subject,
        body_text,
        body_html,
        status,
        transport,
        template_sha_value,
        attempts,
        error,
        provider_message_id,
    )


async def _stamp_notified(conn: asyncpg.Connection, ids: list[int]) -> None:
    """Stamp notified_at on the EXACT captured id set — never a predicate
    re-scan, so a sibling that terminalized during the send window is not
    silently swept up.

    Re-asserting the full owed-set predicate (not just `notified_at IS NULL`) is
    what makes the stamp idempotent AND redrive-safe: `POST /work-ticket/{idx}/run`
    resets `notified_at` to NULL precisely so a redriven ticket re-notifies at its
    true terminal state, and a redrive landing inside our send window would
    otherwise be stamped away here — the ticket would go out reported as `failed`
    and then never be emailed again. Under the predicate it no longer matches
    (it's back to `pending`), so we leave it owed.
    """
    await conn.execute(
        "UPDATE qiita.work_ticket SET notified_at = now()"
        f" WHERE work_ticket_idx = ANY($1::bigint[]) AND {_OWED_SET_WHERE}",
        ids,
    )


async def _active_rows(conn: asyncpg.Connection, originator: int) -> list[dict[str, Any]]:
    """The originator's still-active tickets, tallied per (action, state).

    A snapshot, deliberately: it is read after the owed set, so a ticket that
    terminalizes in between is in neither this count nor this digest — it lands
    in the next one. The number is a "where am I" signal, not a ledger.
    """
    rows = await conn.fetch(_ACTIVE_COUNT_SELECT, originator, list(NON_TERMINAL_WORK_TICKET_STATES))
    return [dict(r) for r in rows]


async def _held_count(conn: asyncpg.Connection, originator: int) -> int:
    """How many of the originator's tickets are parked in retriable-FAILED —
    withheld from email, terminal, and therefore in neither of the other two
    buckets. See `_HELD_COUNT_SELECT`."""
    return await conn.fetchval(_HELD_COUNT_SELECT, originator)


def _digest_context(
    fresh_ids: list[int],
    tickets: list[dict[str, Any]],
    active: dict[str, Any],
    held_total: int,
) -> dict[str, Any]:
    """The receipt's template_context. `work_ticket_idxs` is the top-level key a
    `@>` containment query keys off ("did we email about ticket Y?").

    `active` is the rollup `render_work_ticket_digest` rendered from, not a
    re-tally of the same rows: the receipt is the evidence trail, so it must
    record the claim the email actually MADE — a second, parallel rollup here
    could drift from it.
    """
    counts: dict[str, int] = defaultdict(int)
    for t in tickets:
        counts[t["state"]] += 1
    return {
        "work_ticket_idxs": fresh_ids,
        "counts": dict(counts),
        "active_total": active["total"],
        "active_counts": active["by_state"],
        "active_actions": active["actions"],
        "held_total": held_total,
    }


async def _process_group(
    conn: asyncpg.Connection,
    settings: Settings,
    transport: Transport,
    originator: int,
    rows: list[asyncpg.Record],
    *,
    now: datetime,
    result: SweepResult,
) -> None:
    updated_ats = [r["updated_at"] for r in rows]
    quiesced = (now - max(updated_ats)).total_seconds() >= settings.notify_quiet_period_seconds
    max_waited = (now - min(updated_ats)).total_seconds() >= settings.notify_max_batch_seconds
    if not (quiesced or max_waited):
        # Group still settling and under the max-wait cap → wait for a quieter
        # pass.
        return

    stale_cutoff = now - timedelta(seconds=settings.notify_max_age_seconds)
    stale = [r for r in rows if r["updated_at"] < stale_cutoff]
    fresh = [r for r in rows if r["updated_at"] >= stale_cutoff]

    if stale:
        stale_ids = [r["work_ticket_idx"] for r in stale]
        await _stamp_notified(conn, stale_ids)
        result.stale_drained += len(stale_ids)
        _log.info(
            "notify sweep: drained %d stale ticket(s) for originator %d without emailing",
            len(stale_ids),
            originator,
        )

    if not fresh:
        return

    fresh_ids = [r["work_ticket_idx"] for r in fresh]

    # Gate: no user row (service principal) or opt-out → stamp without emailing
    # so the rows aren't reconsidered every pass.
    user = await conn.fetchrow(
        "SELECT email, receive_processing_emails FROM qiita.user WHERE principal_idx = $1",
        originator,
    )
    if user is None or not user["receive_processing_emails"]:
        await _stamp_notified(conn, fresh_ids)
        result.gated_out += len(fresh_ids)
        return

    tickets = [
        {
            "idx": r["work_ticket_idx"],
            "action_id": r["action_id"],
            "action_version": r["action_version"],
            "state": r["state"],
            "failure_reason": r["failure_reason"],
        }
        for r in fresh
    ]
    active_rows = await _active_rows(conn, originator)
    held_total = await _held_count(conn, originator)
    rendered = render_work_ticket_digest(
        recipient=user["email"],
        tickets=tickets,
        generated_at=now,
        active_rows=active_rows,
        held_total=held_total,
        contact_email=settings.contact_email,
    )
    context = _digest_context(fresh_ids, tickets, summarize_active(active_rows), held_total)
    sha = template_sha(WORK_TICKET_DIGEST_TEMPLATE)

    # Dead-letter cap: give up after NOTIFY_MAX_ATTEMPTS failed sends. Write a
    # dead_letter receipt (evidence), stamp, stop retrying.
    #
    # Accepted behavior: the cap is gated on max(notify_attempts) across the
    # whole fresh group and then dead-letters/stamps ALL fresh ids together, so
    # a brand-new ticket that joins a chronically-failing originator's group can
    # be dead-lettered on its first sweep. This is deliberate — a persistently
    # failing recipient means we give up on that originator's entire current
    # batch rather than let one healthy new ticket keep the group retrying.
    if max(r["notify_attempts"] for r in fresh) >= settings.notify_max_attempts:
        await _insert_receipt(
            conn,
            template_name=WORK_TICKET_DIGEST_TEMPLATE,
            template_context=context,
            recipient_email=user["email"],
            recipient_principal_idx=originator,
            subject=rendered.subject,
            body_text=rendered.text,
            body_html=rendered.html or None,
            status=EmailReceiptStatus.DEAD_LETTER,
            transport=transport.name,
            template_sha_value=sha,
            attempts=settings.notify_max_attempts,
            error=f"gave up after {settings.notify_max_attempts} failed send attempts",
        )
        await _stamp_notified(conn, fresh_ids)
        result.dead_lettered += 1
        _log.warning(
            "notify sweep: dead-lettered digest for originator %d after %d attempts",
            originator,
            settings.notify_max_attempts,
        )
        return

    receipt_idx = await _insert_receipt(
        conn,
        template_name=WORK_TICKET_DIGEST_TEMPLATE,
        template_context=context,
        recipient_email=user["email"],
        recipient_principal_idx=originator,
        subject=rendered.subject,
        body_text=rendered.text,
        body_html=rendered.html or None,
        status=EmailReceiptStatus.PENDING,
        transport=transport.name,
        template_sha_value=sha,
    )

    try:
        message_id = await transport.send(to=user["email"], rendered=rendered)
    except Exception as exc:
        await conn.execute(
            "UPDATE qiita.email_receipt"
            " SET status = $3, error = $2, attempts = attempts + 1"
            " WHERE idx = $1",
            receipt_idx,
            f"{type(exc).__name__}: {exc!s}"[:2000],
            EmailReceiptStatus.FAILED,
        )
        # Leave notified_at NULL → retried next pass; bump per-ticket counter
        # toward the dead-letter cap. Send-then-stamp = at-least-once.
        await conn.execute(
            "UPDATE qiita.work_ticket SET notify_attempts = notify_attempts + 1"
            " WHERE work_ticket_idx = ANY($1::bigint[])",
            fresh_ids,
        )
        result.send_failures += 1
        _log.exception("notify sweep: send failed for originator %d", originator)
        return

    await conn.execute(
        "UPDATE qiita.email_receipt"
        " SET status = $3, sent_at = now(), provider_message_id = $2,"
        "     attempts = attempts + 1"
        " WHERE idx = $1",
        receipt_idx,
        message_id,
        EmailReceiptStatus.SENT,
    )
    await _stamp_notified(conn, fresh_ids)
    result.digests_sent += 1


async def sweep_once(
    pool: asyncpg.Pool,
    settings: Settings,
    transport: Transport,
    *,
    now: datetime | None = None,
) -> SweepResult:
    """Run one sweep pass. `now` is injectable for tests."""
    now = now or datetime.now(UTC)
    result = SweepResult()
    # The session advisory lock is held on one connection across all rendering
    # and every transport.send() for the whole pass, so a slow relay can pin that
    # connection for up to N × SMTP_TIMEOUT_SECONDS. That is intentional —
    # serializing to a single sender is the goal and correctness is unaffected.
    # `pool` is the sweeper's OWN dedicated single-connection pool (built in
    # main.py's lifespan), so this long hold never starves the request pool; the
    # sweeper is a serial loop and never needs a second connection.
    async with pool.acquire() as conn:
        acquired = await conn.fetchval("SELECT pg_try_advisory_lock($1)", _NOTIFY_SWEEP_LOCK_KEY)
        if not acquired:
            _log.debug("notify sweep: advisory lock held elsewhere; skipping pass")
            return result
        result.acquired = True
        try:
            limit = settings.notify_max_rows_per_sweep
            rows = await conn.fetch(f"{_OWED_SET_SELECT} LIMIT $1", limit)
            result.owed_rows = len(rows)
            groups: dict[int, list[asyncpg.Record]] = defaultdict(list)
            for row in rows:
                groups[row["originator_principal_idx"]].append(row)
            result.originators = len(groups)
            # If the pass filled the row cap, the LAST originator in the ORDER BY
            # may be truncated mid-group (its newer tickets didn't fit) — and a
            # partial group can flush prematurely (a smaller max(updated_at) reads
            # as quiesced). Defer that one originator to a later pass, when it
            # fits whole. Progress still holds: it's strictly after every group we
            # keep, so draining the earlier groups shrinks the owed set until it
            # fits. The exception is a single originator whose own backlog exceeds
            # the cap (groups=={that one}); we process it truncated so a giant
            # solo backlog still drains in chunks instead of stalling forever.
            if len(rows) >= limit and len(groups) > 1:
                del groups[rows[-1]["originator_principal_idx"]]
                result.originators = len(groups)
            for originator, group_rows in groups.items():
                try:
                    await _process_group(
                        conn,
                        settings,
                        transport,
                        originator,
                        group_rows,
                        now=now,
                        result=result,
                    )
                except Exception:
                    # One bad recipient/group must not wedge the rest of the pass.
                    _log.exception("notify sweep: originator %d group failed", originator)
        finally:
            await conn.fetchval("SELECT pg_advisory_unlock($1)", _NOTIFY_SWEEP_LOCK_KEY)

    if transport.name == "noop" and result.digests_sent:
        _log.info(
            "notify sweep (NoOpTransport): would have sent %d digest(s) to %d originator(s)",
            result.digests_sent,
            result.originators,
        )
    return result


async def run_sweeper(
    pool: asyncpg.Pool,
    settings: Settings,
    transport: Transport,
) -> None:
    """Long-lived loop: one `sweep_once` per NOTIFY_SWEEP_INTERVAL_SECONDS.

    A bad pass logs and continues — the loop must outlive any single failure.
    Cancelled at shutdown (the CancelledError propagates out to end the task)."""
    _log.info(
        "notify sweeper started (transport=%s, interval=%ds)",
        transport.name,
        settings.notify_sweep_interval_seconds,
    )
    while True:
        try:
            await sweep_once(pool, settings, transport)
        except asyncio.CancelledError:
            raise
        except Exception:
            _log.exception("notify sweep pass failed; continuing")
        await asyncio.sleep(settings.notify_sweep_interval_seconds)
