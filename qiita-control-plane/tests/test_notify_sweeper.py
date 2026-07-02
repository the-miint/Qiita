"""DB tests for the notify sweeper (trailing-debounce digest).

These drive `sweep_once` directly against a real Postgres with an injected
`now`, a `CaptureTransport`, and a `SimpleNamespace` settings stub. Ticket
`updated_at` values are set to `now() + offset` (the set_updated_at trigger only
respects strictly-later timestamps), so tests advance the injected `now` past
them to simulate quiescence / max-wait / staleness rather than fabricating past
timestamps.
"""

import json
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from qiita_control_plane.notify import sweep_once
from qiita_control_plane.notify.transport import CaptureTransport, RenderedEmail
from qiita_control_plane.testing.db_seeds import seed_service_principal, seed_user_principal

pytestmark = pytest.mark.db


def _settings(*, quiet=180, max_batch=900, max_age=21600, max_attempts=5):
    return SimpleNamespace(
        notify_quiet_period_seconds=quiet,
        notify_max_batch_seconds=max_batch,
        notify_max_age_seconds=max_age,
        notify_max_attempts=max_attempts,
    )


@dataclass
class _Env:
    pool: asyncpg.Pool
    action_id: str
    version: str
    ref_idx: int
    principals: list[int]

    async def user(self, *, receive=True) -> int:
        pidx = await seed_user_principal(self.pool, prefix="notify", suffix=uuid4().hex[:8])
        self.principals.append(pidx)
        if not receive:
            await self.pool.execute(
                "UPDATE qiita.user SET receive_processing_emails = false WHERE principal_idx = $1",
                pidx,
            )
        return pidx

    async def service(self) -> int:
        pidx = await seed_service_principal(self.pool, prefix="notify-svc", suffix=uuid4().hex[:8])
        self.principals.append(pidx)
        return pidx

    async def email_of(self, pidx: int) -> str:
        return await self.pool.fetchval(
            "SELECT email FROM qiita.user WHERE principal_idx = $1", pidx
        )

    async def ticket(
        self, *, originator: int, state: str = "completed", failure_type: str | None = None
    ) -> int:
        if state == "failed":
            return await self.pool.fetchval(
                "INSERT INTO qiita.work_ticket"
                " (action_id, action_version, originator_principal_idx, scope_target_kind,"
                "  reference_idx, state, failure_type, failure_stage, failure_reason)"
                " VALUES ($1, $2, $3, 'reference', $4, 'failed'::qiita.work_ticket_state,"
                "         $5::qiita.failure_type, 'finalize'::qiita.work_ticket_failure_stage,"
                "         'boom')"
                " RETURNING work_ticket_idx",
                self.action_id,
                self.version,
                originator,
                self.ref_idx,
                failure_type or "permanent",
            )
        return await self.pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  reference_idx, state)"
            " VALUES ($1, $2, $3, 'reference', $4, $5::qiita.work_ticket_state)"
            " RETURNING work_ticket_idx",
            self.action_id,
            self.version,
            originator,
            self.ref_idx,
            state,
        )

    async def set_updated_at(self, wt_idx: int, offset_seconds: float):
        return await self.pool.fetchval(
            "UPDATE qiita.work_ticket SET updated_at = now() + make_interval(secs => $2)"
            " WHERE work_ticket_idx = $1 RETURNING updated_at",
            wt_idx,
            float(offset_seconds),
        )

    async def minmax(self, ids: list[int]):
        row = await self.pool.fetchrow(
            "SELECT min(updated_at) AS lo, max(updated_at) AS hi FROM qiita.work_ticket"
            " WHERE work_ticket_idx = ANY($1::bigint[])",
            ids,
        )
        return row["lo"], row["hi"]

    async def notified_at(self, wt_idx: int):
        return await self.pool.fetchval(
            "SELECT notified_at FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx
        )

    async def receipts_for(self, pidx: int) -> list[asyncpg.Record]:
        return await self.pool.fetch(
            "SELECT * FROM qiita.email_receipt WHERE recipient_principal_idx = $1 ORDER BY idx",
            pidx,
        )


@pytest.fixture
async def env(postgres_pool):
    ref_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', true,"
        "         (SELECT MIN(idx) FROM qiita.principal)) RETURNING reference_idx",
        f"notify-{uuid4()}",
    )
    action_id = "notify-test-action"
    version = f"v-{uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling)"
        " VALUES ($1, $2, 'reference', $3::text[], $4::jsonb, $5::jsonb, 1, 1, '1 minute')",
        action_id,
        version,
        ["reference:write"],
        json.dumps({"service": False, "human_roles": ["system_admin"]}),
        json.dumps([]),
    )
    e = _Env(postgres_pool, action_id, version, ref_idx, [])
    try:
        yield e
    finally:
        for pidx in e.principals:
            await postgres_pool.execute(
                "DELETE FROM qiita.email_receipt WHERE recipient_principal_idx = $1", pidx
            )
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE reference_idx = $1", ref_idx
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
        )
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref_idx)
        for pidx in e.principals:
            await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", pidx)
            await postgres_pool.execute(
                "DELETE FROM qiita.service_account WHERE principal_idx = $1", pidx
            )
            await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", pidx)


async def test_gate_opt_in_sends_and_stamps(env):
    user = await env.user()
    wt = await env.ticket(originator=user)
    lo, hi = await env.minmax([wt])
    transport = CaptureTransport()

    result = await sweep_once(env.pool, _settings(), transport, now=hi + timedelta(seconds=200))

    assert len(transport.sent) == 1
    to, _rendered, _mid = transport.sent[0]
    assert to == await env.email_of(user)
    assert await env.notified_at(wt) is not None
    assert result.digests_sent == 1


async def test_gate_opt_out_stamps_without_email(env):
    user = await env.user(receive=False)
    wt = await env.ticket(originator=user)
    lo, hi = await env.minmax([wt])
    transport = CaptureTransport()

    result = await sweep_once(env.pool, _settings(), transport, now=hi + timedelta(seconds=200))

    assert transport.sent == []
    assert await env.notified_at(wt) is not None
    assert result.gated_out == 1
    assert await env.receipts_for(user) == []


async def test_service_principal_no_user_row_stamps_without_email(env):
    svc = await env.service()
    wt = await env.ticket(originator=svc)
    lo, hi = await env.minmax([wt])
    transport = CaptureTransport()

    result = await sweep_once(env.pool, _settings(), transport, now=hi + timedelta(seconds=200))

    assert transport.sent == []
    assert await env.notified_at(wt) is not None
    assert result.gated_out == 1


async def test_grouping_by_originator(env):
    u1 = await env.user()
    u2 = await env.user()
    a = await env.ticket(originator=u1)
    b = await env.ticket(originator=u2)
    lo, hi = await env.minmax([a, b])
    transport = CaptureTransport()

    result = await sweep_once(env.pool, _settings(), transport, now=hi + timedelta(seconds=200))

    assert result.digests_sent == 2
    recipients = {to for to, _, _ in transport.sent}
    assert recipients == {await env.email_of(u1), await env.email_of(u2)}
    # Each digest carries only its own originator's ticket.
    for pidx, own in ((u1, a), (u2, b)):
        receipts = await env.receipts_for(pidx)
        assert len(receipts) == 1
        ctx = json.loads(receipts[0]["template_context"])
        assert ctx["work_ticket_idxs"] == [own]


async def test_debounce_fresh_group_not_flushed(env):
    user = await env.user()
    wt = await env.ticket(originator=user)
    lo, hi = await env.minmax([wt])
    transport = CaptureTransport()

    # Only 10s since the last completion — under the 180s quiet window and the
    # 900s max-wait cap → skip.
    result = await sweep_once(env.pool, _settings(), transport, now=hi + timedelta(seconds=10))

    assert transport.sent == []
    assert await env.notified_at(wt) is None
    assert result.digests_sent == 0


async def test_maxwait_flushes_never_quiescing_originator(env):
    user = await env.user()
    old = await env.ticket(originator=user)
    recent = await env.ticket(originator=user)
    await env.set_updated_at(old, 0)
    await env.set_updated_at(recent, 890)
    lo, hi = await env.minmax([old, recent])
    transport = CaptureTransport()

    # now-max = 10s (< 180 quiet, still settling) but now-min = 900s (>= 900
    # max-wait) → flush anyway.
    result = await sweep_once(
        env.pool, _settings(quiet=180, max_batch=900), transport, now=hi + timedelta(seconds=10)
    )

    assert result.digests_sent == 1
    assert await env.notified_at(old) is not None
    assert await env.notified_at(recent) is not None


async def test_straggler_follow_up_sends_second_email(env):
    user = await env.user()
    a = await env.ticket(originator=user)
    lo, hi = await env.minmax([a])
    transport = CaptureTransport()

    await sweep_once(env.pool, _settings(), transport, now=hi + timedelta(seconds=200))
    assert len(transport.sent) == 1

    # A straggler terminalizes after the first digest was sent.
    b = await env.ticket(originator=user)
    _, hi_b = await env.minmax([b])
    await sweep_once(env.pool, _settings(), transport, now=hi_b + timedelta(seconds=200))

    assert len(transport.sent) == 2
    # The second digest covers only the straggler.
    receipts = await env.receipts_for(user)
    assert len(receipts) == 2
    second_ctx = json.loads(receipts[1]["template_context"])
    assert second_ctx["work_ticket_idxs"] == [b]


async def test_max_age_drains_without_emailing(env):
    user = await env.user()
    wt = await env.ticket(originator=user)
    lo, hi = await env.minmax([wt])
    transport = CaptureTransport()

    # max_age=100, now is 200s past → stale → drained, not emailed.
    result = await sweep_once(
        env.pool, _settings(max_age=100), transport, now=hi + timedelta(seconds=200)
    )

    assert transport.sent == []
    assert await env.notified_at(wt) is not None
    assert result.stale_drained == 1
    assert await env.receipts_for(user) == []


async def test_exact_id_stamping_leaves_mid_window_sibling_owed(env):
    user = await env.user()
    a = await env.ticket(originator=user)
    lo, hi = await env.minmax([a])

    sibling_holder: dict[str, int] = {}

    class _InsertingTransport:
        name = "capture"

        def __init__(self):
            self.sent = []

        async def send(self, *, to: str, rendered: RenderedEmail) -> str:
            # A sibling ticket terminalizes DURING the send window.
            sibling_holder["idx"] = await env.ticket(originator=user)
            self.sent.append((to, rendered, "<mid>"))
            return "<mid>"

    transport = _InsertingTransport()
    await sweep_once(env.pool, _settings(), transport, now=hi + timedelta(seconds=200))

    assert len(transport.sent) == 1
    assert await env.notified_at(a) is not None
    # The sibling added mid-send was NOT in the captured id set → still owed.
    sibling = sibling_holder["idx"]
    assert await env.notified_at(sibling) is None


async def test_dead_letter_cap_stops_retrying(env):
    user = await env.user()
    wt = await env.ticket(originator=user)
    await env.pool.execute(
        "UPDATE qiita.work_ticket SET notify_attempts = 5 WHERE work_ticket_idx = $1", wt
    )
    lo, hi = await env.minmax([wt])
    transport = CaptureTransport()

    result = await sweep_once(
        env.pool, _settings(max_attempts=5), transport, now=hi + timedelta(seconds=200)
    )

    assert transport.sent == []
    assert await env.notified_at(wt) is not None
    assert result.dead_lettered == 1
    receipts = await env.receipts_for(user)
    assert len(receipts) == 1
    assert receipts[0]["status"] == "dead_letter"


async def test_retriable_failed_is_withheld(env):
    user = await env.user()
    retriable = await env.ticket(originator=user, state="failed", failure_type="retriable")
    permanent = await env.ticket(originator=user, state="failed", failure_type="permanent")
    lo, hi = await env.minmax([retriable, permanent])
    transport = CaptureTransport()

    result = await sweep_once(env.pool, _settings(), transport, now=hi + timedelta(seconds=200))

    assert result.digests_sent == 1
    # The permanent failure is emailed and stamped; the retriable one is not
    # even in the owed set.
    assert await env.notified_at(permanent) is not None
    assert await env.notified_at(retriable) is None
    receipts = await env.receipts_for(user)
    ctx = json.loads(receipts[0]["template_context"])
    assert ctx["work_ticket_idxs"] == [permanent]


async def test_receipt_row_correctness(env):
    user = await env.user()
    a = await env.ticket(originator=user)
    b = await env.ticket(originator=user)
    lo, hi = await env.minmax([a, b])
    transport = CaptureTransport()

    await sweep_once(env.pool, _settings(), transport, now=hi + timedelta(seconds=200))

    receipts = await env.receipts_for(user)
    assert len(receipts) == 1
    r = receipts[0]
    assert r["template_name"] == "work_ticket_digest"
    ctx = json.loads(r["template_context"])
    assert set(ctx["work_ticket_idxs"]) == {a, b}
    assert r["recipient_email"] == await env.email_of(user)
    assert r["recipient_principal_idx"] == user
    assert r["subject"]
    assert r["body_text"]
    assert r["body_html"]
    assert r["status"] == "sent"
    assert r["sent_at"] is not None
    assert r["transport"] == "capture"
    assert r["provider_message_id"]
    assert len(r["template_sha"]) == 64
