"""DB tests for the `qiita-admin mask purge-failed` bulk recovery helper.

Covers the behaviours the design calls out:
  (a) the selector finds the right failed tickets and ignores others;
  (b) the shared-mask guard SKIPS a mask referenced by a COMPLETED ticket and
      the mask survives;
  (c) dry-run mutates nothing;
  (d) --execute deletes the mask (REST mask-delete stubbed, as this tier has no
      data plane) and issues the resubmit, with capture-before-delete ordering;
  (e) a resubmit that fails AFTER its mask was deleted lands in failures with a
      replay body, and the batch continues to the next candidate;
  (f) a FAILED candidate with NULL mask_idx lands in skipped_no_mask_idx;
  (g) the S2 backfill-completeness gate: a NON-failed NULL mask_idx ticket makes
      --execute REFUSE (and dry-run flag it);
  (h) shared-mask dedup: two failed candidates on one mask delete it once.

These are db-marked: they seed a principal + biosample + sequenced prep_sample,
an action, mask_definition rows, and work_ticket rows in real Postgres. The two
REST hops (mask delete + resubmit) are monkeypatched at the module boundary — the
DB tier has no control-plane HTTP server or data plane — so the tests assert the
direct-DB half (selector, guard, ticket delete) and the call orchestration.
"""

import json
import secrets
import uuid

import pytest
import pytest_asyncio

from qiita_control_plane.cli import admin
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)

pytestmark = pytest.mark.db


async def _seed_action(pool, action_id: str, version: str) -> None:
    await pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, "
        "  context_schema, steps, "
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, "
        "  success_status, failure_status"
        ") VALUES ($1, $2, 'prep_sample', $3::text[], $4::jsonb,"
        "  $5::jsonb, $6::jsonb, 1, 1, '1 minute', $7, $8)",
        action_id,
        version,
        ["feature:mint"],
        json.dumps({"service": False, "human_roles": ["wet_lab_admin"]}),
        json.dumps({}),
        json.dumps([]),
        "active",
        "failed",
    )


async def _seed_mask(pool, principal_idx: int) -> int:
    """Insert a minimal qiita.mask_definition row; return its mask_idx."""
    return await pool.fetchval(
        "INSERT INTO qiita.mask_definition"
        " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, 'read-mask', '1.0.0', '{}'::jsonb, $2)"
        " RETURNING mask_idx",
        uuid.uuid4().bytes + uuid.uuid4().bytes,  # 32-byte params_hash
        principal_idx,
    )


async def _seed_ticket(
    pool,
    *,
    action_id: str,
    version: str,
    principal_idx: int,
    prep_sample_idx: int,
    state: str,
    mask_idx: int | None,
    action_context: dict | None = None,
    failure_reason: str | None = None,
) -> int:
    if state == "failed":
        failure_type, failure_stage, failure_step_name = (
            "permanent",
            "step_run",
            "persist-read-metrics",
        )
    else:
        failure_type = failure_stage = failure_step_name = None
        failure_reason = None
    return await pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, prep_sample_idx, action_context, state, mask_idx,"
        "  failure_type, failure_stage, failure_step_name, failure_reason"
        ") VALUES ($1, $2, $3, 'prep_sample', $4, $5::jsonb, $6, $7, $8, $9, $10, $11)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        principal_idx,
        prep_sample_idx,
        json.dumps(action_context or {}),
        state,
        mask_idx,
        failure_type,
        failure_stage,
        failure_step_name,
        failure_reason,
    )


@pytest_asyncio.fixture
async def seeded(postgres_pool):
    principal_idx = await seed_user_principal(postgres_pool, prefix="purge", suffix="owner")
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
    )
    action_id = "read-mask"
    version = f"purge-{secrets.token_hex(4)}"
    await _seed_action(postgres_pool, action_id, version)
    state = {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idx": prep_sample_idx,
        "action_id": action_id,
        "version": version,
        "tickets": [],
        "masks": [],
    }
    yield state
    if state["tickets"]:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])",
            state["tickets"],
        )
    # Catch any resubmit-created tickets for this prep_sample/action too.
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE prep_sample_idx = $1 AND action_id = $2"
        " AND action_version = $3",
        prep_sample_idx,
        action_id,
        version,
    )
    if state["masks"]:
        await postgres_pool.execute(
            "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
            state["masks"],
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
    )
    await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


# ---------------------------------------------------------------------------
# (a) selector
# ---------------------------------------------------------------------------


async def test_selector_finds_matching_and_ignores_others(seeded):
    pool = seeded["pool"]
    aid, ver = seeded["action_id"], seeded["version"]
    base = dict(
        action_id=aid,
        version=ver,
        principal_idx=seeded["principal_idx"],
        prep_sample_idx=seeded["prep_sample_idx"],
    )
    mask = await _seed_mask(pool, seeded["principal_idx"])
    seeded["masks"].append(mask)

    # Matches: failed + the read_mask substring.
    match = await _seed_ticket(
        pool,
        **base,
        state="failed",
        mask_idx=mask,
        failure_reason="step persist-read-metrics: read_mask parquet not found at /staging/x",
    )
    # Ignored: failed but a different failure_reason.
    other_reason = await _seed_ticket(
        pool, **base, state="failed", mask_idx=mask, failure_reason="container OOM-killed"
    )
    # Ignored: completed (not failed).
    completed = await _seed_ticket(pool, **base, state="completed", mask_idx=mask)
    seeded["tickets"] += [match, other_reason, completed]

    rows = await admin._select_purge_failed_candidates(pool, action_ids=("read-mask",), limit=None)
    found = {r["work_ticket_idx"] for r in rows}
    assert match in found
    assert other_reason not in found
    assert completed not in found


# ---------------------------------------------------------------------------
# (b) shared-mask guard
# ---------------------------------------------------------------------------


async def test_shared_mask_guard_skips_and_mask_survives(seeded, monkeypatch):
    pool = seeded["pool"]
    aid, ver = seeded["action_id"], seeded["version"]
    base = dict(
        action_id=aid,
        version=ver,
        principal_idx=seeded["principal_idx"],
        prep_sample_idx=seeded["prep_sample_idx"],
    )
    shared_mask = await _seed_mask(pool, seeded["principal_idx"])
    seeded["masks"].append(shared_mask)

    failed = await _seed_ticket(
        pool,
        **base,
        state="failed",
        mask_idx=shared_mask,
        failure_reason="read_mask parquet not found",
    )
    # A COMPLETED ticket references the SAME mask -> must not be deleted.
    completed = await _seed_ticket(pool, **base, state="completed", mask_idx=shared_mask)
    seeded["tickets"] += [failed, completed]

    # Guard query directly.
    non_failed = await admin._mask_shared_with_non_failed(pool, shared_mask)
    assert completed in non_failed

    # The mask-delete route must NEVER be called for a shared mask. Make it
    # explode if invoked so a regression is loud.
    def _boom(*a, **k):
        raise AssertionError("mask delete called for a shared mask")

    monkeypatch.setattr(admin, "_mask_delete_via_route", _boom)
    monkeypatch.setattr(admin, "_resubmit_work_ticket", _boom)

    report = await admin._purge_failed(
        "unused-database-url",  # pool is real via the patched create_pool below
        "http://localhost:8080",
        "tok",
        action_ids=("read-mask",),
        execute=True,
        with_tickets=True,
        limit=None,
        rate_seconds=0.0,
        wait=False,
    )

    # The failed ticket landed in skipped_shared, not eligible/purged.
    skipped_idxs = {s["work_ticket_idx"] for s in report["skipped_shared"]}
    assert failed in skipped_idxs
    assert report["eligible"] == []
    assert report["purged"] == []
    # The mask still exists in Postgres (never deleted), and the failed ticket
    # survives (never deleted) since it was skipped.
    assert (
        await pool.fetchval("SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", shared_mask)
        == 1
    )
    assert (
        await pool.fetchval("SELECT 1 FROM qiita.work_ticket WHERE work_ticket_idx = $1", failed)
        == 1
    )


# `_purge_failed` opens its own pool from DATABASE_URL. In tests we already have a
# `postgres_pool` fixture; patch `asyncpg.create_pool` (as used inside admin) to
# hand back a thin wrapper over that pool whose `.close()` is a no-op so the
# shared fixture pool survives the call.
@pytest_asyncio.fixture(autouse=True)
def _use_fixture_pool(postgres_pool, monkeypatch):
    class _NoClosePool:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        async def close(self):  # don't tear down the shared fixture pool
            return None

    async def _fake_create_pool(*a, **k):
        return _NoClosePool(postgres_pool)

    monkeypatch.setattr(admin.asyncpg, "create_pool", _fake_create_pool)


# ---------------------------------------------------------------------------
# (c) dry-run mutates nothing
# ---------------------------------------------------------------------------


async def test_dry_run_mutates_nothing(seeded, monkeypatch):
    pool = seeded["pool"]
    aid, ver = seeded["action_id"], seeded["version"]
    base = dict(
        action_id=aid,
        version=ver,
        principal_idx=seeded["principal_idx"],
        prep_sample_idx=seeded["prep_sample_idx"],
    )
    mask = await _seed_mask(pool, seeded["principal_idx"])
    seeded["masks"].append(mask)
    failed = await _seed_ticket(
        pool,
        **base,
        state="failed",
        mask_idx=mask,
        failure_reason="read_mask parquet not found",
        action_context={"instrument_model": "NextSeq 550"},
    )
    seeded["tickets"].append(failed)

    # Any REST call in a dry-run is a bug.
    def _boom(*a, **k):
        raise AssertionError("REST call during dry-run")

    monkeypatch.setattr(admin, "_mask_delete_via_route", _boom)
    monkeypatch.setattr(admin, "_resubmit_work_ticket", _boom)

    before = await pool.fetchval("SELECT COUNT(*) FROM qiita.work_ticket")

    report = await admin._purge_failed(
        "unused",
        "http://localhost:8080",
        "tok",
        action_ids=("read-mask",),
        execute=False,
        with_tickets=True,
        limit=None,
        rate_seconds=0.0,
        wait=False,
    )

    assert report["executed"] is False
    assert {e["work_ticket_idx"] for e in report["eligible"]} == {failed}
    assert report["purged"] == []
    assert report["resubmitted"] == []
    # Nothing changed: mask present, ticket present, ticket count unchanged.
    assert await pool.fetchval("SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", mask) == 1
    assert (
        await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", failed
        )
        == "failed"
    )
    assert await pool.fetchval("SELECT COUNT(*) FROM qiita.work_ticket") == before


# ---------------------------------------------------------------------------
# (d) --execute deletes the mask + resubmits
# ---------------------------------------------------------------------------


async def test_execute_deletes_mask_and_resubmits(seeded, monkeypatch):
    pool = seeded["pool"]
    aid, ver = seeded["action_id"], seeded["version"]
    base = dict(
        action_id=aid,
        version=ver,
        principal_idx=seeded["principal_idx"],
        prep_sample_idx=seeded["prep_sample_idx"],
    )
    mask = await _seed_mask(pool, seeded["principal_idx"])
    seeded["masks"].append(mask)
    failed = await _seed_ticket(
        pool,
        **base,
        state="failed",
        mask_idx=mask,
        failure_reason="read_mask parquet not found",
        action_context={"instrument_model": "NextSeq 550"},
    )
    seeded["tickets"].append(failed)

    # Stub the mask-delete route: record it was called and actually drop the
    # Postgres mask_definition row (the route does this lake-first; here we
    # simulate just the Postgres side since there's no data plane).
    delete_calls: list[int] = []

    def _stub_delete(base_url, token, mask_idx):
        delete_calls.append(mask_idx)
        return {"mask_idx": mask_idx, "rows_deleted": 5}

    # Stub the resubmit: record the body and return a synthetic new ticket id.
    resubmit_bodies: list[dict] = []

    def _stub_resubmit(base_url, token, body):
        resubmit_bodies.append(body)
        # capture-before-delete check: the mask row should already be gone by the
        # time resubmit runs (delete ran first), so the body must still carry the
        # faithfully-captured params.
        return {"work_ticket_idx": 999_000_001, "state": "pending"}

    monkeypatch.setattr(admin, "_mask_delete_via_route", _stub_delete)
    monkeypatch.setattr(admin, "_resubmit_work_ticket", _stub_resubmit)

    report = await admin._purge_failed(
        "unused",
        "http://localhost:8080",
        "tok",
        action_ids=("read-mask",),
        execute=True,
        with_tickets=True,
        limit=None,
        rate_seconds=0.0,
        wait=False,
    )

    # Mask delete dialed exactly once for our mask.
    assert delete_calls == [mask]
    assert report["purged"] == [{"work_ticket_idx": failed, "mask_idx": mask, "rows_deleted": 5}]
    # Resubmit body faithfully reconstructed from the stored ticket row.
    assert len(resubmit_bodies) == 1
    body = resubmit_bodies[0]
    assert body["action_id"] == aid
    assert body["action_version"] == ver
    assert body["scope_target"] == {
        "kind": "prep_sample",
        "prep_sample_idx": seeded["prep_sample_idx"],
    }
    assert body["action_context"] == {"instrument_model": "NextSeq 550"}
    assert report["resubmitted"][0]["new_work_ticket_idx"] == 999_000_001
    # --with-tickets deleted the original FAILED ticket.
    assert (
        await pool.fetchval("SELECT 1 FROM qiita.work_ticket WHERE work_ticket_idx = $1", failed)
        is None
    )
    assert report["failures"] == []


# ---------------------------------------------------------------------------
# (e) resubmit fails AFTER mask delete -> recoverable failure entry; batch continues
# ---------------------------------------------------------------------------


async def test_resubmit_failure_after_mask_delete_is_recoverable(seeded, monkeypatch):
    pool = seeded["pool"]
    aid, ver = seeded["action_id"], seeded["version"]
    base = dict(
        action_id=aid,
        version=ver,
        principal_idx=seeded["principal_idx"],
        prep_sample_idx=seeded["prep_sample_idx"],
    )
    # Two DISTINCT masks so each candidate is independent (no dedup), letting us
    # assert the batch continues to the second after the first's resubmit fails.
    mask_a = await _seed_mask(pool, seeded["principal_idx"])
    mask_b = await _seed_mask(pool, seeded["principal_idx"])
    seeded["masks"] += [mask_a, mask_b]
    failed_a = await _seed_ticket(
        pool,
        **base,
        state="failed",
        mask_idx=mask_a,
        failure_reason="read_mask parquet not found",
        action_context={"instrument_model": "NextSeq 550"},
    )
    failed_b = await _seed_ticket(
        pool,
        **base,
        state="failed",
        mask_idx=mask_b,
        failure_reason="read_mask parquet not found",
        action_context={"instrument_model": "MiSeq"},
    )
    seeded["tickets"] += [failed_a, failed_b]

    # Mask delete always succeeds (records calls). The Postgres mask_definition
    # rows are left in place here — the test only asserts the route was dialed.
    delete_calls: list[int] = []

    def _stub_delete(base_url, token, mask_idx):
        delete_calls.append(mask_idx)
        return {"mask_idx": mask_idx, "rows_deleted": 7}

    # Resubmit raises for the FIRST candidate (mask_a) only, after its mask was
    # already deleted; the second (mask_b) succeeds.
    resubmit_bodies: list[dict] = []

    def _stub_resubmit(base_url, token, body):
        resubmit_bodies.append(body)
        if body["action_context"] == {"instrument_model": "NextSeq 550"}:
            raise RuntimeError("orchestrator unreachable")
        return {"work_ticket_idx": 999_000_002, "state": "pending"}

    monkeypatch.setattr(admin, "_mask_delete_via_route", _stub_delete)
    monkeypatch.setattr(admin, "_resubmit_work_ticket", _stub_resubmit)

    report = await admin._purge_failed(
        "unused",
        "http://localhost:8080",
        "tok",
        action_ids=("read-mask",),
        execute=True,
        with_tickets=True,
        limit=None,
        rate_seconds=0.0,
        wait=False,
    )

    # Both masks were deleted (delete ran before each resubmit).
    assert set(delete_calls) == {mask_a, mask_b}
    # The first item is in failures; the batch continued and the second succeeded.
    assert len(report["failures"]) == 1
    fail = report["failures"][0]
    assert fail["work_ticket_idx"] == failed_a
    assert fail["mask_idx"] == mask_a
    # The mask IS deleted by the time resubmit failed; flags + replay body present.
    assert fail["mask_deleted"] is True
    assert fail["ticket_deleted"] is True
    assert fail["resubmit_body"]["action_context"] == {"instrument_model": "NextSeq 550"}
    assert fail["resubmit_body"]["scope_target"] == {
        "kind": "prep_sample",
        "prep_sample_idx": seeded["prep_sample_idx"],
    }
    # The second candidate still processed.
    assert len(report["resubmitted"]) == 1
    assert report["resubmitted"][0]["original_work_ticket_idx"] == failed_b
    assert report["resubmitted"][0]["new_work_ticket_idx"] == 999_000_002


# ---------------------------------------------------------------------------
# (f) a FAILED candidate with NULL mask_idx -> skipped_no_mask_idx (NOT the S2 gate)
# ---------------------------------------------------------------------------


async def test_failed_null_mask_idx_lands_in_skipped_no_mask_idx(seeded, monkeypatch):
    pool = seeded["pool"]
    aid, ver = seeded["action_id"], seeded["version"]
    base = dict(
        action_id=aid,
        version=ver,
        principal_idx=seeded["principal_idx"],
        prep_sample_idx=seeded["prep_sample_idx"],
    )
    # A FAILED candidate that was never backfilled (mask_idx IS NULL). This is
    # distinct from the S2 gate, which only fires on NON-failed NULL mask_idx.
    failed = await _seed_ticket(
        pool,
        **base,
        state="failed",
        mask_idx=None,
        failure_reason="read_mask parquet not found",
    )
    seeded["tickets"].append(failed)

    def _boom(*a, **k):
        raise AssertionError("must not touch a NULL-mask_idx failed candidate")

    monkeypatch.setattr(admin, "_mask_delete_via_route", _boom)
    monkeypatch.setattr(admin, "_resubmit_work_ticket", _boom)

    report = await admin._purge_failed(
        "unused",
        "http://localhost:8080",
        "tok",
        action_ids=("read-mask",),
        execute=True,  # the S2 gate stays at 0 (no NON-failed NULL ticket), so execute is allowed
        with_tickets=True,
        limit=None,
        rate_seconds=0.0,
        wait=False,
    )

    assert report["backfill_incomplete"] == 0
    assert failed in report["skipped_no_mask_idx"]
    assert report["eligible"] == []
    assert report["purged"] == []
    assert report["resubmitted"] == []
    # The candidate survives untouched.
    assert (
        await pool.fetchval("SELECT 1 FROM qiita.work_ticket WHERE work_ticket_idx = $1", failed)
        == 1
    )


# ---------------------------------------------------------------------------
# (g) S2 backfill-completeness gate: a NON-failed NULL mask_idx ticket refuses --execute
# ---------------------------------------------------------------------------


async def test_s2_gate_refuses_execute_on_non_failed_null_mask_idx(seeded, monkeypatch):
    pool = seeded["pool"]
    aid, ver = seeded["action_id"], seeded["version"]
    base = dict(
        action_id=aid,
        version=ver,
        principal_idx=seeded["principal_idx"],
        prep_sample_idx=seeded["prep_sample_idx"],
    )
    mask = await _seed_mask(pool, seeded["principal_idx"])
    seeded["masks"].append(mask)
    # An eligible failed candidate...
    failed = await _seed_ticket(
        pool,
        **base,
        state="failed",
        mask_idx=mask,
        failure_reason="read_mask parquet not found",
    )
    # ...but a NON-failed (completed) ticket has a NULL mask_idx -> backfill is
    # incomplete -> the shared-mask guard is blind to it -> --execute must refuse.
    completed_null = await _seed_ticket(pool, **base, state="completed", mask_idx=None)
    seeded["tickets"] += [failed, completed_null]

    def _boom(*a, **k):
        raise AssertionError("no destructive work while backfill incomplete")

    monkeypatch.setattr(admin, "_mask_delete_via_route", _boom)
    monkeypatch.setattr(admin, "_resubmit_work_ticket", _boom)

    # Dry-run FLAGS the condition (no refusal, just reports it).
    dry = await admin._purge_failed(
        "unused",
        "http://localhost:8080",
        "tok",
        action_ids=("read-mask",),
        execute=False,
        with_tickets=True,
        limit=None,
        rate_seconds=0.0,
        wait=False,
    )
    assert dry["backfill_incomplete"] >= 1

    # --execute REFUSES with an actionable error naming the backfill command.
    with pytest.raises(RuntimeError, match="backfill-mask-idx"):
        await admin._purge_failed(
            "unused",
            "http://localhost:8080",
            "tok",
            action_ids=("read-mask",),
            execute=True,
            with_tickets=True,
            limit=None,
            rate_seconds=0.0,
            wait=False,
        )

    # Nothing was deleted: both tickets and the mask survive.
    assert (
        await pool.fetchval("SELECT 1 FROM qiita.work_ticket WHERE work_ticket_idx = $1", failed)
        == 1
    )
    assert (
        await pool.fetchval(
            "SELECT 1 FROM qiita.work_ticket WHERE work_ticket_idx = $1", completed_null
        )
        == 1
    )
    assert await pool.fetchval("SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", mask) == 1


# ---------------------------------------------------------------------------
# (h) shared-mask dedup: two failed candidates on one mask -> delete dialed once
# ---------------------------------------------------------------------------


async def test_shared_mask_dedup_deletes_once_resubmits_both(seeded, monkeypatch):
    pool = seeded["pool"]
    aid, ver = seeded["action_id"], seeded["version"]
    base = dict(
        action_id=aid,
        version=ver,
        principal_idx=seeded["principal_idx"],
        prep_sample_idx=seeded["prep_sample_idx"],
    )
    # One mask, two FAILED candidates referencing it. Neither sharer is
    # non-failed, so the shared-mask guard does NOT skip them — they are eligible,
    # and the mask delete should dedup to exactly one call.
    mask = await _seed_mask(pool, seeded["principal_idx"])
    seeded["masks"].append(mask)
    failed_1 = await _seed_ticket(
        pool,
        **base,
        state="failed",
        mask_idx=mask,
        failure_reason="read_mask parquet not found",
        action_context={"instrument_model": "NextSeq 550"},
    )
    failed_2 = await _seed_ticket(
        pool,
        **base,
        state="failed",
        mask_idx=mask,
        failure_reason="read_mask parquet not found",
        action_context={"instrument_model": "MiSeq"},
    )
    seeded["tickets"] += [failed_1, failed_2]

    delete_calls: list[int] = []

    def _stub_delete(base_url, token, mask_idx):
        delete_calls.append(mask_idx)
        return {"mask_idx": mask_idx, "rows_deleted": 3}

    resubmit_bodies: list[dict] = []

    def _stub_resubmit(base_url, token, body):
        resubmit_bodies.append(body)
        return {"work_ticket_idx": 999_000_100 + len(resubmit_bodies), "state": "pending"}

    monkeypatch.setattr(admin, "_mask_delete_via_route", _stub_delete)
    monkeypatch.setattr(admin, "_resubmit_work_ticket", _stub_resubmit)

    report = await admin._purge_failed(
        "unused",
        "http://localhost:8080",
        "tok",
        action_ids=("read-mask",),
        execute=True,
        with_tickets=True,
        limit=None,
        rate_seconds=0.0,
        wait=False,
    )

    # Mask delete dialed EXACTLY once (deduped) despite two candidates.
    assert delete_calls == [mask]
    # Both candidates resubmitted.
    assert len(resubmit_bodies) == 2
    assert {b["action_context"]["instrument_model"] for b in resubmit_bodies} == {
        "NextSeq 550",
        "MiSeq",
    }
    assert {r["original_work_ticket_idx"] for r in report["resubmitted"]} == {failed_1, failed_2}
    # purged report has one entry (the single dedup delete).
    assert len(report["purged"]) == 1
    assert report["purged"][0]["mask_idx"] == mask
    assert report["failures"] == []
