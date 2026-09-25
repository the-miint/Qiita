"""DB tests for `qiita-admin backfill syndna-read-count`.

The plan resolves each completed, uncounted SynDNA-masked sample to the alignment
file its read-mask ticket's `syndna` step left in scratch — completed step row →
attempt → `output/manifest.json` → the file bound to `alignment` — or to a reason
it cannot. The plan scans the whole database, so every assertion here is about this
test's own pairs.
"""

import json
import uuid

import duckdb
import pytest
import pytest_asyncio
from qiita_common.actions import READ_MASK_ACTION_ID
from qiita_common.models import ScopeTargetKind

from qiita_control_plane.backfill.syndna_read_count import apply_backfill, plan_backfill
from qiita_control_plane.testing.db_seeds import (
    cleanup_reference_graph,
    delete_action_if_created,
    seed_action_if_absent,
    seed_bare_feature,
    seed_bare_reference,
    seed_biosample_with_sequenced_prep_sample,
    seed_reference_membership,
    seed_user_principal,
)

pytestmark = pytest.mark.db

_VERSION = "1.0.0"


@pytest_asyncio.fixture
async def world(postgres_pool, tmp_path):
    pool = postgres_pool
    action_created = await seed_action_if_absent(
        pool,
        action_id=READ_MASK_ACTION_ID,
        version=_VERSION,
        target_kind=ScopeTargetKind.PREP_SAMPLE.value,
    )
    owner = await seed_user_principal(pool, prefix="syndna-bf", suffix="owner")
    reference_idx = await seed_bare_reference(pool, label="syndna-backfill")
    inserts = [await seed_bare_feature(pool) for _ in range(2)]
    for feature_idx in inserts:
        await seed_reference_membership(pool, reference_idx=reference_idx, feature_idx=feature_idx)
    mask_idx = await pool.fetchval(
        "INSERT INTO qiita.mask_definition"
        " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, 'read-mask', '1.0.0', $2::jsonb, $3) RETURNING mask_idx",
        uuid.uuid4().bytes + uuid.uuid4().bytes,
        json.dumps({"resolved_syndna": {"reference_idx": reference_idx}}),
        owner,
    )
    samples: list[tuple[int, int]] = []
    tickets: list[int] = []

    async def masked_sample(*, ticket: bool = True, step: bool = True, attempt: int = 1):
        """A completed gate row, optionally with a completed read-mask ticket whose
        `syndna` step completed on `attempt`. Returns (prep_sample_idx, attempt_dir)."""
        biosample_idx, ps = await seed_biosample_with_sequenced_prep_sample(pool, owner_idx=owner)
        samples.append((biosample_idx, ps))
        await pool.execute(
            "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
            " VALUES ($1, $2, 'completed')",
            mask_idx,
            ps,
        )
        if not ticket:
            return ps, None
        ticket_idx = await pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  prep_sample_idx, mask_idx, state)"
            " VALUES ($1, $2, $3, 'prep_sample', $4, $5, 'completed')"
            " RETURNING work_ticket_idx",
            READ_MASK_ACTION_ID,
            _VERSION,
            owner,
            ps,
            mask_idx,
        )
        tickets.append(ticket_idx)
        if not step:
            return ps, None
        await pool.execute(
            "INSERT INTO qiita.work_ticket_step"
            " (work_ticket_idx, step_index, attempt, step_name, compute_target, state)"
            " VALUES ($1, 0, $2, 'syndna', 'local', 'completed')",
            ticket_idx,
            attempt,
        )
        return ps, tmp_path / str(ticket_idx) / "syndna" / f"attempt-{attempt}"

    yield {
        "pool": pool,
        "mask_idx": mask_idx,
        "inserts": inserts,
        "ticket_root": tmp_path,
        "masked_sample": masked_sample,
    }

    await pool.execute("DELETE FROM qiita.syndna_read_count WHERE mask_idx = $1", mask_idx)
    await pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])", tickets
    )
    await pool.execute("DELETE FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx)
    await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    for biosample_idx, ps in samples:
        await pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps)
        await pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await cleanup_reference_graph(pool, reference_idx=reference_idx, feature_idxs=inserts)
    await delete_action_if_created(
        pool, action_id=READ_MASK_ACTION_ID, version=_VERSION, created=action_created
    )


def _write_output(attempt_dir, rows, *, bind: bool = True):
    """Write the step's alignment file and its manifest, as the step's launcher does."""
    output = attempt_dir / "output"
    output.mkdir(parents=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE a (prep_sample_idx BIGINT, sequence_idx BIGINT,"
            " parent_feature_idx BIGINT)"
        )
        for row in rows:
            conn.execute("INSERT INTO a VALUES (?, ?, ?)", list(row))
        conn.execute(f"COPY a TO '{output / 'syndna_alignment.parquet'}' (FORMAT PARQUET)")
    outputs = {"alignment": "syndna_alignment.parquet"} if bind else {}
    (output / "manifest.json").write_text(json.dumps({"files": [], "outputs": outputs}))


def _mine(plan, mask_idx):
    return {p.prep_sample_idx: p for p in plan.pairs if p.mask_idx == mask_idx}


async def test_plans_each_pair_to_its_file_or_a_reason_and_applies(world):
    pool, mask_idx, (f1, f2) = world["pool"], world["mask_idx"], world["inserts"]
    ok, attempt_dir = await world["masked_sample"](attempt=2)
    _write_output(attempt_dir, [(ok, 1, f1), (ok, 2, f1), (ok, 3, f2)])
    no_ticket, _ = await world["masked_sample"](ticket=False)
    no_step, _ = await world["masked_sample"](step=False)
    gone, _ = await world["masked_sample"]()
    unbound, unbound_dir = await world["masked_sample"]()
    _write_output(unbound_dir, [], bind=False)

    plan = await plan_backfill(pool, ticket_root=world["ticket_root"])
    mine = _mine(plan, mask_idx)

    assert mine[ok].alignment_path == attempt_dir / "output" / "syndna_alignment.parquet"
    assert "no completed read-mask ticket" in mine[no_ticket].reason
    assert "no completed 'syndna' step" in mine[no_step].reason
    assert "no manifest" in mine[gone].reason
    assert "binds no 'alignment'" in mine[unbound].reason

    await apply_backfill(pool, plan)
    rows = await pool.fetch(
        "SELECT prep_sample_idx, feature_idx, read_count FROM qiita.syndna_read_count"
        " WHERE mask_idx = $1",
        mask_idx,
    )
    assert {(r["prep_sample_idx"], r["feature_idx"], r["read_count"]) for r in rows} == {
        (ok, f1, 2),
        (ok, f2, 1),
    }

    # Idempotent: the counted pair is out of the next plan; the residue stays.
    again = _mine(await plan_backfill(pool, ticket_root=world["ticket_root"]), mask_idx)
    assert set(again) == {no_ticket, no_step, gone, unbound}
