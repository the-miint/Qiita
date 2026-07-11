"""Cross-component integration tests for the bulk-block read-mask workflow's
`action:` tail (`delete-block-mask` → `register-files` → `reconcile-block`)
against a real control plane, a real data-plane Flight/DuckLake process, and the
integration Postgres. The analog of `test_read_mask_e2e.py` for the block path.

The native compute steps (`qc`, `host_filter`) are NOT run here — they need
miint/rype/minimap2 and real stored reads (system-tier). Instead we pre-stage a
multi-sample `read_mask.parquet` in host_filter's exact BLOCK emit shape (a
per-row `prep_sample_idx`, not a per-run constant) and drive the REAL runner
adapter `_run_action_primitive` for the three block `action:` entries — parsed in
their shipped order from `workflows/read-mask-block/1.0.0.yaml` — so the real
LIBRARY primitives, the real data-plane `register_files` /
`delete_read_mask_block` DoActions, the real `mask_metrics` aggregate + count
assertion, and the real DuckLake catalog are all exercised end to end.

Two things this branch introduced are otherwise unproven end-to-end:

1. SPLIT-SAMPLE PER-SAMPLE RECONCILE. A sample whose reads are tiled across two
   blocks must finalize (metrics rolled up from the PERSISTED DuckLake aggregate,
   `mask_sample` gate flipped) ONLY once BOTH covering blocks complete — and the
   masked-read export ticket must 409 while a covering block is still in flight,
   201 once complete. This proves the gate has teeth against the real reconcile.

2. IDEMPOTENT BLOCK REPLACE. `delete-block-mask` runs before `register-files`, so
   a block re-run deletes its exact footprint (its member sub-ranges under the
   ticket mask_idx) then re-registers — leaving exactly one logical copy. This
   file proves delete-then-register self-cleans, and that register-twice WITHOUT
   the delete WOULD duplicate (so the no-dup assertion has teeth) and that a
   subsequent delete-block-mask cleans the duplicate back to the exact footprint.

Shared fixtures (`data_plane`, `postgres_pool`, `human_admin_session`,
`ducklake_connect`) live in conftest.py.
"""

from __future__ import annotations

import secrets
from pathlib import Path

import duckdb
import pytest
from fastapi import HTTPException
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.models import MaskedReadExportTicketRequest, ReadMaskReason

from conftest import ducklake_connect

_READ_MASK_BLOCK_YAML_PATH = (
    Path(__file__).parent.parent.parent / "workflows" / "read-mask-block" / "1.0.0.yaml"
)

# Reads per sample. Small — the count assertion checks read_mask row_count ==
# sequence_range count, so the staged rows must match exactly, and a tiny range
# keeps the DuckLake writes cheap.
_READS_PER_SAMPLE = 4

# host_filter's BLOCK emit schema, mirrored exactly (per-row prep_sample_idx).
_READ_MASK_PARQUET_COLUMNS = (
    "mask_idx BIGINT, prep_sample_idx BIGINT, sequence_idx BIGINT, "
    "reason VARCHAR, "
    "left_trim1 UINTEGER, right_trim1 UINTEGER, "
    "left_trim2 UINTEGER, right_trim2 UINTEGER"
)

# A paired-end sample's four reads: 2 pass, 1 host hit, 1 qc failure. The
# both-mates *_r1r2 totals _read_mask_counts / mask_metrics_counts derive are:
#   raw               = all 4 reads, both mates            -> 8
#   biological        = non-qc_* (2 pass + 1 host)          -> 6
#   quality_filtered  = pass only                           -> 4
_REASONS_IN_ORDER = [
    ReadMaskReason.PASS.value,
    ReadMaskReason.PASS.value,
    ReadMaskReason.HOST_RYPE.value,
    ReadMaskReason.QC_TOO_SHORT.value,
]
_EXPECTED_RAW_R1R2 = 8
_EXPECTED_BIOLOGICAL_R1R2 = 6
_EXPECTED_QUALITY_FILTERED_R1R2 = 4


def _block_action_entry(name: str):
    """Parse the shipped read-mask-block YAML and return one `action:`
    WorkflowAction entry by name. Driving whatever the YAML declares (rather than
    hand-constructing entries) means a future reorder / rename is reflected here
    automatically."""
    import yaml
    from qiita_common.actions import ActionDefinition, WorkflowAction

    data = yaml.safe_load(_READ_MASK_BLOCK_YAML_PATH.read_text())
    action = ActionDefinition.model_validate(data)
    for entry in action.steps:
        if isinstance(entry, WorkflowAction) and entry.name == name:
            return entry
    raise AssertionError(f"no action entry named {name!r} in read-mask-block YAML")


def _write_block_read_mask_parquet(
    staging_dir: Path, *, mask_idx: int, rows: list[tuple]
) -> Path:
    """Materialize a block's `read_mask.parquet` under `staging_dir` in
    host_filter's block emit shape and return the parquet path. `rows` are
    (prep_sample_idx, sequence_idx, reason, lt1, rt1, lt2, rt2) tuples — a MULTI
    sample block carries several prep_sample_idx values, one per row."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    path = staging_dir / "read_mask.parquet"
    full_rows = [
        (mask_idx, ps, seq_idx, reason, lt1, rt1, lt2, rt2)
        for (ps, seq_idx, reason, lt1, rt1, lt2, rt2) in rows
    ]
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"CREATE TABLE m ({_READ_MASK_PARQUET_COLUMNS})")
        conn.executemany("INSERT INTO m VALUES (?, ?, ?, ?, ?, ?, ?, ?)", full_rows)
        conn.execute(
            f"COPY (SELECT * FROM m ORDER BY mask_idx, prep_sample_idx, sequence_idx)"
            f" TO '{path}' (FORMAT PARQUET)"
        )
    return path


def _mask_rows_for(prep_sample_idx: int, seq_start: int) -> list[tuple]:
    """A full sample's four paired-end read_mask rows (reasons per
    `_REASONS_IN_ORDER`), sequence_idx running from `seq_start`."""
    return [
        (prep_sample_idx, seq_start + i, reason, 0, 0, 0, 0)
        for i, reason in enumerate(_REASONS_IN_ORDER)
    ]


def _data_plane_url(data_plane) -> str:
    return f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"


def _count_read_mask_rows(data_plane, *, mask_idx: int, prep_sample_idx: int) -> int:
    """Count DuckLake `read_mask` rows for a (mask, prep_sample) — the table is not
    Flight-reachable for privacy, so read it via a direct DuckLake conn."""
    conn = ducklake_connect(data_plane["data_path"])
    try:
        (n,) = conn.execute(
            "SELECT count(*) FROM qiita_lake.read_mask"
            " WHERE mask_idx = ? AND prep_sample_idx = ?",
            [mask_idx, prep_sample_idx],
        ).fetchone()
        return n
    finally:
        conn.close()


async def _run_block_action(
    postgres_pool,
    data_plane,
    *,
    name,
    block_idx,
    mask_idx,
    bound,
    workspace,
    work_ticket_idx=0,
):
    """Drive the REAL runner adapter for one block `action:` entry, block-scoped.
    `work_ticket_idx` names register-files' lake file (`wt{idx}-read_mask.parquet`),
    so each registration needs a distinct value (the DP refuses to overwrite)."""
    from qiita_control_plane.runner import _run_action_primitive

    await _run_action_primitive(
        postgres_pool,
        _block_action_entry(name),
        bound,
        workspace,
        {"kind": "block", "block_idx": block_idx},
        work_ticket_idx=work_ticket_idx,
        signing_key=data_plane["secret"],
        data_plane_url=_data_plane_url(data_plane),
    )


@pytest.fixture
async def block_pool(postgres_pool, human_admin_session):
    """Two sequenced prep_samples in ONE sequenced_pool, each with a
    sequenced_sample subtype + a minted sequence_range (4 reads), a shared
    mask_definition, and a PENDING mask_sample gate per sample. Yields the ids +
    a `make_block(members, state)` helper (block + a block work_ticket carrying
    the mask_idx + the cover-map), tracked for FK-reverse cleanup."""
    from qiita_control_plane.repositories.block import (
        add_block_members,
        create_block,
        set_block_state,
        set_block_work_ticket,
    )
    from qiita_control_plane.repositories.mask_definition import mint_mask_definition
    from qiita_control_plane.repositories.sequence_range import mint_sequence_range
    from qiita_control_plane.testing.db_seeds import (
        seed_biosample_with_sequenced_prep_sample,
        seed_sequenced_sample_subtype,
    )

    owner = human_admin_session["principal_idx"]
    suffix = secrets.token_hex(4)

    # Sample A creates the run + pool; sample B is attached to the SAME pool.
    bs_a, prep_a = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=owner
    )
    run_idx, pool_idx, ss_a = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_a,
        owner_idx=owner,
        sequenced_pool_item_id=f"a-{suffix}",
    )
    bs_b, prep_b = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=owner
    )
    ss_b = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequenced_sample"
        "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        prep_b,
        pool_idx,
        f"b-{suffix}",
        owner,
    )

    starts: dict[int, int] = {}
    async with postgres_pool.acquire() as conn:
        for ps in (prep_a, prep_b):
            rng = await mint_sequence_range(
                conn, prep_sample_idx=ps, count=_READS_PER_SAMPLE, principal_idx=owner
            )
            starts[ps] = rng["sequence_idx_start"]
        mask = await mint_mask_definition(
            conn,
            filter_workflow="read-mask",
            filter_version="1.0.0",
            params={"workflow": "read-mask", "s": suffix},
            principal_idx=owner,
        )
    mask_idx = mask["mask_idx"]
    for ps in (prep_a, prep_b):
        await postgres_pool.execute(
            "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
            " VALUES ($1, $2, 'pending')",
            mask_idx,
            ps,
        )

    action_id = f"rmb-e2e-{suffix}"
    version = "1.0.0"
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'block', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', 'active', 'failed')",
        action_id,
        version,
        '{"service": false, "human_roles": ["system_admin"]}',
    )

    created_blocks: list[int] = []

    async def make_block(*, members, state="processing") -> int:
        async with postgres_pool.acquire() as conn, conn.transaction():
            block_idx = await create_block(conn)
            await add_block_members(conn, block_idx=block_idx, members=members)
        wt_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  block_idx, mask_idx)"
            " VALUES ($1, $2, $3, 'block', $4, $5) RETURNING work_ticket_idx",
            action_id,
            version,
            owner,
            block_idx,
            mask_idx,
        )
        async with postgres_pool.acquire() as conn, conn.transaction():
            await set_block_work_ticket(
                conn, block_idx=block_idx, work_ticket_idx=wt_idx
            )
        async with postgres_pool.acquire() as conn:
            await set_block_state(conn, block_idx=block_idx, new_state=state)
        created_blocks.append(block_idx)
        return block_idx

    yield {
        "pool": postgres_pool,
        "owner": owner,
        "prep_a": prep_a,
        "prep_b": prep_b,
        "ss_a": ss_a,
        "ss_b": ss_b,
        "pool_idx": pool_idx,
        "mask_idx": mask_idx,
        "starts": starts,
        "make_block": make_block,
    }

    if created_blocks:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE block_idx = ANY($1::bigint[])",
            created_blocks,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.block WHERE block_idx = ANY($1::bigint[])",
            created_blocks,
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1", action_id
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequenced_sample WHERE idx = ANY($1::bigint[])", [ss_a, ss_b]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", [prep_a, prep_b]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [bs_a, bs_b]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx
    )


async def _metrics_row(pool, ss_idx):
    return await pool.fetchrow(
        "SELECT raw_read_count_r1r2, biological_read_count_r1r2,"
        " quality_filtered_read_count_r1r2 FROM qiita.sequenced_sample WHERE idx = $1",
        ss_idx,
    )


async def _mask_sample_state(pool, mask_idx, prep_sample_idx):
    return await pool.fetchval(
        "SELECT state FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
        mask_idx,
        prep_sample_idx,
    )


async def _export_ticket_or_status(pool, data_plane, *, prep_sample_idx, mask_idx):
    """Call the real create_masked_read_export_ticket in-process; return the HTTP
    status (201 on a minted ticket, or the HTTPException status_code on a gate
    refusal). Deps (_role/_scope) are bypassed on a direct call, as elsewhere."""
    from qiita_control_plane.routes.admin import create_masked_read_export_ticket

    try:
        await create_masked_read_export_ticket(
            body=MaskedReadExportTicketRequest(
                prep_sample_idx=prep_sample_idx, mask_idx=mask_idx
            ),
            pool=pool,
            signing_key=data_plane["secret"],
            _role=None,
            _scope=None,
        )
        return 201
    except HTTPException as exc:
        return exc.status_code


# ---------------------------------------------------------------------------
# Test 1: split sample across two blocks — per-sample reconcile + export gate.
# ---------------------------------------------------------------------------


async def test_block_split_sample_reconciles_and_gates_export(
    block_pool, data_plane, tmp_path
):
    """A sample split across two blocks finalizes only once BOTH complete, with
    metrics rolled up from the real DuckLake aggregate and the count assertion
    satisfied; the masked-read export ticket 409s while a covering block is in
    flight and 201s once the sample is complete.

    Layout: prep_a is whole in block 1; prep_b is split (first half in block 1,
    second half in block 2). After block 1 reconciles, prep_a is done but prep_b
    still owes block 2's reads.
    """
    pool = block_pool["pool"]
    prep_a, prep_b = block_pool["prep_a"], block_pool["prep_b"]
    mask_idx = block_pool["mask_idx"]
    start_a, start_b = block_pool["starts"][prep_a], block_pool["starts"][prep_b]
    half = _READS_PER_SAMPLE // 2

    # Two blocks: block 1 = prep_a whole + prep_b first half; block 2 = prep_b tail.
    block1 = await block_pool["make_block"](
        members=[
            (prep_a, start_a, start_a + _READS_PER_SAMPLE - 1),
            (prep_b, start_b, start_b + half - 1),
        ]
    )
    block2 = await block_pool["make_block"](
        members=[(prep_b, start_b + half, start_b + _READS_PER_SAMPLE - 1)]
    )

    bound = {"mask_idx": mask_idx}

    # --- Block 1: delete-block-mask (0 rows, fresh) -> register-files -> reconcile.
    stage1 = tmp_path / "block1" / "host_filter"
    rows1 = _mask_rows_for(prep_a, start_a) + _mask_rows_for(prep_b, start_b)[:half]
    _write_block_read_mask_parquet(stage1, mask_idx=mask_idx, rows=rows1)
    await _run_block_action(
        pool,
        data_plane,
        name="delete-block-mask",
        block_idx=block1,
        mask_idx=mask_idx,
        bound=bound,
        workspace=stage1,
    )
    await _run_block_action(
        pool,
        data_plane,
        name="register-files",
        block_idx=block1,
        mask_idx=mask_idx,
        bound={**bound, "read_mask_staging_dir": str(stage1)},
        workspace=stage1,
        work_ticket_idx=block1,
    )
    await _run_block_action(
        pool,
        data_plane,
        name="reconcile-block",
        block_idx=block1,
        mask_idx=mask_idx,
        bound=bound,
        workspace=stage1,
    )

    # prep_a's only covering block completed -> finalized; prep_b still owes block 2.
    assert await _mask_sample_state(pool, mask_idx, prep_a) == "completed"
    assert await _mask_sample_state(pool, mask_idx, prep_b) == "pending"
    row_a = await _metrics_row(pool, block_pool["ss_a"])
    assert row_a["raw_read_count_r1r2"] == _EXPECTED_RAW_R1R2
    assert row_a["biological_read_count_r1r2"] == _EXPECTED_BIOLOGICAL_R1R2
    assert row_a["quality_filtered_read_count_r1r2"] == _EXPECTED_QUALITY_FILTERED_R1R2

    # Export gate: prep_a complete -> ticket minted; prep_b partial -> 409.
    assert (
        await _export_ticket_or_status(
            pool, data_plane, prep_sample_idx=prep_a, mask_idx=mask_idx
        )
        == 201
    )
    assert (
        await _export_ticket_or_status(
            pool, data_plane, prep_sample_idx=prep_b, mask_idx=mask_idx
        )
        == 409
    )

    # --- Block 2: the last covering block for prep_b -> it finalizes.
    stage2 = tmp_path / "block2" / "host_filter"
    rows2 = _mask_rows_for(prep_b, start_b)[half:]
    _write_block_read_mask_parquet(stage2, mask_idx=mask_idx, rows=rows2)
    await _run_block_action(
        pool,
        data_plane,
        name="delete-block-mask",
        block_idx=block2,
        mask_idx=mask_idx,
        bound=bound,
        workspace=stage2,
    )
    await _run_block_action(
        pool,
        data_plane,
        name="register-files",
        block_idx=block2,
        mask_idx=mask_idx,
        bound={**bound, "read_mask_staging_dir": str(stage2)},
        workspace=stage2,
        work_ticket_idx=block2,
    )
    await _run_block_action(
        pool,
        data_plane,
        name="reconcile-block",
        block_idx=block2,
        mask_idx=mask_idx,
        bound=bound,
        workspace=stage2,
    )

    # prep_b now complete: all 4 reads registered (count assertion held inside
    # reconcile), gate flipped, metrics rolled up from the DuckLake aggregate.
    assert await _mask_sample_state(pool, mask_idx, prep_b) == "completed"
    assert (
        _count_read_mask_rows(data_plane, mask_idx=mask_idx, prep_sample_idx=prep_b)
        == _READS_PER_SAMPLE
    )
    row_b = await _metrics_row(pool, block_pool["ss_b"])
    assert row_b["raw_read_count_r1r2"] == _EXPECTED_RAW_R1R2
    assert row_b["biological_read_count_r1r2"] == _EXPECTED_BIOLOGICAL_R1R2
    assert row_b["quality_filtered_read_count_r1r2"] == _EXPECTED_QUALITY_FILTERED_R1R2

    # Export gate now allows prep_b too.
    assert (
        await _export_ticket_or_status(
            pool, data_plane, prep_sample_idx=prep_b, mask_idx=mask_idx
        )
        == 201
    )


# ---------------------------------------------------------------------------
# Test 2: idempotent block replace — delete-then-register self-cleans; teeth.
# ---------------------------------------------------------------------------


async def test_block_delete_then_register_leaves_no_duplicate_rows(
    block_pool, data_plane, tmp_path
):
    """delete-block-mask before register-files makes a block re-run exact: the
    footprint is deleted then re-registered, leaving exactly one logical copy.

    Also proves the no-dup assertion has teeth: register-files twice WITHOUT the
    delete DOES duplicate the block's rows (append-only read_mask, no unique key),
    and a subsequent delete-block-mask cleans that duplicate back to the exact
    footprint. A single whole-sample block keeps the assertions simple.
    """
    pool = block_pool["pool"]
    prep_a = block_pool["prep_a"]
    mask_idx = block_pool["mask_idx"]
    start_a = block_pool["starts"][prep_a]
    n = _READS_PER_SAMPLE

    block = await block_pool["make_block"](members=[(prep_a, start_a, start_a + n - 1)])
    bound = {"mask_idx": mask_idx}
    rows = _mask_rows_for(prep_a, start_a)

    def _stage(tag: str) -> Path:
        # register-files MOVES the parquet out of staging, so each registration
        # needs its own fresh copy — exactly as a resubmit re-emits from host_filter.
        d = tmp_path / tag / "host_filter"
        _write_block_read_mask_parquet(d, mask_idx=mask_idx, rows=rows)
        return d

    # Each register-files call names a distinct lake file by work_ticket_idx (the
    # DP refuses to overwrite), exactly as a resubmit's fresh ticket would.
    async def _delete_then_register(tag: str, wt: int):
        d = _stage(tag)
        await _run_block_action(
            pool,
            data_plane,
            name="delete-block-mask",
            block_idx=block,
            mask_idx=mask_idx,
            bound=bound,
            workspace=d,
        )
        await _run_block_action(
            pool,
            data_plane,
            name="register-files",
            block_idx=block,
            mask_idx=mask_idx,
            bound={**bound, "read_mask_staging_dir": str(d)},
            workspace=d,
            work_ticket_idx=wt,
        )

    async def _register_only(tag: str, wt: int):
        d = _stage(tag)
        await _run_block_action(
            pool,
            data_plane,
            name="register-files",
            block_idx=block,
            mask_idx=mask_idx,
            bound={**bound, "read_mask_staging_dir": str(d)},
            workspace=d,
            work_ticket_idx=wt,
        )

    def count() -> int:
        return _count_read_mask_rows(
            data_plane, mask_idx=mask_idx, prep_sample_idx=prep_a
        )

    # First run: delete (0 rows, fresh) then register -> exactly n rows.
    await _delete_then_register("run1", wt=block * 10 + 1)
    assert count() == n

    # Re-run the block via delete-then-register -> still exactly n (self-clean).
    await _delete_then_register("run2", wt=block * 10 + 2)
    assert count() == n

    # Teeth: register WITHOUT the delete -> the block's rows duplicate.
    await _register_only("run3-nodelete", wt=block * 10 + 3)
    assert count() == 2 * n

    # delete-block-mask cleans the whole footprint (both copies); the following
    # register re-lays exactly one copy.
    await _delete_then_register("run4-recover", wt=block * 10 + 4)
    assert count() == n
