"""Cross-component integration tests for the `align` workflow's `action:` tail
(`delete-alignment-block` → `register-files` → `reconcile-alignment-block`)
against a real control plane, a real data-plane Flight/DuckLake process, and the
integration Postgres. The alignment analog of `test_read_mask_block_e2e.py`.

The native compute step (`align_sharded`) is NOT run here — it needs miint/rype +
real sharded indexes + real masked reads (system-tier; `test_sharded_alignment.py`
drives it directly against real miint). Instead we pre-stage a multi-sample
`alignment.parquet` in `align_sharded`'s exact output shape (the 25-column DuckLake
`alignment` schema, keyed by `alignment_idx`, per-row `prep_sample_idx`, and
DELIBERATELY multiple rows per read to model cross-shard + PE multiplicity) and
drive the REAL runner adapter `_run_action_primitive` for the three `action:`
entries — parsed in their shipped order from `workflows/align/1.0.0.yaml` — so the
real LIBRARY primitives, the real data-plane `register_files` /
`delete_alignment_block` DoActions, and the real DuckLake catalog are exercised
end to end.

Two things this milestone introduced are otherwise unproven end-to-end:

1. PER-SAMPLE ALIGNMENT RECONCILE (no count-assertion). A sample split across two
   blocks must finalize its `alignment_sample` gate ONLY once BOTH covering blocks
   complete — and, unlike read-mask reconcile, WITHOUT a row-count assertion
   (alignment rows are not 1:1 with reads: a read routed to K shards emits K rows,
   a PE read emits one row per mate). This file stages 2 rows per read to prove the
   gate flips on block completion regardless of row multiplicity.

2. IDEMPOTENT BLOCK REPLACE. `delete-alignment-block` runs before `register-files`,
   so a block re-run deletes its exact footprint (its member sub-ranges under the
   ticket alignment_idx, feature_idx-agnostic) then re-registers — leaving exactly
   one logical copy per (read, feature). This proves delete-then-register
   self-cleans, that register-twice WITHOUT the delete WOULD duplicate, and that a
   subsequent delete-alignment-block cleans the duplicate back to the exact
   footprint.

Shared fixtures (`data_plane`, `postgres_pool`, `human_admin_session`,
`ducklake_connect`) live in conftest.py.
"""

from __future__ import annotations

import secrets
from pathlib import Path

import duckdb
import pytest
from qiita_common.api_paths import LOOPBACK_HOST

from conftest import ducklake_connect

_ALIGN_YAML_PATH = (
    Path(__file__).parent.parent.parent / "workflows" / "align" / "1.0.0.yaml"
)

# Reads per sample. Small — the DuckLake writes are cheap and the exact row count
# (reads × rows-per-read) is asserted, so keep it tiny.
_READS_PER_SAMPLE = 4

# Alignment rows emitted per read — models multiplicity (a read aligning to two
# shards' features). The delete/register footprint is feature_idx-agnostic, so a
# block's footprint is reads × this, and reconcile does NOT count-assert against it.
_ROWS_PER_READ = 2
_FEATURE_IDS = (10, 11)  # the two feature_idx a read aligns to (distinct per shard)

# The DuckLake `alignment` table schema, verbatim from
# qiita-data-plane/src/ducklake.rs::ensure_alignment_tables — the staged
# alignment.parquet must match it exactly for ducklake_add_data_files to register.
_ALIGNMENT_PARQUET_DDL = """
    alignment_idx    BIGINT NOT NULL,
    prep_sample_idx  BIGINT NOT NULL,
    sequence_idx     BIGINT NOT NULL,
    feature_idx      BIGINT NOT NULL,
    mate_feature_idx BIGINT,
    flags            USMALLINT,
    position         BIGINT,
    stop_position    BIGINT,
    mapq             UTINYINT,
    cigar            VARCHAR,
    mate_position    BIGINT,
    template_length  BIGINT,
    tag_as           BIGINT,
    tag_xs           BIGINT,
    tag_ys           BIGINT,
    tag_xn           BIGINT,
    tag_xm           BIGINT,
    tag_xo           BIGINT,
    tag_xg           BIGINT,
    tag_nm           BIGINT,
    tag_yt           VARCHAR,
    tag_md           VARCHAR,
    tag_sa           VARCHAR
"""


def _align_action_entry(name: str):
    """Parse the shipped align YAML and return one `action:` WorkflowAction entry by
    name. Driving whatever the YAML declares (not hand-constructed entries) means a
    future reorder / rename is reflected here automatically."""
    import yaml
    from qiita_common.actions import ActionDefinition, WorkflowAction

    data = yaml.safe_load(_ALIGN_YAML_PATH.read_text())
    action = ActionDefinition.model_validate(data)
    for entry in action.steps:
        if isinstance(entry, WorkflowAction) and entry.name == name:
            return entry
    raise AssertionError(f"no action entry named {name!r} in align YAML")


def _write_alignment_parquet(
    staging_dir: Path, *, alignment_idx: int, members: list[tuple[int, int, int]]
) -> Path:
    """Materialize a block's `alignment.parquet` under `staging_dir` in
    align_sharded's output shape (the DuckLake `alignment` schema) and return the
    path. For each member `(prep_sample_idx, seq_start, seq_stop)`, emit
    `_ROWS_PER_READ` rows per sequence_idx in [seq_start, seq_stop] — one per
    _FEATURE_IDS entry — so the block carries read multiplicity."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    path = staging_dir / "alignment.parquet"
    rows: list[tuple] = []
    for prep_sample_idx, seq_start, seq_stop in members:
        for seq_idx in range(seq_start, seq_stop + 1):
            for feature_idx in _FEATURE_IDS:
                # (alignment_idx, prep_sample_idx, sequence_idx, feature_idx,
                #  mate_feature_idx, flags, position) + NULL tail.
                rows.append(
                    (
                        alignment_idx,
                        prep_sample_idx,
                        seq_idx,
                        feature_idx,
                        None,  # mate_feature_idx
                        0,  # flags
                        100,  # position
                    )
                )
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"CREATE TABLE a ({_ALIGNMENT_PARQUET_DDL})")
        conn.executemany(
            "INSERT INTO a (alignment_idx, prep_sample_idx, sequence_idx, feature_idx,"
            " mate_feature_idx, flags, position) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.execute(
            "COPY (SELECT * FROM a"
            " ORDER BY alignment_idx, prep_sample_idx, sequence_idx, feature_idx, position)"
            f" TO '{path}' (FORMAT PARQUET)"
        )
    return path


def _data_plane_url(data_plane) -> str:
    return f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"


def _count_alignment_rows(
    data_plane, *, alignment_idx: int, prep_sample_idx: int
) -> int:
    """Count DuckLake `alignment` rows for an (alignment, prep_sample) — the table
    is not Flight-reachable (a sink this milestone), so read it via a direct
    DuckLake conn (mirrors the read-mask e2e's read_mask read-back)."""
    conn = ducklake_connect(data_plane["data_path"])
    try:
        (n,) = conn.execute(
            "SELECT count(*) FROM qiita_lake.alignment"
            " WHERE alignment_idx = ? AND prep_sample_idx = ?",
            [alignment_idx, prep_sample_idx],
        ).fetchone()
        return n
    finally:
        conn.close()


async def _run_align_action(
    postgres_pool,
    data_plane,
    *,
    name,
    block_idx,
    alignment_idx,
    bound,
    workspace,
    work_ticket_idx=0,
):
    """Drive the REAL runner adapter for one align `action:` entry, block-scoped.
    `work_ticket_idx` names register-files' lake file (`wt{idx}-alignment.parquet`),
    so each registration needs a distinct value (the DP refuses to overwrite)."""
    from qiita_control_plane.runner import _run_action_primitive

    await _run_action_primitive(
        postgres_pool,
        _align_action_entry(name),
        bound,
        workspace,
        {"kind": "block", "block_idx": block_idx},
        work_ticket_idx=work_ticket_idx,
        signing_key=data_plane["secret"],
        data_plane_url=_data_plane_url(data_plane),
    )


@pytest.fixture
async def align_block_pool(postgres_pool, human_admin_session):
    """Two sequenced prep_samples in ONE sequenced_pool, each with a
    sequenced_sample subtype + a minted sequence_range (4 reads), a minted
    `alignment_definition`, and a PENDING `alignment_sample` gate per sample. Yields
    the ids + a `make_block(members, state)` helper (block + a block work_ticket
    carrying the alignment_idx + the cover-map), tracked for FK-reverse cleanup."""
    from qiita_control_plane.repositories.alignment_definition import (
        mint_alignment_definition,
    )
    from qiita_control_plane.repositories.block import (
        add_block_members,
        create_alignment_sample_pending,
        create_block,
        set_block_state,
        set_block_work_ticket,
    )
    from qiita_control_plane.repositories.sequence_range import mint_sequence_range
    from qiita_control_plane.testing.db_seeds import (
        seed_biosample_with_sequenced_prep_sample,
        seed_sequenced_sample_subtype,
    )

    owner = human_admin_session["principal_idx"]
    suffix = secrets.token_hex(4)

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
                conn,
                prep_sample_idx=ps,
                count=_READS_PER_SAMPLE,
                principal_idx=owner,
                work_ticket_idx=None,
            )
            starts[ps] = rng["sequence_idx_start"]
        # A distinct alignment identity per test run (suffix in the params so the
        # canonical hash is unique — avoids colliding with a prior run's row).
        align_row = await mint_alignment_definition(
            conn,
            params={
                "reference_idx": 1,
                "aligner": "minimap2",
                "mask_idx": 1,
                "shard_ids": [0, 1],
                "s": suffix,
            },
            principal_idx=owner,
        )
    alignment_idx = align_row["alignment_idx"]
    async with postgres_pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=[prep_a, prep_b]
        )

    action_id = f"align-e2e-{suffix}"
    version = "1.0.0"
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'block', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', NULL, NULL)",
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
            "  block_idx, alignment_idx)"
            " VALUES ($1, $2, $3, 'block', $4, $5) RETURNING work_ticket_idx",
            action_id,
            version,
            owner,
            block_idx,
            alignment_idx,
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
        "pool_idx": pool_idx,
        "alignment_idx": alignment_idx,
        "starts": starts,
        "make_block": make_block,
    }

    # FK-reverse Postgres cleanup. The DuckLake `alignment` rows we registered are
    # left as harmless orphans (each test run uses a unique alignment_idx, and the
    # catalog is reset between integration phases) — the same discipline the
    # read-mask block e2e uses for its read_mask rows.
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
        "DELETE FROM qiita.alignment_sample WHERE alignment_idx = $1", alignment_idx
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
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )


async def _alignment_sample_state(pool, alignment_idx, prep_sample_idx):
    return await pool.fetchval(
        "SELECT state FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        alignment_idx,
        prep_sample_idx,
    )


# ---------------------------------------------------------------------------
# Test 1: split sample across two blocks — per-sample reconcile, no count-assert.
# ---------------------------------------------------------------------------


async def test_align_block_split_sample_reconciles_gate(
    align_block_pool, data_plane, tmp_path
):
    """A sample split across two blocks finalizes its alignment_sample gate only
    once BOTH complete, with the alignment rows keyed by alignment_idx and NO
    count-assertion (each read carries _ROWS_PER_READ rows).

    Layout: prep_a is whole in block 1; prep_b is split (first half in block 1,
    second half in block 2). After block 1 reconciles, prep_a is done but prep_b
    still owes block 2's reads.
    """
    pool = align_block_pool["pool"]
    prep_a, prep_b = align_block_pool["prep_a"], align_block_pool["prep_b"]
    alignment_idx = align_block_pool["alignment_idx"]
    start_a, start_b = (
        align_block_pool["starts"][prep_a],
        align_block_pool["starts"][prep_b],
    )
    half = _READS_PER_SAMPLE // 2

    # block 1 = prep_a whole + prep_b first half; block 2 = prep_b tail.
    block1 = await align_block_pool["make_block"](
        members=[
            (prep_a, start_a, start_a + _READS_PER_SAMPLE - 1),
            (prep_b, start_b, start_b + half - 1),
        ]
    )
    block2 = await align_block_pool["make_block"](
        members=[(prep_b, start_b + half, start_b + _READS_PER_SAMPLE - 1)]
    )

    bound = {"alignment_idx": alignment_idx}

    # --- Block 1: delete-alignment-block (0 rows, fresh) -> register -> reconcile.
    stage1 = tmp_path / "block1" / "align_sharded"
    members1 = [
        (prep_a, start_a, start_a + _READS_PER_SAMPLE - 1),
        (prep_b, start_b, start_b + half - 1),
    ]
    _write_alignment_parquet(stage1, alignment_idx=alignment_idx, members=members1)
    await _run_align_action(
        pool,
        data_plane,
        name="delete-alignment-block",
        block_idx=block1,
        alignment_idx=alignment_idx,
        bound=bound,
        workspace=stage1,
    )
    await _run_align_action(
        pool,
        data_plane,
        name="register-files",
        block_idx=block1,
        alignment_idx=alignment_idx,
        bound={**bound, "alignment_staging_dir": str(stage1)},
        workspace=stage1,
        work_ticket_idx=block1,
    )
    await _run_align_action(
        pool,
        data_plane,
        name="reconcile-alignment-block",
        block_idx=block1,
        alignment_idx=alignment_idx,
        bound=bound,
        workspace=stage1,
    )

    # prep_a's only covering block completed -> finalized; prep_b still owes block 2.
    assert await _alignment_sample_state(pool, alignment_idx, prep_a) == "completed"
    assert await _alignment_sample_state(pool, alignment_idx, prep_b) == "pending"
    # prep_a: all 4 reads, _ROWS_PER_READ each. prep_b: half its reads so far.
    assert (
        _count_alignment_rows(
            data_plane, alignment_idx=alignment_idx, prep_sample_idx=prep_a
        )
        == _READS_PER_SAMPLE * _ROWS_PER_READ
    )
    assert (
        _count_alignment_rows(
            data_plane, alignment_idx=alignment_idx, prep_sample_idx=prep_b
        )
        == half * _ROWS_PER_READ
    )

    # --- Block 2: the last covering block for prep_b -> it finalizes.
    stage2 = tmp_path / "block2" / "align_sharded"
    members2 = [(prep_b, start_b + half, start_b + _READS_PER_SAMPLE - 1)]
    _write_alignment_parquet(stage2, alignment_idx=alignment_idx, members=members2)
    await _run_align_action(
        pool,
        data_plane,
        name="delete-alignment-block",
        block_idx=block2,
        alignment_idx=alignment_idx,
        bound=bound,
        workspace=stage2,
    )
    await _run_align_action(
        pool,
        data_plane,
        name="register-files",
        block_idx=block2,
        alignment_idx=alignment_idx,
        bound={**bound, "alignment_staging_dir": str(stage2)},
        workspace=stage2,
        work_ticket_idx=block2,
    )
    await _run_align_action(
        pool,
        data_plane,
        name="reconcile-alignment-block",
        block_idx=block2,
        alignment_idx=alignment_idx,
        bound=bound,
        workspace=stage2,
    )

    # prep_b now complete: gate flipped, all its reads registered (no count-assert).
    assert await _alignment_sample_state(pool, alignment_idx, prep_b) == "completed"
    assert (
        _count_alignment_rows(
            data_plane, alignment_idx=alignment_idx, prep_sample_idx=prep_b
        )
        == _READS_PER_SAMPLE * _ROWS_PER_READ
    )


# ---------------------------------------------------------------------------
# Test 2: idempotent block replace — delete-then-register self-cleans; teeth.
# ---------------------------------------------------------------------------


async def test_align_block_delete_then_register_leaves_no_duplicate_rows(
    align_block_pool, data_plane, tmp_path
):
    """delete-alignment-block before register-files makes a block re-run exact: the
    footprint is deleted then re-registered, leaving exactly one logical copy per
    (read, feature).

    Also proves the no-dup assertion has teeth: register-files twice WITHOUT the
    delete DOES duplicate (append-only alignment, no unique key), and a subsequent
    delete-alignment-block cleans that duplicate back to the exact footprint. The
    footprint is feature_idx-agnostic, so all _ROWS_PER_READ rows of each read go.
    """
    pool = align_block_pool["pool"]
    prep_a = align_block_pool["prep_a"]
    alignment_idx = align_block_pool["alignment_idx"]
    start_a = align_block_pool["starts"][prep_a]
    n = _READS_PER_SAMPLE
    footprint = n * _ROWS_PER_READ

    block = await align_block_pool["make_block"](
        members=[(prep_a, start_a, start_a + n - 1)]
    )
    bound = {"alignment_idx": alignment_idx}
    members = [(prep_a, start_a, start_a + n - 1)]

    def _stage(tag: str) -> Path:
        # register-files MOVES the parquet out of staging, so each registration
        # needs its own fresh copy — exactly as a resubmit re-emits from align_sharded.
        d = tmp_path / tag / "align_sharded"
        _write_alignment_parquet(d, alignment_idx=alignment_idx, members=members)
        return d

    async def _delete_then_register(tag: str, wt: int):
        d = _stage(tag)
        await _run_align_action(
            pool,
            data_plane,
            name="delete-alignment-block",
            block_idx=block,
            alignment_idx=alignment_idx,
            bound=bound,
            workspace=d,
        )
        await _run_align_action(
            pool,
            data_plane,
            name="register-files",
            block_idx=block,
            alignment_idx=alignment_idx,
            bound={**bound, "alignment_staging_dir": str(d)},
            workspace=d,
            work_ticket_idx=wt,
        )

    async def _register_only(tag: str, wt: int):
        d = _stage(tag)
        await _run_align_action(
            pool,
            data_plane,
            name="register-files",
            block_idx=block,
            alignment_idx=alignment_idx,
            bound={**bound, "alignment_staging_dir": str(d)},
            workspace=d,
            work_ticket_idx=wt,
        )

    def count() -> int:
        return _count_alignment_rows(
            data_plane, alignment_idx=alignment_idx, prep_sample_idx=prep_a
        )

    # First run: delete (0 rows, fresh) then register -> exactly the footprint.
    await _delete_then_register("run1", wt=block * 10 + 1)
    assert count() == footprint

    # Re-run the block via delete-then-register -> still exactly the footprint.
    await _delete_then_register("run2", wt=block * 10 + 2)
    assert count() == footprint

    # Teeth: register WITHOUT the delete -> the block's rows duplicate.
    await _register_only("run3-nodelete", wt=block * 10 + 3)
    assert count() == 2 * footprint

    # delete-alignment-block cleans the whole footprint (both copies); the
    # following register re-lays exactly one copy.
    await _delete_then_register("run4-recover", wt=block * 10 + 4)
    assert count() == footprint
