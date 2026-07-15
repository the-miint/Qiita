"""DB-tier tests for the feature-table (OGU) runner resolver
(`_resolve_feature_table_bindings`): derive/verify reference, gate cohort
completeness, and stage the feature->genome map Parquet.
"""

import uuid

import pyarrow.parquet as pq
import pytest
from qiita_common.backend_failure import BackendFailure, FailureKind

from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.repositories.block import (
    create_alignment_sample_pending,
    finalize_alignment_sample,
)
from qiita_control_plane.runner import (
    GENOME_MAP_PATH_BINDING,
    _resolve_feature_table_bindings,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db


async def _seed_scenario(pool, *, n_features=2, n_samples=2, completed=2):
    """Seed a reference (n_features, each its own genome + membership), an
    alignment against it, n_samples sequenced prep_samples, and their
    alignment_sample gate rows (the first `completed` flipped to 'completed').

    Returns a dict with reference_idx, alignment_idx, prep_sample_idxs, pairs
    (feature_idx, genome_idx), plus biosample/genome/feature ids for cleanup.
    """
    principal_idx = await seed_user_principal(pool, prefix="ft-res", suffix=uuid.uuid4().hex[:8])
    reference_idx = await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', false, $2) RETURNING reference_idx",
        f"ft-res-{uuid.uuid4()}",
        principal_idx,
    )
    pairs: list[tuple[int, int]] = []
    for _ in range(n_features):
        feature_idx = await pool.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES ($1) RETURNING feature_idx",
            uuid.uuid4(),
        )
        genome_idx = await pool.fetchval(
            "INSERT INTO qiita.genome (source, source_id) VALUES ('refseq', $1)"
            " RETURNING genome_idx",
            str(uuid.uuid4()),
        )
        await pool.execute(
            "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
            feature_idx,
            genome_idx,
        )
        await pool.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx) VALUES ($1, $2)",
            reference_idx,
            feature_idx,
        )
        pairs.append((feature_idx, genome_idx))

    async with pool.acquire() as conn:
        row = await mint_alignment_definition(
            conn,
            params={
                "reference_idx": reference_idx,
                "aligner": "minimap2",
                "mask_idx": 1,
                "shard_ids": [0],
            },
            principal_idx=principal_idx,
        )
    alignment_idx = row["alignment_idx"]

    biosample_idxs: list[int] = []
    prep_sample_idxs: list[int] = []
    for _ in range(n_samples):
        bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(
            pool, owner_idx=principal_idx
        )
        biosample_idxs.append(bs_idx)
        prep_sample_idxs.append(ps_idx)

    async with pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=prep_sample_idxs
        )
        for ps_idx in prep_sample_idxs[:completed]:
            await finalize_alignment_sample(
                conn, alignment_idx=alignment_idx, prep_sample_idx=ps_idx
            )

    return {
        "reference_idx": reference_idx,
        "alignment_idx": alignment_idx,
        "prep_sample_idxs": prep_sample_idxs,
        "pairs": pairs,
        "biosample_idxs": biosample_idxs,
        "genome_idxs": [g for _f, g in pairs],
        "feature_idxs": [f for f, _g in pairs],
        "principal_idx": principal_idx,
    }


async def _cleanup(pool, s):
    # alignment_definition CASCADEs alignment_sample; then unwind FK order.
    await pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", s["alignment_idx"]
    )
    await pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", s["prep_sample_idxs"]
    )
    await pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", s["biosample_idxs"]
    )
    await pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", s["reference_idx"]
    )
    await pool.execute(
        "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])", s["feature_idxs"]
    )
    await pool.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", s["feature_idxs"]
    )
    await pool.execute(
        "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])", s["genome_idxs"]
    )
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", s["reference_idx"])
    # Principal last — biosample/reference/alignment_definition all RESTRICT-ref it.
    await pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", s["principal_idx"])
    await pool.execute("DELETE FROM qiita.principal WHERE idx = $1", s["principal_idx"])


async def test_resolver_happy_path_stages_genome_map(postgres_pool, tmp_path):
    s = await _seed_scenario(postgres_pool, completed=2)
    try:
        result = await _resolve_feature_table_bindings(
            postgres_pool,
            action_context={
                "alignment_idx": s["alignment_idx"],
                "prep_sample_idx": s["prep_sample_idxs"],
                "coverage_threshold": 0.01,
            },
            reference_idx=s["reference_idx"],
            workspace=tmp_path,
        )
        path = result[GENOME_MAP_PATH_BINDING]
        assert path.exists()
        table = pq.read_table(str(path))
        got = set(
            zip(table.column("feature_idx").to_pylist(), table.column("genome_idx").to_pylist())
        )
        assert got == set(s["pairs"])
    finally:
        await _cleanup(postgres_pool, s)


async def test_resolver_incomplete_cohort_raises(postgres_pool, tmp_path):
    s = await _seed_scenario(postgres_pool, n_samples=2, completed=1)  # one sample still pending
    try:
        with pytest.raises(BackendFailure) as exc:
            await _resolve_feature_table_bindings(
                postgres_pool,
                action_context={
                    "alignment_idx": s["alignment_idx"],
                    "prep_sample_idx": s["prep_sample_idxs"],
                },
                reference_idx=s["reference_idx"],
                workspace=tmp_path,
            )
        assert exc.value.kind == FailureKind.BAD_INPUT
    finally:
        await _cleanup(postgres_pool, s)


async def test_resolver_reference_mismatch_raises(postgres_pool, tmp_path):
    s = await _seed_scenario(postgres_pool, completed=2)
    try:
        with pytest.raises(BackendFailure) as exc:
            await _resolve_feature_table_bindings(
                postgres_pool,
                action_context={
                    "alignment_idx": s["alignment_idx"],
                    "prep_sample_idx": s["prep_sample_idxs"],
                },
                reference_idx=s["reference_idx"] + 999_999,  # not the alignment's reference
                workspace=tmp_path,
            )
        assert exc.value.kind == FailureKind.BAD_INPUT
    finally:
        await _cleanup(postgres_pool, s)


async def test_resolver_unknown_alignment_raises(postgres_pool, tmp_path):
    with pytest.raises(BackendFailure) as exc:
        await _resolve_feature_table_bindings(
            postgres_pool,
            action_context={"alignment_idx": 999_999_999, "prep_sample_idx": [1]},
            reference_idx=1,
            workspace=tmp_path,
        )
    assert exc.value.kind == FailureKind.BAD_INPUT


async def test_resolver_cohort_member_with_no_gate_row_raises(postgres_pool, tmp_path):
    """A cohort member with NO alignment_sample row at all (never part of this
    alignment) is 'incomplete' just like a pending one — refuse to build."""
    s = await _seed_scenario(postgres_pool, completed=2)  # every seeded sample completed
    try:
        with pytest.raises(BackendFailure) as exc:
            await _resolve_feature_table_bindings(
                postgres_pool,
                action_context={
                    "alignment_idx": s["alignment_idx"],
                    # a positive prep_sample_idx that has no alignment_sample row
                    "prep_sample_idx": [*s["prep_sample_idxs"], 999_999_999],
                },
                reference_idx=s["reference_idx"],
                workspace=tmp_path,
            )
        assert exc.value.kind == FailureKind.BAD_INPUT
    finally:
        await _cleanup(postgres_pool, s)


@pytest.mark.parametrize(
    "action_context",
    [
        {"prep_sample_idx": [1]},  # missing alignment_idx
        {"alignment_idx": 0, "prep_sample_idx": [1]},  # non-positive alignment_idx
        {"alignment_idx": True, "prep_sample_idx": [1]},  # bool masquerading as int
        {"alignment_idx": 1, "prep_sample_idx": []},  # empty cohort
        {"alignment_idx": 1, "prep_sample_idx": [1, -2]},  # non-positive member
        {"alignment_idx": 1, "prep_sample_idx": [1, True]},  # bool member
        {"alignment_idx": 1, "prep_sample_idx": "nope"},  # wrong type
    ],
)
async def test_resolver_bad_action_context_raises(postgres_pool, tmp_path, action_context):
    with pytest.raises(BackendFailure) as exc:
        await _resolve_feature_table_bindings(
            postgres_pool,
            action_context=action_context,
            reference_idx=1,
            workspace=tmp_path,
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
