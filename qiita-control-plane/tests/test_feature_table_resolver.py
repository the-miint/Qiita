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
from qiita_control_plane.runner._feature_table import (
    DENOVO_GENOME_MAP_PATH_BINDING,
    DENOVO_PROCESSING_IDX_BINDING,
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


# ---------------------------------------------------------------------------
# The de novo arm: pairing the two alignments, and the per-sample arm gate.
# ---------------------------------------------------------------------------


async def _seed_denovo(pool, s, *, mask_idx=1, assembly_states=None, aligned=None, minted=True):
    """Add a de novo alignment over `s`'s samples, an assembly run, and both gates.

    `assembly_states` maps prep_sample_idx -> assembly_sample state (absent key ⇒ no
    row at all, which is a distinct case from every state). `aligned` is the subset
    that reaches 'completed' in the de novo alignment_sample gate; None ⇒ every
    sample whose assembly state is 'completed'. `minted=False` leaves one membership
    row's genome_idx NULL, which is what the map's completeness check refuses.

    Returns the added ids so `_cleanup_denovo` can unwind them.
    """
    processing_idx = await pool.fetchval(
        "INSERT INTO qiita.processing (params_hash, workflow, version, params)"
        " VALUES ($1, 'long-read-assembly', '1.0.0', '{}'::jsonb) RETURNING processing_idx",
        uuid.uuid4().bytes + uuid.uuid4().bytes,
    )
    async with pool.acquire() as conn:
        row = await mint_alignment_definition(
            conn,
            params={
                "subject": "assembly",
                "aligner": "minimap2",
                "mask_idx": mask_idx,
                "processing_idx": processing_idx,
            },
            principal_idx=s["principal_idx"],
        )
    denovo_alignment_idx = row["alignment_idx"]

    assembly_states = (
        {ps: "completed" for ps in s["prep_sample_idxs"]}
        if assembly_states is None
        else assembly_states
    )
    for ps_idx, state in assembly_states.items():
        # 'invalidated' carries a biconditional CHECK on its three provenance
        # columns, so the withdrawal has to be seeded as a real withdrawal.
        invalidation = (
            (None, None, None)
            if state != "invalidated"
            else ("now()", s["principal_idx"], "seeded withdrawal")
        )
        await pool.execute(
            "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state,"
            " invalidated_at, invalidated_by_idx, invalidation_reason)"
            " VALUES ($1, $2, $3, CASE WHEN $4::text IS NULL THEN NULL ELSE now() END, $5, $6)",
            processing_idx,
            ps_idx,
            state,
            invalidation[0],
            invalidation[1],
            invalidation[2],
        )

    # One contig per assembled sample, each its own qiita genome — the shape PR 1's
    # mint produces, which is what the de novo map reads.
    contigs: list[tuple[int, int]] = []
    for ps_idx, state in assembly_states.items():
        if state != "completed":
            continue
        feature_idx = await pool.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES ($1) RETURNING feature_idx",
            uuid.uuid4(),
        )
        genome_idx = await pool.fetchval(
            "INSERT INTO qiita.genome (source, source_id, prep_sample_idx)"
            " VALUES ('qiita', $1, $2) RETURNING genome_idx",
            str(uuid.uuid4()),
            ps_idx,
        )
        await pool.execute(
            "INSERT INTO qiita.assembly_membership"
            " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx, genome_idx)"
            " VALUES ($1, $2, 'MAG', 'bin.1', $3, $4)",
            ps_idx,
            processing_idx,
            feature_idx,
            genome_idx if minted else None,
        )
        contigs.append((feature_idx, genome_idx))

    expected = [ps for ps, st in assembly_states.items() if st == "completed"]
    to_align = expected if aligned is None else aligned
    if to_align:
        async with pool.acquire() as conn, conn.transaction():
            await create_alignment_sample_pending(
                conn, alignment_idx=denovo_alignment_idx, prep_sample_idxs=list(to_align)
            )
            for ps_idx in to_align:
                await finalize_alignment_sample(
                    conn, alignment_idx=denovo_alignment_idx, prep_sample_idx=ps_idx
                )

    return {
        "denovo_alignment_idx": denovo_alignment_idx,
        "processing_idx": processing_idx,
        "contigs": contigs,
    }


async def _cleanup_denovo(pool, d):
    await pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1",
        d["denovo_alignment_idx"],
    )
    await pool.execute(
        "DELETE FROM qiita.assembly_membership WHERE processing_idx = $1", d["processing_idx"]
    )
    await pool.execute(
        "DELETE FROM qiita.assembly_sample WHERE processing_idx = $1", d["processing_idx"]
    )
    await pool.execute(
        "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])",
        [g for _f, g in d["contigs"]],
    )
    await pool.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
        [f for f, _g in d["contigs"]],
    )
    await pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = $1", d["processing_idx"]
    )


def _context(s, d=None, **extra):
    ctx = {
        "alignment_idx": s["alignment_idx"],
        "prep_sample_idx": s["prep_sample_idxs"],
        "coverage_threshold": 0.01,
    }
    if d is not None:
        ctx["denovo_alignment_idx"] = d["denovo_alignment_idx"]
    return ctx | extra


async def test_resolver_stages_the_denovo_map_keyed_by_sample(postgres_pool, tmp_path):
    """The happy combined path. The de novo map is THREE columns where the reference
    one is two, and the extra column is `prep_sample_idx` — a contig is
    content-addressed, so without it two samples' rows on a shared contig are
    indistinguishable and each sample's reads split across both genomes.
    """
    s = await _seed_scenario(postgres_pool, completed=2)
    d = await _seed_denovo(postgres_pool, s)
    try:
        result = await _resolve_feature_table_bindings(
            postgres_pool,
            action_context=_context(s, d),
            reference_idx=s["reference_idx"],
            workspace=tmp_path,
        )
        path = result[DENOVO_GENOME_MAP_PATH_BINDING]
        table = pq.read_table(str(path))
        assert table.column_names == ["prep_sample_idx", "feature_idx", "genome_idx"]
        assert set(
            zip(table.column("feature_idx").to_pylist(), table.column("genome_idx").to_pylist())
        ) == set(d["contigs"])
        # The assembly run reaches the job as a scalar, read off the alignment rather
        # than named by the caller.
        assert result[DENOVO_PROCESSING_IDX_BINDING] == d["processing_idx"]
        # The reference arm is untouched by any of it.
        assert result[GENOME_MAP_PATH_BINDING].exists()
    finally:
        await _cleanup_denovo(postgres_pool, d)
        await _cleanup(postgres_pool, s)


async def test_resolver_without_the_denovo_key_binds_nothing_extra(postgres_pool, tmp_path):
    """Absent `denovo_alignment_idx` the resolver is the reference-only one — no de
    novo bindings at all, so the step is dispatched exactly as it was before the arm
    existed. The control for every assertion above."""
    s = await _seed_scenario(postgres_pool, completed=2)
    try:
        result = await _resolve_feature_table_bindings(
            postgres_pool,
            action_context=_context(s),
            reference_idx=s["reference_idx"],
            workspace=tmp_path,
        )
        assert set(result) == {GENOME_MAP_PATH_BINDING}
    finally:
        await _cleanup(postgres_pool, s)


async def test_resolver_refuses_a_reference_alignment_as_the_denovo_arm(postgres_pool, tmp_path):
    """Passing the SAME alignment for both arms is caught on `params.subject`. It has
    to be: after precedence every read the "de novo" arm placed is removed from the
    reference arm, so an unchecked self-pairing returns an empty table rather than an
    error."""
    s = await _seed_scenario(postgres_pool, completed=2)
    try:
        with pytest.raises(BackendFailure) as excinfo:
            await _resolve_feature_table_bindings(
                postgres_pool,
                action_context=_context(s, denovo_alignment_idx=s["alignment_idx"]),
                reference_idx=s["reference_idx"],
                workspace=tmp_path,
            )
        assert excinfo.value.kind is FailureKind.BAD_INPUT
        assert "not a de novo alignment" in str(excinfo.value)
    finally:
        await _cleanup(postgres_pool, s)


async def test_resolver_refuses_arms_aligned_at_different_masks(postgres_pool, tmp_path):
    """Different masks filtered different reads, so the two arms are not two
    placements of one read population. Precedence over them would be deciding between
    populations, which is not what the table claims to do."""
    s = await _seed_scenario(postgres_pool, completed=2)
    d = await _seed_denovo(postgres_pool, s, mask_idx=99)
    try:
        with pytest.raises(BackendFailure) as excinfo:
            await _resolve_feature_table_bindings(
                postgres_pool,
                action_context=_context(s, d),
                reference_idx=s["reference_idx"],
                workspace=tmp_path,
            )
        assert excinfo.value.kind is FailureKind.BAD_INPUT
        assert "same masked pass-set" in str(excinfo.value)
    finally:
        await _cleanup_denovo(postgres_pool, d)
        await _cleanup(postgres_pool, s)


async def test_a_sample_that_assembled_nothing_is_reference_only_not_a_refusal(
    postgres_pool, tmp_path
):
    """`no_data` is the design's graceful path: the sample has no contigs, so no de
    novo arm is expected and the cohort still builds. This is the one absence that is
    not a refusal, and the whole reason the arm gate reads `assembly_sample` rather
    than reusing the alignment completeness check."""
    s = await _seed_scenario(postgres_pool, completed=2)
    first, second = s["prep_sample_idxs"]
    d = await _seed_denovo(
        postgres_pool, s, assembly_states={first: "completed", second: "no_data"}
    )
    try:
        result = await _resolve_feature_table_bindings(
            postgres_pool,
            action_context=_context(s, d),
            reference_idx=s["reference_idx"],
            workspace=tmp_path,
        )
        table = pq.read_table(str(result[DENOVO_GENOME_MAP_PATH_BINDING]))
        # Only the assembled sample is in the map; the other is simply absent, which
        # is what makes it reference-only downstream.
        assert set(table.column("prep_sample_idx").to_pylist()) == {first}
    finally:
        await _cleanup_denovo(postgres_pool, d)
        await _cleanup(postgres_pool, s)


@pytest.mark.parametrize("state", ["pending", "invalidated"])
async def test_a_sample_whose_assembly_is_not_terminal_or_is_withdrawn_refuses(
    postgres_pool, tmp_path, state
):
    """Neither is a state a table may be built over — 'pending' would change under
    the caller, 'invalidated' names contigs someone withdrew. Both are refused
    whatever the de novo alignment gate says, which is the asymmetry with 'no_data'
    above: all three are "no usable de novo arm", only one of them is expected."""
    s = await _seed_scenario(postgres_pool, completed=2)
    first, second = s["prep_sample_idxs"]
    d = await _seed_denovo(
        postgres_pool, s, assembly_states={first: "completed", second: state}, aligned=[first]
    )
    try:
        with pytest.raises(BackendFailure) as excinfo:
            await _resolve_feature_table_bindings(
                postgres_pool,
                action_context=_context(s, d),
                reference_idx=s["reference_idx"],
                workspace=tmp_path,
            )
        assert excinfo.value.kind is FailureKind.BAD_INPUT
        assert "neither 'completed' nor 'no_data'" in str(excinfo.value)
    finally:
        await _cleanup_denovo(postgres_pool, d)
        await _cleanup(postgres_pool, s)


async def test_a_sample_the_assembly_run_never_reached_refuses(postgres_pool, tmp_path):
    """No `assembly_sample` row at all. Absence is NOT 'no_data': the run never
    reached this sample, so nothing has established that it had nothing to assemble
    — treating it as reference-only would answer for a sample whose de novo arm may
    simply not have run."""
    s = await _seed_scenario(postgres_pool, completed=2)
    first, _second = s["prep_sample_idxs"]
    d = await _seed_denovo(postgres_pool, s, assembly_states={first: "completed"})
    try:
        with pytest.raises(BackendFailure) as excinfo:
            await _resolve_feature_table_bindings(
                postgres_pool,
                action_context=_context(s, d),
                reference_idx=s["reference_idx"],
                workspace=tmp_path,
            )
        assert excinfo.value.kind is FailureKind.BAD_INPUT
        assert "neither 'completed' nor 'no_data'" in str(excinfo.value)
    finally:
        await _cleanup_denovo(postgres_pool, d)
        await _cleanup(postgres_pool, s)


async def test_an_assembled_sample_with_no_denovo_alignment_refuses(postgres_pool, tmp_path):
    """The silent-wrong-answer case, and the reason "no alignment row → skip this
    sample" is not an acceptable loosening: this sample HAS contigs, so leaving it
    out of the de novo arm returns a reference-only answer for a sample that should
    have had both."""
    s = await _seed_scenario(postgres_pool, completed=2)
    first, _second = s["prep_sample_idxs"]
    d = await _seed_denovo(postgres_pool, s, aligned=[first])  # the second never aligned
    try:
        with pytest.raises(BackendFailure) as excinfo:
            await _resolve_feature_table_bindings(
                postgres_pool,
                action_context=_context(s, d),
                reference_idx=s["reference_idx"],
                workspace=tmp_path,
            )
        assert excinfo.value.kind is FailureKind.BAD_INPUT
        assert "should have had both arms" in str(excinfo.value)
    finally:
        await _cleanup_denovo(postgres_pool, d)
        await _cleanup(postgres_pool, s)


async def test_a_run_with_an_unminted_membership_refuses(postgres_pool, tmp_path):
    """A membership row with no `genome_idx` drops that contig from the map, and the
    drop is invisible: the genome keeps its other contigs, so its denominator comes
    back short and its breadth comes back high. Refused with the backfill named."""
    s = await _seed_scenario(postgres_pool, completed=2)
    d = await _seed_denovo(postgres_pool, s, minted=False)
    try:
        with pytest.raises(BackendFailure) as excinfo:
            await _resolve_feature_table_bindings(
                postgres_pool,
                action_context=_context(s, d),
                reference_idx=s["reference_idx"],
                workspace=tmp_path,
            )
        assert excinfo.value.kind is FailureKind.BAD_INPUT
        assert "assembly-genome backfill" in str(excinfo.value)
    finally:
        await _cleanup_denovo(postgres_pool, d)
        await _cleanup(postgres_pool, s)
