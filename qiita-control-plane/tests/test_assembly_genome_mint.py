"""DB-bound tests for the assembly genome mint and its backfill.

Two things are under test. First, that `write_assembly_membership` mints one
qiita.genome per assembled SUBJECT — per refined bin, per LCG contig, per unbinned
contig — and stamps it onto `assembly_membership.genome_idx`.

Second, that it does so without touching the reference graph. An assembled contig
whose bytes match a reference sequence resolves to the SAME content-addressed
feature_idx, so the two graphs meet at that feature. Writing the assembly edge into
`qiita.feature_genome` would put sample-derived genomes inside every reference map
sharing a contig and inside the reach of the GLOBAL `qiita.reference_exclusion`
blocklist, which expands a blocked genome to all its features through that junction.
`test_a_shared_contig_never_reaches_the_reference_graph` is what pins the two apart;
it fails the moment the edge moves back.
"""

import secrets

import asyncpg
import duckdb
import pytest
from qiita_common.assembly_constants import KIND_LCG, KIND_MAG, KIND_UNBINNED
from qiita_common.models import GenomeSource

from qiita_control_plane.actions import library as lib
from qiita_control_plane.actions.sequenced_pool import (
    SequencedPoolDeleteBlocked,
    assert_sequenced_pool_deletable,
)
from qiita_control_plane.backfill.assembly_genome import (
    BackfillPlan,
    apply_backfill,
    plan_backfill,
)
from qiita_control_plane.repositories.assembly import assembly_genome_source_id
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.repositories.reference_exclusion import (
    RESOLVE_EXCLUDED_FEATURES_SQL,
)
from qiita_control_plane.testing.db_seeds import (
    canonical_sequence_hashes,
    seed_biosample_with_sequenced_prep_sample,
    seed_feature_genome,
    seed_genome,
    seed_reference_with_sequences,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)

pytestmark = pytest.mark.db

# Four distinct sequences, none the reverse complement of another, so the canonical
# strand-folding hash gives four features rather than collapsing a pair (which
# `test_write_assembly_membership` covers deliberately and this one must not hit).
# `_SHARED` is the one the reference also carries.
#
# They are also distinct from `test_write_assembly_membership`'s contigs under the
# canonical hash, which is case-INSENSITIVE and strand-folding: these modules share
# a worker database, and a sequence colliding across them makes one module's
# teardown delete the other's feature mid-run. Verified with
# `canonical_sequence_hashes`, not by eye.
_SHARED = "GATTACAGGCCTAAGTCCGATTGC"
_MAG2 = "CCTAGGTTACGGATCCATTAAGCG"
_LCG = "TTACGGCATTCCGGAATTACGGCA"
_UNB = "CCAATTGGCCATTACGGTTAGCAT"

# (read_id, kind, bin_id, sequence). A MAG bin groups two contigs under one
# bin_id; LCG and UNBINNED carry the contig id as bin_id, which is what makes each
# of them its own subject.
#
# `c5` REPEATS `c1`'s bytes inside `c1`'s own bin, which is the case
# `ASSEMBLY_MEMBERSHIP_JOIN_SQL`'s DISTINCT exists for: `assembly_hash` composes
# read_id as kind:bin_id:sequence_index, so duplicated bytes in one bin arrive as
# two read_ids resolving to one feature_idx. Without that DISTINCT the write hands
# Postgres the same conflict target twice and `ON CONFLICT DO UPDATE` raises
# `cardinality_violation` — so this row is what makes the suite fail if it is
# dropped. It changes no count below: the pair collapses back to one row.
_CONTIGS = [
    ("c1", KIND_MAG, "bin.1", _SHARED),
    ("c2", KIND_MAG, "bin.1", _MAG2),
    ("c3", KIND_LCG, "c3", _LCG),
    ("c4", KIND_UNBINNED, "c4", _UNB),
    ("c5", KIND_MAG, "bin.1", _SHARED),
]


def _write(path, schema, rows):
    with duckdb.connect(":memory:") as c:
        c.execute(f"CREATE TEMP TABLE t ({schema})")
        c.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in rows[0])})", rows)
        c.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")


def _contig_hashes():
    """One canonical hash per `_CONTIGS` entry, positionally aligned.

    ONE CALL PER SEQUENCE, deliberately. `canonical_sequence_hashes` runs a
    `SELECT DISTINCT` with no ORDER BY, so a batch call returns the hashes in an
    arbitrary order — zipping that against `_CONTIGS` silently pairs each contig
    with someone else's feature, and every assertion downstream still passes.
    """
    return [canonical_sequence_hashes([seq])[0] for *_, seq in _CONTIGS]


async def _stage_assembly(pool, tmp_path, feature_idxs):
    """Write the three Parquets `write_assembly_membership` joins, for `_CONTIGS`
    against already-minted features, plus the genomes_dir it reads the per-contig
    attribute sidecar from. Returns the four paths in call order.

    The genomes_dir is left EMPTY: these tests are about the genome mint, and an
    absent sidecar is the state every run assembled before it existed is in, so
    they exercise that path while `test_write_assembly_membership.py` covers the
    attributes themselves."""
    hashes = _contig_hashes()
    bin_map, manifest, feature_map = (
        tmp_path / "bin_map.parquet",
        tmp_path / "manifest.parquet",
        tmp_path / "feature_map.parquet",
    )
    _write(
        bin_map,
        "read_id VARCHAR, kind VARCHAR, bin_id VARCHAR, contig_id VARCHAR",
        [(rid, kind, bid, f"ctg_{rid}") for rid, kind, bid, _ in _CONTIGS],
    )
    _write(
        manifest,
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [(rid, str(h), len(seq)) for (rid, _, _, seq), h in zip(_CONTIGS, hashes, strict=True)],
    )
    # Keyed by HASH, deduplicated: production's feature_map is one row per
    # sequence_hash, and emitting a repeated hash twice would fan the join across
    # both copies rather than exercising the in-bin duplicate.
    by_hash = dict(zip(hashes, feature_idxs, strict=True))
    _write(
        feature_map,
        "sequence_hash UUID, feature_idx BIGINT",
        [(str(h), f) for h, f in by_hash.items()],
    )
    genomes_dir = tmp_path / "genomes"
    genomes_dir.mkdir(exist_ok=True)
    return bin_map, manifest, feature_map, genomes_dir


async def _setup(postgres_pool, tmp_path, *, label):
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix=label, suffix=suffix)
    _bs, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    version = f"v-{label}-{suffix}"
    async with postgres_pool.acquire() as conn:
        row = await mint_processing(
            conn,
            workflow="long-read-assembly",
            version=version,
            params={"workflow": "long-read-assembly", "version": version},
        )
    processing_idx = row["processing_idx"]
    # The reference carries `_SHARED` only. Seeding it FIRST mints that feature,
    # so the assembly below resolves to the same feature_idx — the collision this
    # module exists to keep out of the reference graph.
    reference_idx = await seed_reference_with_sequences(
        postgres_pool,
        name=f"{label}-ref-{suffix}",
        created_by_idx=principal_idx,
        sequences=[_SHARED],
    )
    hashes = _contig_hashes()
    await postgres_pool.execute(
        "INSERT INTO qiita.feature (sequence_hash) SELECT unnest($1::uuid[])"
        " ON CONFLICT (sequence_hash) DO NOTHING",
        hashes,
    )
    # Positionally aligned with `_CONTIGS`, so a repeated sequence repeats its
    # feature_idx here — `feats[0]` is `_SHARED`'s, which the reference also holds.
    feature_idxs = [
        await postgres_pool.fetchval(
            "SELECT feature_idx FROM qiita.feature WHERE sequence_hash = $1", h
        )
        for h in hashes
    ]
    paths = await _stage_assembly(postgres_pool, tmp_path, feature_idxs)
    return principal_idx, prep_sample_idx, processing_idx, reference_idx, feature_idxs, paths


async def _teardown(pool, *, prep_sample_idx, reference_idx, feature_idxs):
    """FK-reverse, and the order matters: qiita.genome cannot go while a
    feature_genome row points at it (bare FK) or an assembly_membership row does
    (likewise). Robust to a test that failed part-way, which is when it runs."""
    await pool.execute(
        "DELETE FROM qiita.assembly_membership WHERE prep_sample_idx = $1", prep_sample_idx
    )
    await pool.execute(
        "DELETE FROM qiita.reference_exclusion x USING qiita.genome g"
        " WHERE x.genome_idx = g.genome_idx AND g.prep_sample_idx = $1",
        prep_sample_idx,
    )
    await pool.execute(
        "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])", feature_idxs
    )
    await pool.execute("DELETE FROM qiita.genome WHERE prep_sample_idx = $1", prep_sample_idx)
    await pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)


async def _subjects(pool, prep_sample_idx):
    """`(kind, bin_id) -> (genome_idx, contig_count)` as actually stamped."""
    rows = await pool.fetch(
        "SELECT kind, bin_id, genome_idx, count(*) AS n"
        "  FROM qiita.assembly_membership WHERE prep_sample_idx = $1"
        " GROUP BY kind, bin_id, genome_idx ORDER BY kind, bin_id",
        prep_sample_idx,
    )
    return {(r["kind"], r["bin_id"]): (r["genome_idx"], r["n"]) for r in rows}


async def test_mint_writes_one_genome_per_subject(postgres_pool, tmp_path):
    """One genome per (prep_sample, processing, kind, bin_id): the MAG's two
    contigs share one, the LCG and the unbinned contig each get their own."""
    principal_idx, prep, proc, ref, feats, paths = await _setup(
        postgres_pool, tmp_path, label="agm-subjects"
    )
    try:
        await lib.write_assembly_membership(postgres_pool, prep, proc, *paths)

        subjects = await _subjects(postgres_pool, prep)
        assert set(subjects) == {(KIND_MAG, "bin.1"), (KIND_LCG, "c3"), (KIND_UNBINNED, "c4")}
        assert subjects[(KIND_MAG, "bin.1")][1] == 2, "a bin groups its contigs under one genome"
        assert subjects[(KIND_LCG, "c3")][1] == 1
        assert subjects[(KIND_UNBINNED, "c4")][1] == 1

        genome_idxs = [g for g, _ in subjects.values()]
        assert None not in genome_idxs, "every row carries a genome"
        assert len(set(genome_idxs)) == 3, "the three subjects are three distinct genomes"

        # Provenance: qiita-origin, pointing at the sample, keyed on the shared
        # derivation rather than on whatever this test would compute by hand.
        for (kind, bin_id), (genome_idx, _) in subjects.items():
            row = await postgres_pool.fetchrow(
                "SELECT source, source_id, prep_sample_idx FROM qiita.genome WHERE genome_idx = $1",
                genome_idx,
            )
            assert row["source"] == GenomeSource.QIITA.value
            assert row["prep_sample_idx"] == prep
            assert row["source_id"] == assembly_genome_source_id(
                prep_sample_idx=prep, processing_idx=proc, kind=kind, bin_id=bin_id
            )
    finally:
        await _teardown(postgres_pool, prep_sample_idx=prep, reference_idx=ref, feature_idxs=feats)


async def test_a_rerun_restamps_rather_than_leaving_a_null(postgres_pool, tmp_path):
    """The replay case, and why the insert DO UPDATEs.

    A row written before the mint existed — or by a backfill — conflicts on the
    natural PK. Under DO NOTHING it would keep its NULL genome_idx, which a
    genome-level roll-up over these contigs would have to drop rather than report.
    """
    principal_idx, prep, proc, ref, feats, paths = await _setup(
        postgres_pool, tmp_path, label="agm-rerun"
    )
    try:
        await lib.write_assembly_membership(postgres_pool, prep, proc, *paths)
        first = await _subjects(postgres_pool, prep)

        # Simulate a row that predates the mint.
        await postgres_pool.execute(
            "UPDATE qiita.assembly_membership SET genome_idx = NULL WHERE prep_sample_idx = $1",
            prep,
        )
        assert all(g is None for g, _ in (await _subjects(postgres_pool, prep)).values()), (
            "precondition"
        )

        await lib.write_assembly_membership(postgres_pool, prep, proc, *paths)
        assert await _subjects(postgres_pool, prep) == first, "re-stamped to the same genomes"

        # And no second set of genomes: the upsert re-resolves (source, source_id).
        assert (
            await postgres_pool.fetchval(
                "SELECT count(*) FROM qiita.genome WHERE prep_sample_idx = $1", prep
            )
            == 3
        )
    finally:
        await _teardown(postgres_pool, prep_sample_idx=prep, reference_idx=ref, feature_idxs=feats)


async def test_a_shared_contig_never_reaches_the_reference_graph(postgres_pool, tmp_path):
    """Three parts, and together they are why the edge is a column.

    `_SHARED` is a member of the reference AND an assembled contig, on one
    content-addressed feature_idx. The assembly mint must leave every
    reference-side reader of that feature exactly as it found it.
    """
    principal_idx, prep, proc, ref, feats, paths = await _setup(
        postgres_pool, tmp_path, label="agm-shared"
    )
    shared_feature = feats[0]
    # The whole test rests on this: `_SHARED` is one content-addressed feature that
    # the reference and the assembly BOTH claim. If the seeds ever stopped colliding
    # here, every assertion below would pass while testing nothing.
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.reference_membership"
            " WHERE reference_idx = $1 AND feature_idx = $2",
            ref,
            shared_feature,
        )
        == 1
    ), "fixture no longer shares a feature between the reference and the assembly"
    # Give the reference's shared feature a genome of its own, so the reference
    # genome map is non-empty and a change to it would show.
    ref_genome_idx, _ = await seed_genome(postgres_pool)
    await seed_feature_genome(postgres_pool, feature_idx=shared_feature, genome_idx=ref_genome_idx)
    try:
        before = await postgres_pool.fetch(
            "SELECT rm.feature_idx, fg.genome_idx FROM qiita.reference_membership rm"
            " JOIN qiita.feature_genome fg USING (feature_idx)"
            " WHERE rm.reference_idx = $1 ORDER BY 1, 2",
            ref,
        )

        await lib.write_assembly_membership(postgres_pool, prep, proc, *paths)

        # 1. No feature_genome row for any assembled contig — the assembly edge
        #    lives on assembly_membership.genome_idx and nowhere else.
        qiita_edges = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.feature_genome fg"
            " JOIN qiita.genome g USING (genome_idx)"
            " WHERE g.prep_sample_idx = $1",
            prep,
        )
        assert qiita_edges == 0

        # 2. The reference's genome map is byte-identical.
        after = await postgres_pool.fetch(
            "SELECT rm.feature_idx, fg.genome_idx FROM qiita.reference_membership rm"
            " JOIN qiita.feature_genome fg USING (feature_idx)"
            " WHERE rm.reference_idx = $1 ORDER BY 1, 2",
            ref,
        )
        assert [tuple(r) for r in after] == [tuple(r) for r in before]

        # 3. Blocking the assembly's genome must not exclude the reference's
        #    feature. This is the consequence that decided the design: the blocklist
        #    resolves through feature_genome, globally and unscoped by reference.
        mag_genome = (await _subjects(postgres_pool, prep))[(KIND_MAG, "bin.1")][0]
        await postgres_pool.execute(
            "INSERT INTO qiita.reference_exclusion (genome_idx, reason, excluded_by_idx)"
            " VALUES ($1, 'test', $2)",
            mag_genome,
            principal_idx,
        )
        excluded = {
            r["feature_idx"] for r in await postgres_pool.fetch(RESOLVE_EXCLUDED_FEATURES_SQL)
        }
        assert shared_feature not in excluded
    finally:
        await _teardown(postgres_pool, prep_sample_idx=prep, reference_idx=ref, feature_idxs=feats)
        await postgres_pool.execute(
            "DELETE FROM qiita.genome WHERE genome_idx = $1", ref_genome_idx
        )


async def test_backfill_stamps_unminted_rows_and_then_reports_nothing(postgres_pool, tmp_path):
    """The backfill converges on the same genomes the inline mint produces, and a
    second run is a no-op — the empty plan being the completeness signal a
    feature-table build reads."""
    principal_idx, prep, proc, ref, feats, paths = await _setup(
        postgres_pool, tmp_path, label="agm-backfill"
    )
    try:
        await lib.write_assembly_membership(postgres_pool, prep, proc, *paths)
        inline = await _subjects(postgres_pool, prep)

        # Return the rows to their pre-mint state, genomes and all.
        await postgres_pool.execute(
            "UPDATE qiita.assembly_membership SET genome_idx = NULL WHERE prep_sample_idx = $1",
            prep,
        )
        await postgres_pool.execute("DELETE FROM qiita.genome WHERE prep_sample_idx = $1", prep)

        plan = await plan_backfill(postgres_pool)
        mine = [s for s in plan.subjects if s.prep_sample_idx == prep]
        assert len(mine) == 3, "one subject per (kind, bin_id), not one per contig"
        assert sum(s.contig_count for s in mine) == 4

        mine_plan = BackfillPlan(subjects=mine, already_stamped_rows=plan.already_stamped_rows)
        stamped = await apply_backfill(postgres_pool, mine_plan)
        assert stamped == 4

        # Same subjects, same grouping. genome_idx values differ from `inline`
        # (those rows were deleted), so compare the shape rather than the ids.
        backfilled = await _subjects(postgres_pool, prep)
        assert set(backfilled) == set(inline)
        assert {k: n for k, (_, n) in backfilled.items()} == {k: n for k, (_, n) in inline.items()}
        assert None not in [g for g, _ in backfilled.values()]

        again = await plan_backfill(postgres_pool)
        assert [s for s in again.subjects if s.prep_sample_idx == prep] == []
    finally:
        await _teardown(postgres_pool, prep_sample_idx=prep, reference_idx=ref, feature_idxs=feats)


async def test_the_pool_delete_order_is_what_unblocks_a_prep_sample(postgres_pool, tmp_path):
    """The cascade contract, as the schema enforces it.

    `genome.prep_sample_idx` RESTRICTs, so an assembled prep_sample cannot be
    deleted while its genomes exist; and `assembly_membership.genome_idx` is a bare
    FK (NO ACTION), so those genomes cannot be deleted while rows point at them.
    Detach, delete genomes, delete prep_sample — in that order, or not at all.
    """

    principal_idx, prep, proc, ref, feats, paths = await _setup(
        postgres_pool, tmp_path, label="agm-cascade"
    )
    try:
        await lib.write_assembly_membership(postgres_pool, prep, proc, *paths)

        # Skipping the detach: the genome delete is refused.
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await postgres_pool.execute("DELETE FROM qiita.genome WHERE prep_sample_idx = $1", prep)

        # Detaching but not deleting the genomes: the prep_sample delete is refused.
        await postgres_pool.execute(
            "UPDATE qiita.assembly_membership SET genome_idx = NULL WHERE prep_sample_idx = $1",
            prep,
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep)

        # Both, in order: the prep_sample goes, and its rows CASCADE with it.
        await postgres_pool.execute(
            "DELETE FROM qiita.genome g WHERE g.prep_sample_idx = $1"
            " AND NOT EXISTS (SELECT 1 FROM qiita.feature_genome fg"
            "                  WHERE fg.genome_idx = g.genome_idx)",
            prep,
        )
        await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep)
        assert (
            await postgres_pool.fetchval(
                "SELECT count(*) FROM qiita.assembly_membership WHERE prep_sample_idx = $1", prep
            )
            == 0
        )
    finally:
        await _teardown(postgres_pool, prep_sample_idx=prep, reference_idx=ref, feature_idxs=feats)


async def test_a_reference_claimed_genome_blocks_the_pool_delete_before_it_purges(
    postgres_pool, tmp_path
):
    """The gate `assert_sequenced_pool_deletable` grew, and why it is a gate.

    The cascade's genome delete deliberately skips a genome a reference claims
    through `feature_genome` — dropping the reference's claim is not its call. That
    skip leaves `genome.prep_sample_idx`'s RESTRICT to abort the prep_sample delete,
    and the route purges the pool's DuckLake rows BEFORE the Postgres transaction,
    which a rollback does not restore. So the refusal has to happen up front, and
    `force` must not override it: force cannot make the genome deletable.
    """
    principal_idx, prep, proc, ref, feats, paths = await _setup(
        postgres_pool, tmp_path, label="agm-gate"
    )
    shared_feature = feats[0]
    try:
        await lib.write_assembly_membership(postgres_pool, prep, proc, *paths)
        mag_genome = (await _subjects(postgres_pool, prep))[(KIND_MAG, "bin.1")][0]

        # A reference load claims the assembly's genome: the same row is now spoken
        # for from both sides.
        await seed_feature_genome(postgres_pool, feature_idx=shared_feature, genome_idx=mag_genome)

        # run -> pool -> sequenced_sample, which is the scope
        # `assert_sequenced_pool_deletable` resolves its prep_samples from.
        _run_idx, pool_idx, _ss_idx = await seed_sequenced_sample_subtype(
            postgres_pool,
            prep_sample_idx=prep,
            owner_idx=principal_idx,
            sequenced_pool_item_id="1",
        )

        for force in (False, True):
            with pytest.raises(SequencedPoolDeleteBlocked) as excinfo:
                await assert_sequenced_pool_deletable(postgres_pool, pool_idx, force=force)
            assert excinfo.value.promoted_genomes == 1
            assert "claimed by a reference" in str(excinfo.value)

        # Detach the reference's claim and the same pool becomes deletable.
        await postgres_pool.execute(
            "DELETE FROM qiita.feature_genome WHERE genome_idx = $1", mag_genome
        )
        assert await assert_sequenced_pool_deletable(postgres_pool, pool_idx, force=False) == [prep]
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = $1", prep
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.sequenced_pool WHERE sequencing_run_idx IN"
            " (SELECT idx FROM qiita.sequencing_run WHERE created_by_idx = $1)",
            principal_idx,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.sequencing_run WHERE created_by_idx = $1", principal_idx
        )
        await _teardown(postgres_pool, prep_sample_idx=prep, reference_idx=ref, feature_idxs=feats)
