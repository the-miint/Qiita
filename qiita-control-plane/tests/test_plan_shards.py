"""Tests for the plan-shards assignment core.

`plan_shards` turns the tiler + persistence into an end-to-end shard
assignment for one reference: stream (feature_idx, genome_idx) from Postgres,
DoGet the reference's taxonomy from the data plane, reduce to one lineage per
genome in DuckDB, tile lineage-sorted (`tile_by_lineage`), expand back to
features, and persist via `write_shard_assignment`.

The DuckDB reduction (`_genome_lineages`) and expansion (`_compute_shards`) are
pure over an in-memory connection, so the lineage reconstruction, the
min-feature representative determinism, and the genome->feature round-trip are
unit-testable with no DB or data plane. The end-to-end `plan_shards` gets a DB
test with the DoGet seam stubbed (a taxonomy Parquet written directly).
"""

import secrets

import duckdb
import pytest

from qiita_control_plane.actions import library
from qiita_control_plane.actions.library import _compute_shards, _genome_lineages
from qiita_control_plane.shard_planner import LineageItem

# ---------------------------------------------------------------------------
# Pure-unit: the DuckDB lineage reduce + shard expand (in-memory connection)
# ---------------------------------------------------------------------------

_TAXONOMY_DDL = (
    "CREATE TABLE taxonomy ("
    " feature_idx BIGINT, domain VARCHAR, phylum VARCHAR, class VARCHAR,"
    ' "order" VARCHAR, family VARCHAR, genus VARCHAR, species VARCHAR, strain VARCHAR)'
)


def _con(member_genome_rows, taxonomy_rows):
    """In-memory DuckDB with member_genome + taxonomy seeded.

    member_genome_rows: list[(feature_idx, genome_idx)].
    taxonomy_rows: list[(feature_idx, domain, phylum, class, order, family,
    genus, species, strain)] — pass None for absent ranks.
    """
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE member_genome (feature_idx BIGINT, genome_idx BIGINT)")
    if member_genome_rows:
        con.executemany("INSERT INTO member_genome VALUES (?, ?)", member_genome_rows)
    con.execute(_TAXONOMY_DDL)
    if taxonomy_rows:
        con.executemany("INSERT INTO taxonomy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", taxonomy_rows)
    return con


def test_genome_lineages_concats_ranks_semicolon():
    con = _con(
        [(10, 1)],
        [(10, "Bacteria", "Firmicutes", None, None, None, None, None, None)],
    )
    items = _genome_lineages(con)
    assert items == [LineageItem(item_id=1, lineage="Bacteria;Firmicutes")]


def test_genome_lineages_all_null_taxonomy_is_empty_string():
    """A genome whose features have no taxonomy row (LEFT JOIN miss) or all-NULL
    ranks reduces to lineage '' — unclassified, which sorts first."""
    con = _con([(10, 1)], [])  # no taxonomy row at all
    assert _genome_lineages(con) == [LineageItem(item_id=1, lineage="")]

    con2 = _con([(20, 2)], [(20, None, None, None, None, None, None, None, None)])
    assert _genome_lineages(con2) == [LineageItem(item_id=2, lineage="")]


def test_genome_lineages_min_feature_representative():
    """A genome with per-feature-divergent taxonomy takes the lineage of its
    LOWEST feature_idx (deterministic, input-order-independent)."""
    con = _con(
        [(99, 1), (11, 1)],  # inserted hi-then-lo; representative is feature 11
        [
            (99, "Zeta", None, None, None, None, None, None, None),
            (11, "Alpha", "Beta", None, None, None, None, None, None),
        ],
    )
    assert _genome_lineages(con) == [LineageItem(item_id=1, lineage="Alpha;Beta")]


def test_genome_lineages_lowest_classified_member_when_lowest_feature_unclassified():
    """When the LOWEST feature_idx member carries no taxonomy row (e.g. it was
    excluded, so its LEFT JOIN misses), the representative lineage falls to the
    lowest feature_idx member that IS classified — the genome keeps a real
    lineage instead of tiling as unclassified. Biology: a genome is one organism,
    so its contigs share one lineage; blocking the lowest contig must not relocate
    the healthy siblings to the unclassified shard."""
    con = _con(
        [(11, 1), (99, 1)],  # feature 11 (lowest) has NO taxonomy row
        [(99, "Zeta", None, None, None, None, None, None, None)],
    )
    assert _genome_lineages(con) == [LineageItem(item_id=1, lineage="Zeta")]


def test_genome_lineages_no_classified_member_stays_unclassified():
    """A multi-member genome where NO member has taxonomy (every LEFT JOIN misses)
    still reduces to '' — the classified-member FILTER yields an empty aggregate
    → NULL → the `lineage or ''` fallback."""
    con = _con([(11, 1), (99, 1)], [])
    assert _genome_lineages(con) == [LineageItem(item_id=1, lineage="")]


def test_genome_lineages_lowest_classified_member_wins_the_feature_idx_tiebreak():
    """The representative is the lowest-feature_idx CLASSIFIED member — not the
    lexicographically smallest lineage. Here the lowest member (3) is unclassified,
    the lowest CLASSIFIED member (20) carries a lex-LARGER lineage ('Zeta') than a
    higher member (99, 'Alpha'), so the arg_min-over-feature_idx tie-break must
    still pick 20's 'Zeta'. Pins the tie-break against a `min(lineage)` refactor."""
    con = _con(
        [(3, 1), (20, 1), (99, 1)],  # feature 3 (lowest) has NO taxonomy row
        [
            (20, "Zeta", None, None, None, None, None, None, None),  # lowest CLASSIFIED
            (99, "Alpha", None, None, None, None, None, None, None),  # higher, lex-smaller
        ],
    )
    assert _genome_lineages(con) == [LineageItem(item_id=1, lineage="Zeta")]


def test_compute_shards_round_trip_every_member_feature_once():
    """Every genome-bearing member feature lands in exactly one shard; the
    returned shards are feature lists indexed by shard_id."""
    # 3 genomes, 4 features (g1 has two features).
    member = [(10, 1), (11, 1), (20, 2), (30, 3)]
    tax = [
        (10, "Bacteria", "Firmicutes", None, None, None, None, None, None),
        (11, "Bacteria", "Firmicutes", None, None, None, None, None, None),
        (20, None, None, None, None, None, None, None, None),  # unclassified
        (30, "Bacteria", "Actinobacteria", None, None, None, None, None, None),
    ]
    shards = _compute_shards(_con(member, tax), num_shards=3)
    # 3 genomes, num_shards=3 -> one genome per shard, lineage-sorted:
    #   '' (g2, feature 20) < 'Bacteria;Actinobacteria' (g3, 30)
    #     < 'Bacteria;Firmicutes' (g1, 10+11)
    assert shards == [[20], [30], [10, 11]]
    # Every member feature appears exactly once across all shards.
    flat = sorted(f for shard in shards for f in shard)
    assert flat == [10, 11, 20, 30]


def test_compute_shards_shared_feature_lands_in_exactly_one_shard():
    """A feature shared by two genomes (an identical plasmid → one feature_idx
    under both) whose lineages tile to DIFFERENT shards must land in exactly one
    shard — deterministically the lowest shard_id — so the shard lists stay
    disjoint and write_shard_assignment never stamps the row twice. Before the
    many-to-many fix the feature appeared in BOTH shard lists."""
    # feature 100 is shared by genome 1 ('Aaa' -> low shard) and genome 2
    # ('Zzz' -> high shard); each genome also has a unique classified contig so
    # neither shard empties.
    member = [(10, 1), (100, 1), (20, 2), (100, 2)]
    tax = [
        (10, "Aaa", None, None, None, None, None, None, None),
        (20, "Zzz", None, None, None, None, None, None, None),
    ]
    shards = _compute_shards(_con(member, tax), num_shards=2)
    # 'Aaa' (g1) sorts before 'Zzz' (g2): g1 -> shard 0, g2 -> shard 1. The shared
    # feature 100 (min shard_id 0) joins shard 0 and is absent from shard 1.
    assert shards == [[10, 100], [20]]
    flat = [f for shard in shards for f in shard]
    assert flat.count(100) == 1


def test_compute_shards_shared_feature_that_empties_a_shard_is_dropped_and_reindexed():
    """If deduping a shared feature to its lowest shard leaves a higher shard with
    no features, that shard drops out and the survivors re-index to contiguous
    positions — so `len(shards)` is the non-empty count and the fan-out (which
    dispatches one child per returned shard) never gets an empty one."""
    # g1: unique contig 10 ('Aaa') + shared plasmid 100 ('Mmm').
    # g2: ONLY the shared plasmid 100 -> its shard empties when 100 migrates down.
    # g3: unique contig 30 ('Zzz').
    member = [(10, 1), (100, 1), (100, 2), (30, 3)]
    tax = [
        (10, "Aaa", None, None, None, None, None, None, None),
        (100, "Mmm", None, None, None, None, None, None, None),
        (30, "Zzz", None, None, None, None, None, None, None),
    ]
    shards = _compute_shards(_con(member, tax), num_shards=3)
    # Tiling: g1 'Aaa'->0, g2 'Mmm'->1, g3 'Zzz'->2. Feature 100's min shard is 0,
    # so g2's shard 1 empties and drops; g3 (was shard 2) re-buckets to position 1.
    assert shards == [[10, 100], [30]]
    assert len(shards) == 2  # 3 genomes, num_shards=3, but one shard emptied


def test_compute_shards_fewer_genomes_than_num_shards():
    """N shards never exceeds the genome count (no empty shards)."""
    shards = _compute_shards(_con([(10, 1), (20, 2)], []), num_shards=1000)
    assert len(shards) == 2


def test_compute_shards_zero_genomes_is_empty():
    assert _compute_shards(_con([], []), num_shards=1000) == []


# ---------------------------------------------------------------------------
# DB: plan_shards end-to-end (DoGet seam stubbed with a taxonomy Parquet)
# ---------------------------------------------------------------------------


async def _mint_feature(pool, h):
    return await pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES ($1::uuid) RETURNING feature_idx", h
    )


async def _mint_genome(pool, source_id):
    return await pool.fetchval(
        "INSERT INTO qiita.genome (source, source_id) VALUES ('refseq', $1) RETURNING genome_idx",
        source_id,
    )


@pytest.mark.db
async def test_plan_shards_assigns_and_leaves_no_genome_null(postgres_pool, monkeypatch, tmp_path):
    """plan_shards persists a shard per genome and leaves a no-genome member
    feature's shard_id NULL. The DoGet is stubbed by writing the taxonomy
    Parquet directly (no data plane)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    suffix = secrets.token_hex(4)
    ref = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1', 'sequence_reference',"
        "         (SELECT MIN(idx) FROM qiita.principal)) RETURNING reference_idx",
        f"plan-shards-e2e-{suffix}",
    )
    g_a = g_b = None
    try:
        # Two genomes (one with two features) + one no-genome feature.
        f_a1 = await _mint_feature(postgres_pool, f"{suffix}-0000-0000-0000-000000000001")
        f_a2 = await _mint_feature(postgres_pool, f"{suffix}-0000-0000-0000-000000000002")
        f_b = await _mint_feature(postgres_pool, f"{suffix}-0000-0000-0000-000000000003")
        f_none = await _mint_feature(postgres_pool, f"{suffix}-0000-0000-0000-000000000004")
        g_a = await _mint_genome(postgres_pool, f"g-a-{suffix}")
        g_b = await _mint_genome(postgres_pool, f"g-b-{suffix}")
        for f in (f_a1, f_a2, f_b, f_none):
            await postgres_pool.execute(
                "INSERT INTO qiita.reference_membership (reference_idx, feature_idx)"
                " VALUES ($1, $2)",
                ref,
                f,
            )
        for f, g in ((f_a1, g_a), (f_a2, g_a), (f_b, g_b)):
            await postgres_pool.execute(
                "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
                f,
                g,
            )

        # Stub the DoGet: write the taxonomy Parquet the seam would fetch.
        def fake_doget(data_plane_url, ticket_bytes, out_path):
            table = pa.table(
                {
                    "feature_idx": pa.array([f_a1, f_a2, f_b], pa.int64()),
                    "domain": pa.array(["Bacteria", "Bacteria", "Archaea"], pa.string()),
                    "phylum": pa.array(["Firmicutes", "Firmicutes", "Euryarchaeota"], pa.string()),
                    "class": pa.array([None, None, None], pa.string()),
                    "order": pa.array([None, None, None], pa.string()),
                    "family": pa.array([None, None, None], pa.string()),
                    "genus": pa.array([None, None, None], pa.string()),
                    "species": pa.array([None, None, None], pa.string()),
                    "strain": pa.array([None, None, None], pa.string()),
                }
            )
            pq.write_table(table, str(out_path))
            return out_path

        monkeypatch.setattr(library, "_do_get_reference_taxonomy", fake_doget)

        n = await library.plan_shards(
            postgres_pool,
            ref,
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://unused:1",
            workspace=tmp_path,
            num_shards=1000,
        )
        assert n == 2  # two genomes -> two shards

        rows = await postgres_pool.fetch(
            "SELECT feature_idx, shard_id FROM qiita.reference_membership WHERE reference_idx = $1",
            ref,
        )
        by_feature = {r["feature_idx"]: r["shard_id"] for r in rows}
        # No-genome feature stays NULL (deferred).
        assert by_feature[f_none] is None
        # Both features of genome A share one shard; genome B is a different shard.
        assert by_feature[f_a1] == by_feature[f_a2]
        assert by_feature[f_b] != by_feature[f_a1]
        assert {by_feature[f_a1], by_feature[f_b]} == {0, 1}
    finally:
        genome_idxs = [g for g in (g_a, g_b) if g is not None]
        await postgres_pool.execute(
            "DELETE FROM qiita.feature_genome WHERE genome_idx = ANY($1::bigint[])", genome_idxs
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", ref
        )
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref)
        await postgres_pool.execute(
            "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])", genome_idxs
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.feature WHERE sequence_hash::text LIKE $1", f"{suffix}-%"
        )
