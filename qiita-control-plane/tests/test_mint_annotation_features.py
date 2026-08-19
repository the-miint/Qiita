"""DB tests for `mint-annotation-features` — the three identities it mints.

This primitive is the only place `annotation_idx` and `annotation_term_idx` come
into existence, and its zero-annotation path runs on EVERY reference-add (almost
none of which carry a GFF3). Both halves are covered here, because a break in
either is invisible until a reference is actually ingested.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import duckdb
import pytest

pytestmark = pytest.mark.db


_ANNOTATION_MANIFEST_SCHEMA = (
    "sequence_hash UUID, parent_sequence_hash UUID, parent_read_id VARCHAR, "
    "annotation_id VARCHAR, source VARCHAR, annotation_type VARCHAR, "
    "position BIGINT, stop_position BIGINT, strand VARCHAR, "
    "score DOUBLE, phase SMALLINT, attributes MAP(VARCHAR, VARCHAR)"
)


def _write_parquet(path: Path, schema_sql: str, rows: list[tuple]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"CREATE TEMP TABLE t ({schema_sql})")
        if rows:
            conn.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in rows[0])})", rows)
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    return path


def _rows(path: Path) -> list[dict]:
    with duckdb.connect(":memory:") as conn:
        cols = [c[0] for c in conn.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()]
        return [
            dict(zip(cols, r, strict=True))
            for r in conn.execute(f"SELECT * FROM '{path}'").fetchall()
        ]


async def _reference(pool) -> int:
    from qiita_control_plane.testing.db_seeds import seed_user_principal

    suffix = uuid.uuid4().hex[:8]
    owner_idx = await seed_user_principal(pool, prefix="mint-annot", suffix=suffix)
    return await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2) RETURNING reference_idx",
        f"mint-annot-{suffix}",
        owner_idx,
    )


async def _feature(pool, sequence_hash: uuid.UUID) -> int:
    return await pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES ($1) RETURNING feature_idx",
        sequence_hash,
    )


async def test_no_gff_still_writes_a_typed_empty_annotation_map(postgres_pool, tmp_path):
    """The path EVERY reference-add takes. reference_load binds `annotation_map`
    unconditionally, so an absent file is a FileNotFoundError rather than an absent
    annotation set — the empty map must be written, and it must be a real Parquet.

    This is a regression test with a specific failure in mind: the map is COPY'd with
    PARQUET_OPTS (which sets ROW_GROUP_SIZE_BYTES), and DuckDB rejects that at BIND time
    unless preserve_insertion_order is off. That raises on the ZERO-row write, so it
    broke every reference-add — GFF3 or not — while every GFF-bearing test still passed.
    """
    from qiita_control_plane.actions.library import mint_annotation_features

    reference_idx = await _reference(postgres_pool)
    empty_manifest = _write_parquet(
        tmp_path / "annotation_manifest.parquet", _ANNOTATION_MANIFEST_SCHEMA, []
    )
    feature_map = _write_parquet(
        tmp_path / "feature_map.parquet", "sequence_hash UUID, feature_idx BIGINT", []
    )

    annotation_map, minted, reused = await mint_annotation_features(
        postgres_pool, reference_idx, empty_manifest, feature_map, tmp_path / "out"
    )

    assert annotation_map.exists(), "the map must be written even with zero annotations"
    assert (minted, reused) == (0, 0)
    assert _rows(annotation_map) == []
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.reference_annotation WHERE reference_idx = $1",
            reference_idx,
        )
        == 0
    )


async def test_mints_annotation_idx_terms_and_links(postgres_pool, tmp_path):
    """The populated path. One interval carrying THREE cross-references in one Dbxref —
    the shape NCBI actually emits (4816 features of E. coli RefSeq carry three, 4161
    carry five) — must produce three terms and three links, not one.
    """
    from qiita_control_plane.actions.library import mint_annotation_features

    reference_idx = await _reference(postgres_pool)
    parent_hash, interval_hash = uuid.uuid4(), uuid.uuid4()
    parent_feature_idx = await _feature(postgres_pool, parent_hash)

    accession = f"EG{uuid.uuid4().hex[:8]}"
    manifest = _write_parquet(
        tmp_path / "annotation_manifest.parquet",
        _ANNOTATION_MANIFEST_SCHEMA,
        [
            (
                str(interval_hash),
                str(parent_hash),
                "plasmid_01",
                "insert_01",
                "RefSeq",
                "CDS",
                2001,
                3001,
                "+",
                None,
                0,
                {
                    "Dbxref": f"GeneID:944742,ECOCYC:{accession},UniProtKB/Swiss-Prot:P0AD86",
                    "product": "16S ribosomal RNA",
                },
            )
        ],
    )
    feature_map = _write_parquet(
        tmp_path / "feature_map.parquet",
        "sequence_hash UUID, feature_idx BIGINT",
        [(str(parent_hash), parent_feature_idx)],
    )

    annotation_map, minted, _ = await mint_annotation_features(
        postgres_pool, reference_idx, manifest, feature_map, tmp_path / "out"
    )

    assert minted == 1  # the interval's own feature_idx
    (mapped,) = _rows(annotation_map)
    assert mapped["parent_feature_idx"] == parent_feature_idx
    assert (mapped["position"], mapped["stop_position"]) == (2001, 3001)

    row = await postgres_pool.fetchrow(
        "SELECT * FROM qiita.reference_annotation WHERE reference_idx = $1", reference_idx
    )
    assert row["annotation_idx"] == mapped["annotation_idx"]
    assert row["annotation_id"] == "insert_01"  # provenance, not identity
    assert row["phase"] == 0
    assert row["score"] is None

    # THE assertion: one interval, three systems, three terms — a single annotation→term
    # FK could have held exactly one of them and would have dropped the other two.
    terms = await postgres_pool.fetch(
        "SELECT t.system, t.system_id, t.definition FROM qiita.annotation_term t"
        " JOIN qiita.annotation_to_term l ON l.annotation_term_idx = t.annotation_term_idx"
        " WHERE l.annotation_idx = $1 ORDER BY t.system",
        row["annotation_idx"],
    )
    assert [(t["system"], t["system_id"]) for t in terms] == [
        ("ECOCYC", accession),
        ("GeneID", "944742"),
        # The system itself contains a colon-adjacent slash and the id follows the FIRST
        # colon — splitting on the wrong one truncates the accession.
        ("UniProtKB/Swiss-Prot", "P0AD86"),
    ]
    assert all(t["definition"] == "16S ribosomal RNA" for t in terms)


async def test_reingest_is_idempotent_and_returns_the_same_annotation_idx(postgres_pool, tmp_path):
    """A re-run of a reference-add must UPSERT, not duplicate — and must hand back the
    EXISTING annotation_idx for every row. `ON CONFLICT DO NOTHING` returns nothing for
    the rows that conflicted, which would silently leave them out of the map and out of
    the lake; `DO UPDATE` is what makes the second run return a complete map.
    """
    from qiita_control_plane.actions.library import mint_annotation_features

    reference_idx = await _reference(postgres_pool)
    parent_hash, interval_hash = uuid.uuid4(), uuid.uuid4()
    parent_feature_idx = await _feature(postgres_pool, parent_hash)

    manifest = _write_parquet(
        tmp_path / "annotation_manifest.parquet",
        _ANNOTATION_MANIFEST_SCHEMA,
        [
            (
                str(interval_hash),
                str(parent_hash),
                "plasmid_01",
                "insert_01",
                "syndna",
                "insert",
                101,
                201,
                "+",
                None,
                None,
                {"Dbxref": "RFAM:RF00177"},
            )
        ],
    )
    feature_map = _write_parquet(
        tmp_path / "feature_map.parquet",
        "sequence_hash UUID, feature_idx BIGINT",
        [(str(parent_hash), parent_feature_idx)],
    )

    first, _, _ = await mint_annotation_features(
        postgres_pool, reference_idx, manifest, feature_map, tmp_path / "out1"
    )
    second, _, _ = await mint_annotation_features(
        postgres_pool, reference_idx, manifest, feature_map, tmp_path / "out2"
    )

    (a,), (b,) = _rows(first), _rows(second)
    assert a["annotation_idx"] == b["annotation_idx"], (
        "the second run must return the EXISTING annotation_idx, not nothing and not a new one"
    )
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.reference_annotation WHERE reference_idx = $1",
            reference_idx,
        )
        == 1
    ), "the natural key must collapse the re-ingest onto one row"
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.annotation_to_term WHERE annotation_idx = $1",
            a["annotation_idx"],
        )
        == 1
    ), "term links must not accumulate duplicates across re-runs"


async def test_one_xref_cited_by_two_annotations_does_not_raise(postgres_pool, tmp_path):
    """The mainline NCBI shape, and a regression test with a specific crash in mind.

    A RefSeq `gene` line and its `CDS` child sit at the same window and BOTH carry the
    same `GeneID:` — so one batch proposes that term twice. Postgres raises
    `cardinality_violation` ("ON CONFLICT DO UPDATE command cannot affect row a second
    time") when a single INSERT proposes two rows with one conflict key, so the term
    UPSERT has to run at the (system, system_id) grain, not at the annotation grain.

    The gene/CDS pair is also exactly the case where only ONE of the two rows carries the
    `product`: the definition must come from the row that has it, not from whichever
    happened to sort first.
    """
    from qiita_control_plane.actions.library import mint_annotation_features

    reference_idx = await _reference(postgres_pool)
    parent_hash = uuid.uuid4()
    parent_feature_idx = await _feature(postgres_pool, parent_hash)
    gene_id = f"GID{uuid.uuid4().hex[:8]}"

    def row(annotation_type, attributes, interval_hash):
        return (
            str(interval_hash),
            str(parent_hash),
            "chromosome_01",
            f"{annotation_type}-b0001",
            "RefSeq",
            annotation_type,
            190,
            256,
            "+",
            None,
            None,
            attributes,
        )

    manifest = _write_parquet(
        tmp_path / "annotation_manifest.parquet",
        _ANNOTATION_MANIFEST_SCHEMA,
        [
            # The gene: carries the xref, but NO product.
            row("gene", {"Dbxref": f"GeneID:{gene_id}"}, uuid.uuid4()),
            # Its CDS child: same xref, and the product that names it.
            row(
                "CDS",
                {"Dbxref": f"GeneID:{gene_id}", "product": "thr operon leader peptide"},
                uuid.uuid4(),
            ),
        ],
    )
    feature_map = _write_parquet(
        tmp_path / "feature_map.parquet",
        "sequence_hash UUID, feature_idx BIGINT",
        [(str(parent_hash), parent_feature_idx)],
    )

    await mint_annotation_features(
        postgres_pool, reference_idx, manifest, feature_map, tmp_path / "out"
    )

    # ONE term, cited by BOTH annotations.
    term = await postgres_pool.fetchrow(
        "SELECT annotation_term_idx, definition FROM qiita.annotation_term"
        " WHERE system = 'GeneID' AND system_id = $1",
        gene_id,
    )
    assert term is not None
    assert term["definition"] == "thr operon leader peptide", (
        "the definition must come from the row that HAS one — the gene carries the xref "
        "with no product, and only its CDS child names it"
    )
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.annotation_to_term l"
            " JOIN qiita.reference_annotation ra ON ra.annotation_idx = l.annotation_idx"
            " WHERE ra.reference_idx = $1 AND l.annotation_term_idx = $2",
            reference_idx,
            term["annotation_term_idx"],
        )
        == 2
    ), "both the gene and its CDS cite the term"
