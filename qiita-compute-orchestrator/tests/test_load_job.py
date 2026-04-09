"""Tests for LocalBackend load job."""

import hashlib
import json
from uuid import UUID

import duckdb
import pytest
from helpers import TEST_SEQUENCES

REFERENCE_IDX = 1


def _seq_hash(seq: str) -> UUID:
    return UUID(hashlib.md5(seq.encode()).hexdigest())


TEST_HASHES = {name: _seq_hash(seq) for name, seq in TEST_SEQUENCES.items()}

# Arbitrary feature_idx assignments (as if minted by control plane).
TEST_FEATURE_MAP: dict[UUID, int] = {
    hash_val: 100 + i for i, hash_val in enumerate(TEST_HASHES.values())
}


@pytest.fixture
def fasta_path(fasta_file):
    """Extract just the path from the shared fasta_file fixture."""
    path, _ = fasta_file
    return path


@pytest.fixture
def manifest_file(tmp_path):
    """Create a hash manifest matching the test FASTA."""
    entries = [
        {
            "read_id": name,
            "sequence_hash": str(TEST_HASHES[name]),
            "length": len(seq),
        }
        for name, seq in TEST_SEQUENCES.items()
    ]
    manifest = {"reference_idx": REFERENCE_IDX, "entries": entries}
    path = tmp_path / "hash_manifest.json"
    path.write_text(json.dumps(manifest, indent=2))
    return path


@pytest.fixture
def feature_map():
    """Feature map as returned by control plane mint endpoint."""
    return dict(TEST_FEATURE_MAP)


@pytest.fixture
def taxonomy_file(tmp_path):
    """Create a 5-entry taxonomy TSV in GG2 format (with strain on seq4)."""
    path = tmp_path / "taxonomy.tsv"
    taxa = {
        "seq1": (
            "d__Bacteria; p__Bacillota; c__Bacilli;"
            " o__Lactobacillales; f__Lactobacillaceae;"
            " g__Lactobacillus; s__Lactobacillus acidophilus"
        ),
        "seq2": (
            "d__Bacteria; p__Pseudomonadota; c__Gammaproteobacteria;"
            " o__Enterobacterales; f__Enterobacteriaceae;"
            " g__Escherichia; s__Escherichia coli"
        ),
        "seq3": "d__Bacteria; p__Bacillota; c__Bacilli; o__; f__; g__; s__",
        "seq4": (
            "d__Archaea; p__Euryarchaeota; c__Methanobacteria;"
            " o__Methanobacteriales; f__Methanobacteriaceae;"
            " g__Methanobacterium; s__Methanobacterium formicicum;"
            " t__Methanobacterium formicicum DSM 2320"
        ),
        "seq5": "d__Bacteria; p__Actinomycetota; c__; o__; f__; g__; s__",
    }
    lines = ["Feature ID\tTaxon"]
    for read_id, taxon in taxa.items():
        lines.append(f"{read_id}\t{taxon}")
    path.write_text("\n".join(lines) + "\n")
    return path


@pytest.fixture
def tree_file(tmp_path):
    """Create a newick tree with 5 tips matching test sequences."""
    nwk = "((seq1:0.1,seq2:0.2):0.3,(seq3:0.4,(seq4:0.5,seq5:0.6):0.7):0.8);"
    path = tmp_path / "tree.nwk"
    path.write_text(nwk)
    return path


# --- Sequences Parquet ---


async def test_load_job_produces_sequences_parquet(
    manifest_file, fasta_path, feature_map, tmp_path
):
    """Load job must produce a reference_sequences.parquet file."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
    )
    assert (output_dir / "reference_sequences.parquet").exists()


async def test_load_job_sequences_schema_and_data(manifest_file, fasta_path, feature_map, tmp_path):
    """Sequences Parquet must have correct columns and one row per sequence."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
    )

    pq_path = output_dir / "reference_sequences.parquet"
    with duckdb.connect(":memory:") as conn:
        cols = conn.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{pq_path}')"
        ).fetchall()
        col_names = [c[0] for c in cols]
        assert col_names == [
            "feature_idx",
            "sequence",
            "sequence_hash",
            "sequence_length_bp",
        ]

        rows = conn.execute(f"SELECT * FROM '{pq_path}' ORDER BY feature_idx").fetchall()
        assert len(rows) == 5

        # Verify a specific row: seq1 hash -> feature_idx 100
        seq1_hash = TEST_HASHES["seq1"]
        seq1_fidx = TEST_FEATURE_MAP[seq1_hash]
        row = conn.execute(
            "SELECT feature_idx, sequence, sequence_length_bp"
            f" FROM '{pq_path}' WHERE feature_idx = ?",
            [seq1_fidx],
        ).fetchone()
        assert row[0] == seq1_fidx
        assert row[1] == TEST_SEQUENCES["seq1"]
        assert row[2] == len(TEST_SEQUENCES["seq1"])


# --- Taxonomy Parquet ---


async def test_load_job_produces_taxonomy_parquet(
    manifest_file, fasta_path, feature_map, taxonomy_file, tmp_path
):
    """Load job must produce reference_taxonomy.parquet when taxonomy_path provided."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        taxonomy_path=taxonomy_file,
    )
    assert (output_dir / "reference_taxonomy.parquet").exists()


async def test_load_job_taxonomy_schema(
    manifest_file, fasta_path, feature_map, taxonomy_file, tmp_path
):
    """Taxonomy Parquet must have all expected columns including strain."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        taxonomy_path=taxonomy_file,
    )

    pq_path = output_dir / "reference_taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
        cols = conn.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{pq_path}')"
        ).fetchall()
        col_names = [c[0] for c in cols]
        assert col_names == [
            "reference_idx",
            "feature_idx",
            "domain",
            "phylum",
            "class",
            "order",
            "family",
            "genus",
            "species",
            "strain",
            "ncbi_taxon_id",
        ]

        distinct = conn.execute(f"SELECT DISTINCT reference_idx FROM '{pq_path}'").fetchall()
        assert distinct == [(REFERENCE_IDX,)]


async def test_load_job_taxonomy_parses_ranks(
    manifest_file, fasta_path, feature_map, taxonomy_file, tmp_path
):
    """Taxonomy Parquet must correctly parse GG2 rank-prefixed lineages."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        taxonomy_path=taxonomy_file,
    )

    pq_path = output_dir / "reference_taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
        seq1_fidx = TEST_FEATURE_MAP[TEST_HASHES["seq1"]]
        row = conn.execute(
            f"SELECT domain, phylum, genus, species FROM '{pq_path}' WHERE feature_idx = ?",
            [seq1_fidx],
        ).fetchone()
        assert row[0] == "Bacteria"
        assert row[1] == "Bacillota"
        assert row[2] == "Lactobacillus"
        assert row[3] == "Lactobacillus acidophilus"


async def test_load_job_taxonomy_strain_parsed(
    manifest_file, fasta_path, feature_map, taxonomy_file, tmp_path
):
    """Taxonomy Parquet must correctly parse t__ strain field when present."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        taxonomy_path=taxonomy_file,
    )

    pq_path = output_dir / "reference_taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
        # seq4 has a t__ strain field
        seq4_fidx = TEST_FEATURE_MAP[TEST_HASHES["seq4"]]
        row = conn.execute(
            f"SELECT species, strain FROM '{pq_path}' WHERE feature_idx = ?",
            [seq4_fidx],
        ).fetchone()
        assert row[0] == "Methanobacterium formicicum"
        assert row[1] == "Methanobacterium formicicum DSM 2320"

        # seq1 has no strain — should be NULL
        seq1_fidx = TEST_FEATURE_MAP[TEST_HASHES["seq1"]]
        row = conn.execute(
            f"SELECT strain FROM '{pq_path}' WHERE feature_idx = ?",
            [seq1_fidx],
        ).fetchone()
        assert row[0] is None


async def test_load_job_taxonomy_empty_ranks_are_null(
    manifest_file, fasta_path, feature_map, taxonomy_file, tmp_path
):
    """Empty rank prefixes (e.g., 'f__') must become NULL in Parquet."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        taxonomy_path=taxonomy_file,
    )

    pq_path = output_dir / "reference_taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
        seq3_fidx = TEST_FEATURE_MAP[TEST_HASHES["seq3"]]
        row = conn.execute(
            'SELECT domain, phylum, class, "order", family, genus, species'
            f" FROM '{pq_path}' WHERE feature_idx = ?",
            [seq3_fidx],
        ).fetchone()
        assert row[0] == "Bacteria"
        assert row[1] == "Bacillota"
        assert row[2] == "Bacilli"
        assert row[3] is None  # "o__" -> NULL
        assert row[4] is None  # "f__" -> NULL
        assert row[5] is None  # "g__" -> NULL
        assert row[6] is None  # "s__" -> NULL


async def test_load_job_taxonomy_partial_coverage(manifest_file, fasta_path, feature_map, tmp_path):
    """Taxonomy covering a subset of sequences is allowed (not all need taxonomy)."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    # Taxonomy for only 2 of 5 sequences
    tax_path = tmp_path / "partial_taxonomy.tsv"
    tax_path.write_text(
        "Feature ID\tTaxon\n"
        "seq1\td__Bacteria; p__Bacillota; c__; o__; f__; g__; s__\n"
        "seq2\td__Bacteria; p__Pseudomonadota; c__; o__; f__; g__; s__\n"
    )

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        taxonomy_path=tax_path,
    )

    pq_path = output_dir / "reference_taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
        count = conn.execute(f"SELECT count(*) FROM '{pq_path}'").fetchone()[0]
        assert count == 2


# --- Phylogeny Parquet ---


async def test_load_job_produces_phylogeny_parquet(
    manifest_file, fasta_path, feature_map, tree_file, tmp_path
):
    """Load job must produce reference_phylogeny.parquet when tree_path provided."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        tree_path=tree_file,
    )
    assert (output_dir / "reference_phylogeny.parquet").exists()


async def test_load_job_phylogeny_has_reference_idx_and_tips(
    manifest_file, fasta_path, feature_map, tree_file, tmp_path
):
    """Phylogeny Parquet must include reference_idx and mark tips correctly."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        tree_path=tree_file,
    )

    pq_path = output_dir / "reference_phylogeny.parquet"
    with duckdb.connect(":memory:") as conn:
        distinct = conn.execute(f"SELECT DISTINCT reference_idx FROM '{pq_path}'").fetchall()
        assert distinct == [(REFERENCE_IDX,)]

        tip_count = conn.execute(
            f"SELECT count(*) FROM '{pq_path}' WHERE is_tip = true"
        ).fetchone()[0]
        assert tip_count == 5

        tip_names = sorted(
            row[0]
            for row in conn.execute(f"SELECT name FROM '{pq_path}' WHERE is_tip = true").fetchall()
        )
        assert tip_names == ["seq1", "seq2", "seq3", "seq4", "seq5"]


async def test_load_job_phylogeny_schema(
    manifest_file, fasta_path, feature_map, tree_file, tmp_path
):
    """Phylogeny Parquet must have the correct column order."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        tree_path=tree_file,
    )

    pq_path = output_dir / "reference_phylogeny.parquet"
    with duckdb.connect(":memory:") as conn:
        cols = conn.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{pq_path}')"
        ).fetchall()
        col_names = [c[0] for c in cols]
        assert col_names == [
            "reference_idx",
            "node_index",
            "name",
            "branch_length",
            "edge_id",
            "parent_index",
            "is_tip",
        ]


# --- Tip features ---


async def test_load_job_produces_tip_features(
    manifest_file, fasta_path, feature_map, tree_file, tmp_path
):
    """Load job must produce tip_features.json mapping tips to feature_idx."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        tree_path=tree_file,
    )

    tip_path = output_dir / "tip_features.json"
    assert tip_path.exists()
    tips = json.loads(tip_path.read_text())
    assert len(tips) == 5

    for entry in tips:
        assert entry["reference_idx"] == REFERENCE_IDX
        assert isinstance(entry["node_index"], int)
        assert isinstance(entry["feature_idx"], int)
        assert entry["feature_idx"] in TEST_FEATURE_MAP.values()


# --- Optional files omitted ---


async def test_load_job_no_taxonomy_no_tree(manifest_file, fasta_path, feature_map, tmp_path):
    """Without optional files, only sequences Parquet is produced."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_file,
        fasta_path=fasta_path,
        feature_map=feature_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
    )
    assert (output_dir / "reference_sequences.parquet").exists()
    assert (output_dir / "reference_membership.parquet").exists()
    assert not (output_dir / "reference_taxonomy.parquet").exists()
    assert not (output_dir / "reference_phylogeny.parquet").exists()
    assert not (output_dir / "tip_features.json").exists()


# --- Error cases ---


async def test_load_job_rejects_missing_manifest(fasta_path, feature_map, tmp_path):
    """Load job must raise FileNotFoundError on missing manifest."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    with pytest.raises(FileNotFoundError):
        await backend.run_load_job(
            manifest_path=tmp_path / "nonexistent.json",
            fasta_path=fasta_path,
            feature_map=feature_map,
            output_dir=tmp_path / "output",
            reference_idx=REFERENCE_IDX,
        )


async def test_load_job_rejects_missing_fasta(manifest_file, feature_map, tmp_path):
    """Load job must raise FileNotFoundError on missing FASTA."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    with pytest.raises(FileNotFoundError):
        await backend.run_load_job(
            manifest_path=manifest_file,
            fasta_path=tmp_path / "nonexistent.fasta",
            feature_map=feature_map,
            output_dir=tmp_path / "output",
            reference_idx=REFERENCE_IDX,
        )


async def test_load_job_rejects_missing_jplace(manifest_file, fasta_path, feature_map, tmp_path):
    """Load job must raise FileNotFoundError on nonexistent jplace_path."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    with pytest.raises(FileNotFoundError):
        await backend.run_load_job(
            manifest_path=manifest_file,
            fasta_path=fasta_path,
            feature_map=feature_map,
            output_dir=tmp_path / "output",
            reference_idx=REFERENCE_IDX,
            jplace_path=tmp_path / "nonexistent.jplace",
        )


async def test_load_job_rejects_unmapped_hash(manifest_file, fasta_path, tmp_path):
    """Load job must raise ValueError when feature_map is missing hashes."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    with pytest.raises(ValueError, match="unmapped"):
        await backend.run_load_job(
            manifest_path=manifest_file,
            fasta_path=fasta_path,
            feature_map={},
            output_dir=tmp_path / "output",
            reference_idx=REFERENCE_IDX,
        )


async def test_load_job_rejects_unmatched_tips_without_jplace(
    manifest_file, fasta_path, feature_map, tmp_path
):
    """Tree tips not in manifest must raise ValueError when jplace_path is None."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    tree_path = tmp_path / "bad_tree.nwk"
    tree_path.write_text("((seq1:0.1,unknown_tip:0.2):0.3,seq2:0.4);")

    entries = [
        {
            "read_id": name,
            "sequence_hash": str(TEST_HASHES[name]),
            "length": len(TEST_SEQUENCES[name]),
        }
        for name in ["seq1", "seq2"]
    ]
    manifest = {"reference_idx": REFERENCE_IDX, "entries": entries}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    partial_map = {TEST_HASHES[name]: 100 + i for i, name in enumerate(["seq1", "seq2"])}

    fasta = tmp_path / "partial.fasta"
    fasta.write_text(">seq1\nATCGATCGATCG\n>seq2\nGCTAGCTAGCTA\n")

    backend = LocalBackend()
    with pytest.raises(ValueError, match="no matching sequence"):
        await backend.run_load_job(
            manifest_path=manifest_path,
            fasta_path=fasta,
            feature_map=partial_map,
            output_dir=tmp_path / "output",
            reference_idx=REFERENCE_IDX,
            tree_path=tree_path,
        )


async def test_load_job_allows_unmatched_tips_with_jplace(
    manifest_file, fasta_path, feature_map, tmp_path
):
    """Unmatched tips are allowed when jplace_path signals placements exist."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    tree_path = tmp_path / "tree_with_placed.nwk"
    tree_path.write_text("((seq1:0.1,placed_seq:0.2):0.3,seq2:0.4);")

    entries = [
        {
            "read_id": name,
            "sequence_hash": str(TEST_HASHES[name]),
            "length": len(TEST_SEQUENCES[name]),
        }
        for name in ["seq1", "seq2"]
    ]
    manifest = {"reference_idx": REFERENCE_IDX, "entries": entries}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    partial_map = {TEST_HASHES[name]: 100 + i for i, name in enumerate(["seq1", "seq2"])}

    fasta = tmp_path / "partial.fasta"
    fasta.write_text(">seq1\nATCGATCGATCG\n>seq2\nGCTAGCTAGCTA\n")

    jplace = tmp_path / "placements.jplace"
    jplace.write_text("{}")

    backend = LocalBackend()
    output_dir = tmp_path / "output"
    await backend.run_load_job(
        manifest_path=manifest_path,
        fasta_path=fasta,
        feature_map=partial_map,
        output_dir=output_dir,
        reference_idx=REFERENCE_IDX,
        tree_path=tree_path,
        jplace_path=jplace,
    )

    tips = json.loads((output_dir / "tip_features.json").read_text())
    assert len(tips) == 2
    tip_fidxs = {t["feature_idx"] for t in tips}
    assert tip_fidxs == {100, 101}


async def test_load_job_rejects_taxonomy_with_unknown_sequences(
    manifest_file, fasta_path, feature_map, tmp_path
):
    """Taxonomy entries for sequences not in manifest must raise ValueError."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    tax_path = tmp_path / "bad_taxonomy.tsv"
    tax_path.write_text(
        "Feature ID\tTaxon\n"
        "seq1\td__Bacteria; p__; c__; o__; f__; g__; s__\n"
        "not_in_manifest\td__Bacteria; p__; c__; o__; f__; g__; s__\n"
    )

    backend = LocalBackend()
    with pytest.raises(ValueError, match="not in manifest"):
        await backend.run_load_job(
            manifest_path=manifest_file,
            fasta_path=fasta_path,
            feature_map=feature_map,
            output_dir=tmp_path / "output",
            reference_idx=REFERENCE_IDX,
            taxonomy_path=tax_path,
        )


async def test_load_job_rejects_taxonomy_bad_header(
    manifest_file, fasta_path, feature_map, tmp_path
):
    """Taxonomy file with wrong header must raise ValueError."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    tax_path = tmp_path / "bad_header.tsv"
    tax_path.write_text("wrong_header\ttaxonomy\nseq1\td__Bacteria; p__; c__; o__; f__; g__; s__\n")

    backend = LocalBackend()
    with pytest.raises(ValueError, match="Unexpected taxonomy header"):
        await backend.run_load_job(
            manifest_path=manifest_file,
            fasta_path=fasta_path,
            feature_map=feature_map,
            output_dir=tmp_path / "output",
            reference_idx=REFERENCE_IDX,
            taxonomy_path=tax_path,
        )


# --- Taxonomy parser unit tests ---


def test_parse_taxonomy_rejects_wrong_prefix():
    """Taxonomy parser must reject misplaced rank prefixes."""
    from qiita_compute_orchestrator.backends.local import _parse_taxonomy

    with pytest.raises(ValueError, match="expected prefix"):
        _parse_taxonomy("p__Bacillota; d__Bacteria; c__; o__; f__; g__; s__")


def test_parse_taxonomy_rejects_blank_field():
    """Taxonomy parser must reject blank fields (no prefix)."""
    from qiita_compute_orchestrator.backends.local import _parse_taxonomy

    # Middle blank field (tab artifact or malformed data)
    with pytest.raises(ValueError, match="blank"):
        _parse_taxonomy("d__Bacteria; ; c__Bacilli; o__; f__; g__; s__")


def test_parse_taxonomy_rejects_too_many_fields():
    """Taxonomy parser must reject >8 semicolon-separated fields."""
    from qiita_compute_orchestrator.backends.local import _parse_taxonomy

    with pytest.raises(ValueError, match="expected at most 8"):
        _parse_taxonomy("d__X; p__X; c__X; o__X; f__X; g__X; s__X; t__X; extra")


def test_parse_taxonomy_with_strain():
    """Taxonomy parser must accept t__ strain as the 8th field."""
    from qiita_compute_orchestrator.backends.local import _parse_taxonomy

    result = _parse_taxonomy("d__Bacteria; p__X; c__X; o__X; f__X; g__X; s__X; t__strain1")
    assert len(result) == 8
    assert result[0] == "Bacteria"
    assert result[7] == "strain1"


def test_parse_taxonomy_7_fields_strain_is_none():
    """With only 7 fields (no t__), strain must be None."""
    from qiita_compute_orchestrator.backends.local import _parse_taxonomy

    result = _parse_taxonomy("d__Bacteria; p__X; c__X; o__X; f__X; g__X; s__X")
    assert len(result) == 8
    assert result[7] is None
