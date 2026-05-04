"""Tests for LocalBackend load job."""

import hashlib
from uuid import UUID

import duckdb
import pytest
from helpers import TEST_SEQUENCES

REFERENCE_IDX = 1


def _seq_hash(seq: str) -> UUID:
    return UUID(hashlib.md5(seq.encode()).hexdigest())


TEST_HASHES = {name: _seq_hash(seq) for name, seq in TEST_SEQUENCES.items()}

TEST_FEATURE_MAP: dict[UUID, int] = {
    hash_val: 100 + i for i, hash_val in enumerate(TEST_HASHES.values())
}


@pytest.fixture
def fasta_path(fasta_file):
    path, _ = fasta_file
    return path


@pytest.fixture
def manifest_file(tmp_path):
    """Manifest as Parquet — columns (read_id, sequence_hash UUID, length).
    Mirrors what LocalBackend._run_hash now writes."""
    rows = [(name, str(TEST_HASHES[name]), len(seq)) for name, seq in TEST_SEQUENCES.items()]
    path = tmp_path / "manifest.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TEMP TABLE m (read_id VARCHAR, sequence_hash UUID, length BIGINT)")
        conn.executemany("INSERT INTO m VALUES (?, ?::uuid, ?)", rows)
        conn.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")
    return path


@pytest.fixture
def feature_map_file(tmp_path):
    """Feature map as Parquet — columns (sequence_hash UUID, feature_idx BIGINT).
    Mirrors what library.mint_features now writes."""
    rows = [(str(h), fidx) for h, fidx in TEST_FEATURE_MAP.items()]
    path = tmp_path / "feature_map.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TEMP TABLE fm (sequence_hash UUID, feature_idx BIGINT)")
        conn.executemany("INSERT INTO fm VALUES (?::uuid, ?)", rows)
        conn.execute(f"COPY fm TO '{path}' (FORMAT PARQUET)")
    return path


@pytest.fixture
def taxonomy_file(tmp_path):
    """Taxonomy as Parquet with (feature_id, taxonomy) columns."""
    path = tmp_path / "taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE tax (feature_id VARCHAR, taxonomy VARCHAR)")
        conn.executemany(
            "INSERT INTO tax VALUES (?, ?)",
            [
                (
                    "seq1",
                    "d__Bacteria; p__Bacillota; c__Bacilli;"
                    " o__Lactobacillales; f__Lactobacillaceae;"
                    " g__Lactobacillus; s__Lactobacillus acidophilus",
                ),
                (
                    "seq2",
                    "d__Bacteria; p__Pseudomonadota; c__Gammaproteobacteria;"
                    " o__Enterobacterales; f__Enterobacteriaceae;"
                    " g__Escherichia; s__Escherichia coli",
                ),
                ("seq3", "d__Bacteria; p__Bacillota; c__Bacilli; o__; f__; g__; s__"),
                (
                    "seq4",
                    "d__Archaea; p__Euryarchaeota; c__Methanobacteria;"
                    " o__Methanobacteriales; f__Methanobacteriaceae;"
                    " g__Methanobacterium; s__Methanobacterium formicicum;"
                    " t__Methanobacterium formicicum DSM 2320",
                ),
                ("seq5", "d__Bacteria; p__Actinomycetota; c__; o__; f__; g__; s__"),
            ],
        )
        conn.execute(f"COPY tax TO '{path}' (FORMAT PARQUET)")
    return path


@pytest.fixture
def tree_file(tmp_path):
    nwk = "((seq1:0.1,seq2:0.2):0.3,(seq3:0.4,(seq4:0.5,seq5:0.6):0.7):0.8);"
    path = tmp_path / "tree.nwk"
    path.write_text(nwk)
    return path


async def _run_load(backend, manifest_file, fasta_path, feature_map_file, tmp_path, **kwargs):
    """Helper to run the load step with common args. Optional taxonomy_path /
    tree_path / jplace_path kwargs flow into the step's `inputs` dict."""
    output_dir = tmp_path / "output"
    inputs = {
        "manifest": manifest_file,
        "fasta_path": fasta_path,
        "feature_map": feature_map_file,
    }
    inputs.update(kwargs)
    await backend.run_step("load", inputs, output_dir, reference_idx=REFERENCE_IDX)
    return output_dir


# --- Sequence metadata ---


async def test_sequence_metadata_schema(manifest_file, fasta_path, feature_map_file, tmp_path):
    """reference_sequences.parquet must have feature_idx, sequence_hash, sequence_length_bp."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    out = await _run_load(LocalBackend(), manifest_file, fasta_path, feature_map_file, tmp_path)
    pq = out / "reference_sequences.parquet"
    assert pq.exists()

    with duckdb.connect(":memory:") as conn:
        cols = [
            r[0]
            for r in conn.execute(
                f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{pq}')"
            ).fetchall()
        ]
        assert cols == ["feature_idx", "sequence_hash", "sequence_length_bp"]
        assert conn.execute(f"SELECT count(*) FROM '{pq}'").fetchone()[0] == 5


# --- Sequence chunks ---


async def test_sequence_chunks_schema(manifest_file, fasta_path, feature_map_file, tmp_path):
    """reference_sequence_chunks.parquet must have feature_idx, chunk_index, chunk_data."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    out = await _run_load(LocalBackend(), manifest_file, fasta_path, feature_map_file, tmp_path)
    pq = out / "reference_sequence_chunks.parquet"
    assert pq.exists()

    with duckdb.connect(":memory:") as conn:
        cols = [
            r[0]
            for r in conn.execute(
                f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{pq}')"
            ).fetchall()
        ]
        assert cols == ["feature_idx", "chunk_index", "chunk_data"]
        # Short sequences (≤64KB) should each be 1 chunk
        assert conn.execute(f"SELECT count(*) FROM '{pq}'").fetchone()[0] == 5


async def test_sequence_chunks_reassemble(manifest_file, fasta_path, feature_map_file, tmp_path):
    """Chunked sequences must reassemble to the original sequence."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    out = await _run_load(LocalBackend(), manifest_file, fasta_path, feature_map_file, tmp_path)
    seq1_fidx = TEST_FEATURE_MAP[TEST_HASHES["seq1"]]

    with duckdb.connect(":memory:") as conn:
        row = conn.execute(
            "SELECT string_agg(chunk_data, '' ORDER BY chunk_index) AS seq"
            f" FROM '{out / 'reference_sequence_chunks.parquet'}'"
            f" WHERE feature_idx = {seq1_fidx}"
        ).fetchone()
        assert row[0] == TEST_SEQUENCES["seq1"]


# --- Membership ---


async def test_membership_parquet(manifest_file, fasta_path, feature_map_file, tmp_path):
    """reference_membership.parquet must have one row per feature."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    out = await _run_load(LocalBackend(), manifest_file, fasta_path, feature_map_file, tmp_path)
    pq = out / "reference_membership.parquet"
    assert pq.exists()

    with duckdb.connect(":memory:") as conn:
        count = conn.execute(f"SELECT count(*) FROM '{pq}'").fetchone()[0]
        assert count == 5
        ref = conn.execute(f"SELECT DISTINCT reference_idx FROM '{pq}'").fetchone()[0]
        assert ref == REFERENCE_IDX


# --- Taxonomy ---


async def test_taxonomy_parquet(
    manifest_file, fasta_path, feature_map_file, taxonomy_file, tmp_path
):
    """Taxonomy Parquet must parse GG2 ranks correctly from Parquet input."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    out = await _run_load(
        LocalBackend(),
        manifest_file,
        fasta_path,
        feature_map_file,
        tmp_path,
        taxonomy_path=taxonomy_file,
    )
    pq = out / "reference_taxonomy.parquet"
    assert pq.exists()

    with duckdb.connect(":memory:") as conn:
        assert conn.execute(f"SELECT count(*) FROM '{pq}'").fetchone()[0] == 5

        seq1_fidx = TEST_FEATURE_MAP[TEST_HASHES["seq1"]]
        row = conn.execute(
            f"SELECT domain, phylum FROM '{pq}' WHERE feature_idx = ?",
            [seq1_fidx],
        ).fetchone()
        assert row[0] == "Bacteria"
        assert row[1] == "Bacillota"

        # seq4 has strain
        seq4_fidx = TEST_FEATURE_MAP[TEST_HASHES["seq4"]]
        strain = conn.execute(
            f"SELECT strain FROM '{pq}' WHERE feature_idx = ?",
            [seq4_fidx],
        ).fetchone()[0]
        assert strain == "Methanobacterium formicicum DSM 2320"

        # seq3 has empty ranks → NULL
        seq3_fidx = TEST_FEATURE_MAP[TEST_HASHES["seq3"]]
        row = conn.execute(
            f"SELECT \"order\", family FROM '{pq}' WHERE feature_idx = ?",
            [seq3_fidx],
        ).fetchone()
        assert row[0] is None
        assert row[1] is None


# --- Phylogeny ---


async def test_phylogeny_has_feature_idx_on_tips(
    manifest_file, fasta_path, feature_map_file, tree_file, tmp_path
):
    """Phylogeny tips must have feature_idx populated, internal nodes NULL."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    out = await _run_load(
        LocalBackend(),
        manifest_file,
        fasta_path,
        feature_map_file,
        tmp_path,
        tree_path=tree_file,
    )
    pq = out / "reference_phylogeny.parquet"
    assert pq.exists()

    with duckdb.connect(":memory:") as conn:
        # All 5 tips should have non-NULL feature_idx
        tips_with_fidx = conn.execute(
            f"SELECT count(*) FROM '{pq}' WHERE is_tip AND feature_idx IS NOT NULL"
        ).fetchone()[0]
        assert tips_with_fidx == 5

        # Internal nodes should have NULL feature_idx
        internal_with_fidx = conn.execute(
            f"SELECT count(*) FROM '{pq}' WHERE NOT is_tip AND feature_idx IS NOT NULL"
        ).fetchone()[0]
        assert internal_with_fidx == 0

        # feature_idx values must match what we minted
        tip_fidxs = set(
            r[0] for r in conn.execute(f"SELECT feature_idx FROM '{pq}' WHERE is_tip").fetchall()
        )
        assert tip_fidxs == set(TEST_FEATURE_MAP.values())


async def test_phylogeny_allows_unmatched_tips(
    manifest_file, fasta_path, feature_map_file, tmp_path
):
    """Tips without matching sequences get NULL feature_idx (no error)."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    tree_path = tmp_path / "tree_extra.nwk"
    tree_path.write_text("((seq1:0.1,unknown_tip:0.2):0.3,seq2:0.4);")

    # Manifest with only seq1 and seq2 — written as Parquet to match the
    # production format the load step expects.
    manifest_rows = [(n, str(TEST_HASHES[n]), len(TEST_SEQUENCES[n])) for n in ["seq1", "seq2"]]
    manifest_path = tmp_path / "partial_manifest.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TEMP TABLE m (read_id VARCHAR, sequence_hash UUID, length BIGINT)")
        conn.executemany("INSERT INTO m VALUES (?, ?::uuid, ?)", manifest_rows)
        conn.execute(f"COPY m TO '{manifest_path}' (FORMAT PARQUET)")

    fasta = tmp_path / "partial.fasta"
    fasta.write_text(">seq1\nATCGATCGATCG\n>seq2\nGCTAGCTAGCTA\n")

    partial_fm = tmp_path / "partial_fm.parquet"
    fm_rows = [(str(TEST_HASHES[n]), 100 + idx) for idx, n in enumerate(["seq1", "seq2"])]
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TEMP TABLE fm (sequence_hash UUID, feature_idx BIGINT)")
        conn.executemany("INSERT INTO fm VALUES (?::uuid, ?)", fm_rows)
        conn.execute(f"COPY fm TO '{partial_fm}' (FORMAT PARQUET)")

    out = tmp_path / "output"
    backend = LocalBackend()
    await backend.run_step(
        "load",
        {
            "manifest": manifest_path,
            "fasta_path": fasta,
            "feature_map": partial_fm,
            "tree_path": tree_path,
        },
        out,
        reference_idx=REFERENCE_IDX,
    )

    pq = out / "reference_phylogeny.parquet"
    with duckdb.connect(":memory:") as conn:
        # 3 tips total (seq1, unknown_tip, seq2), 2 with feature_idx
        tips = conn.execute(f"SELECT count(*) FROM '{pq}' WHERE is_tip").fetchone()[0]
        assert tips == 3
        matched = conn.execute(
            f"SELECT count(*) FROM '{pq}' WHERE is_tip AND feature_idx IS NOT NULL"
        ).fetchone()[0]
        assert matched == 2


# --- Optional files omitted ---


async def test_no_optional_files(manifest_file, fasta_path, feature_map_file, tmp_path):
    """Without optional files, only sequences + membership are produced."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    out = await _run_load(LocalBackend(), manifest_file, fasta_path, feature_map_file, tmp_path)
    assert (out / "reference_sequences.parquet").exists()
    assert (out / "reference_sequence_chunks.parquet").exists()
    assert (out / "reference_membership.parquet").exists()
    assert not (out / "reference_taxonomy.parquet").exists()
    assert not (out / "reference_phylogeny.parquet").exists()
    assert not (out / "reference_placements.parquet").exists()


# --- Error cases ---


async def test_rejects_missing_manifest(fasta_path, feature_map_file, tmp_path):
    from qiita_compute_orchestrator.backends.local import LocalBackend

    with pytest.raises(FileNotFoundError):
        await LocalBackend().run_step(
            "load",
            {
                "manifest": tmp_path / "nope.parquet",
                "fasta_path": fasta_path,
                "feature_map": feature_map_file,
            },
            tmp_path / "out",
            reference_idx=REFERENCE_IDX,
        )


async def test_rejects_unmapped_hash(manifest_file, fasta_path, tmp_path):
    from qiita_compute_orchestrator.backends.local import LocalBackend

    # Empty feature map — Parquet with the right schema but zero rows.
    empty_fm = tmp_path / "empty.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TEMP TABLE fm (sequence_hash UUID, feature_idx BIGINT)")
        conn.execute(f"COPY fm TO '{empty_fm}' (FORMAT PARQUET)")

    with pytest.raises(ValueError, match="unmapped"):
        await LocalBackend().run_step(
            "load",
            {
                "manifest": manifest_file,
                "fasta_path": fasta_path,
                "feature_map": empty_fm,
            },
            tmp_path / "out",
            reference_idx=REFERENCE_IDX,
        )
