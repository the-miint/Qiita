"""Tests for the reference_load native job.

Calls ``execute()`` directly (not through LocalBackend / run_native_job)
so failures point at the re-key + lift logic, not framework wiring. The
upstream fixtures synthesize hash_sequences' Parquets (manifest, the
hash-keyed reference_sequence, the hash-keyed chunks) plus
mint-features' feature_map. This file does not depend on a real
hash_sequences invocation — that integration is covered separately;
here the contract under test is "given these four Parquets and these
optional inputs, produce the six DuckLake-shape staging files."

Fixtures avoid any FASTA-on-disk path: hash_sequences reads from a
DoPut'd Parquet, and reference_load reads from hash_sequences' outputs,
so no test file in this suite handles a FASTA.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path
from uuid import UUID

import duckdb
import pytest

_REFERENCE_LOAD_LOGGER = "qiita_compute_orchestrator.jobs.reference_load"

# Canonical test sequences shared across hash_sequences and the
# reference-load suite. Five short sequences mean every chunk is a
# single row, which is fine here — the multi-chunk path is covered in
# test_hash_sequences.py.
_TEST_SEQUENCES = {
    "seq1": "ATCGATCGATCG",
    "seq2": "GCTAGCTAGCTA",
    "seq3": "AAATTTTCCCGGG",
    "seq4": "TTTTAAAACCCC",
    "seq5": "GGGGCCCCAAAA",
}


def _canonical(seq: str) -> str:
    """Mirror hash_sequences' canonical form: LEAST(upper, miint revcomp).
    Tests construct fixtures with the canonical form directly so they
    don't have to depend on miint being installed."""
    rc = seq.translate(str.maketrans("ACGTRYSWKMBDHVN", "TGCAYRSWMKVHDBN"))[::-1].upper()
    up = seq.upper()
    return min(up, rc)


_CANON = {name: _canonical(seq) for name, seq in _TEST_SEQUENCES.items()}
_HASHES = {name: UUID(hashlib.md5(c.encode()).hexdigest()) for name, c in _CANON.items()}
# Distinct feature_idx per sequence_hash, base 100 so the values are
# visibly distinct from chunk_index and other small integers in
# assertions.
_FEATURE_MAP: dict[UUID, int] = {h: 100 + i for i, h in enumerate(_HASHES.values())}

_REFERENCE_IDX = 7

# Full-coverage semicolon-delimited taxonomy strings, one per test
# sequence. seq3 truncates to class (empty ranks past c__); seq4 carries
# a strain (t__). Shared by the full-coverage `taxonomy_path` fixture and
# the coverage-gap tests, which build subsets/supersets from it.
_TAXONOMY_STRINGS = {
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


def _run(inputs, workspace) -> dict:
    from qiita_compute_orchestrator.jobs.reference_load import execute

    return asyncio.run(execute(inputs, workspace))


def _write_parquet(path: Path, schema_sql: str, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"CREATE TEMP TABLE t ({schema_sql})")
        if rows:
            placeholders = ", ".join("?" for _ in rows[0])
            conn.executemany(f"INSERT INTO t VALUES ({placeholders})", rows)
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")


def _write_taxonomy_parquet(path: Path, entries: list[tuple[str, str]]) -> Path:
    """Write a `(feature_id, taxonomy)` staging Parquet from a list of
    (feature_id, semicolon-delimited-rank-string) rows."""
    _write_parquet(path, "feature_id VARCHAR, taxonomy VARCHAR", entries)
    return path


@pytest.fixture
def staging_inputs(tmp_path):
    """Synthesize hash_sequences' two Parquets + mint-features'
    feature_map. Returns a dict the caller spreads into the Inputs
    model."""
    manifest_path = tmp_path / "manifest.parquet"
    _write_parquet(
        manifest_path,
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [(name, str(_HASHES[name]), len(_TEST_SEQUENCES[name])) for name in _TEST_SEQUENCES],
    )

    feature_map_path = tmp_path / "feature_map.parquet"
    _write_parquet(
        feature_map_path,
        "sequence_hash UUID, feature_idx BIGINT",
        [(str(h), fidx) for h, fidx in _FEATURE_MAP.items()],
    )

    # hash_sequences emits reference_sequence_chunks as a DIRECTORY of
    # `part_*.parquet` files (avoids the single-writer OOM at GG2 scale).
    # Test fixture matches that contract: one directory, one part inside.
    # Chunks carry the source (unfolded) bytes of each upload read; the
    # canonical hash lives on the row but the chunk_data is what was
    # uploaded. For the test fixture we use _CANON[name] as a stand-in
    # for the upload bytes (single-strand inputs in this fixture).
    reference_sequence_chunks_dir = tmp_path / "reference_sequence_chunks"
    reference_sequence_chunks_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(
        reference_sequence_chunks_dir / "part_00000.parquet",
        "sequence_hash UUID, chunk_index INTEGER, chunk_data VARCHAR",
        [(str(_HASHES[name]), 0, _CANON[name]) for name in _TEST_SEQUENCES],
    )

    return {
        "manifest": manifest_path,
        "feature_map": feature_map_path,
        "reference_sequence_chunks": reference_sequence_chunks_dir,
    }


def _inputs(**kwargs):
    from qiita_compute_orchestrator.jobs.reference_load import Inputs

    base = {"reference_idx": _REFERENCE_IDX, "work_ticket_idx": 1}
    base.update(kwargs)
    return Inputs(**base)


# =============================================================================
# Sequence + membership outputs (always written)
# =============================================================================


def test_emits_feature_idx_keyed_sequences(staging_inputs, tmp_path):
    """reference_sequences.parquet is keyed by feature_idx — the
    hash → feature_idx re-key happens here, not in hash_sequences."""
    outputs = _run(_inputs(**staging_inputs), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_sequences.parquet"
    assert pq.exists()
    with duckdb.connect(":memory:") as conn:
        cols = {c[0]: c[1] for c in conn.execute(f"DESCRIBE SELECT * FROM '{pq}'").fetchall()}
        assert cols == {
            "feature_idx": "BIGINT",
            "sequence_hash": "UUID",
            "sequence_length_bp": "BIGINT",
        }
        rows = conn.execute(
            f"SELECT feature_idx, CAST(sequence_hash AS VARCHAR), sequence_length_bp"
            f" FROM '{pq}' ORDER BY feature_idx"
        ).fetchall()
    expected = sorted(
        (fidx, str(h), len(next(seq for n, seq in _TEST_SEQUENCES.items() if _HASHES[n] == h)))
        for h, fidx in _FEATURE_MAP.items()
    )
    assert rows == expected


def test_emits_feature_idx_keyed_chunks(staging_inputs, tmp_path):
    """reference_sequence_chunks/ is a directory of part_*.parquet
    files keyed by feature_idx; the chunk_data column carries the
    canonical sequence verbatim. The runner's register-files
    convention picks this directory up as a multi-file DuckLake
    table."""
    outputs = _run(_inputs(**staging_inputs), tmp_path / "ws")
    chunks_dir = outputs["staging_dir"] / "reference_sequence_chunks"
    assert chunks_dir.is_dir()
    parts = sorted(chunks_dir.glob("part_*.parquet"))
    assert parts, "expected at least one part file"
    parts_glob = str(chunks_dir / "part_*.parquet")
    with duckdb.connect(":memory:") as conn:
        cols = {
            c[0]: c[1]
            for c in conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [parts_glob]).fetchall()
        }
        assert cols == {
            "feature_idx": "BIGINT",
            "chunk_index": "INTEGER",
            "chunk_data": "VARCHAR",
        }
        # Reassemble per feature_idx → canonical form.
        rows = conn.execute(
            "SELECT feature_idx, string_agg(chunk_data, '' ORDER BY chunk_index)"
            " FROM read_parquet(?) GROUP BY feature_idx ORDER BY feature_idx",
            [parts_glob],
        ).fetchall()
    by_name = {_HASHES[n]: _CANON[n] for n in _TEST_SEQUENCES}
    fidx_to_canon = {_FEATURE_MAP[h]: c for h, c in by_name.items()}
    assert {fidx: canon for fidx, canon in rows} == fidx_to_canon


def test_returns_feature_keyed_chunks_binding(staging_inputs, tmp_path):
    """execute() exposes the feature-keyed chunks directory as its own
    `reference_sequence_chunks` binding (in addition to `staging_dir`).

    host-reference-add's build_rype_index step consumes this binding to feed
    rype the feature-keyed chunks. It must point at the same on-disk dir
    `register-files` registers as the multi-file `reference_sequence_chunks`
    DuckLake table — and it must be read BEFORE register-files, which MOVES
    those part files into permanent storage."""
    outputs = _run(_inputs(**staging_inputs), tmp_path / "ws")
    chunks = outputs["reference_sequence_chunks"]
    assert chunks == outputs["staging_dir"] / "reference_sequence_chunks"
    assert chunks.is_dir()
    assert sorted(chunks.glob("part_*.parquet")), "binding must point at the part files"


def test_emits_reference_membership_with_reference_idx(staging_inputs, tmp_path):
    """reference_membership.parquet ties every feature_idx to this run's
    reference_idx — one row per minted feature."""
    outputs = _run(_inputs(**staging_inputs), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_membership.parquet"
    assert pq.exists()
    with duckdb.connect(":memory:") as conn:
        cols = {c[0]: c[1] for c in conn.execute(f"DESCRIBE SELECT * FROM '{pq}'").fetchall()}
        assert cols == {"reference_idx": "BIGINT", "feature_idx": "BIGINT"}
        ref_idxs = {
            r[0] for r in conn.execute(f"SELECT DISTINCT reference_idx FROM '{pq}'").fetchall()
        }
        assert ref_idxs == {_REFERENCE_IDX}
        feature_idxs = {r[0] for r in conn.execute(f"SELECT feature_idx FROM '{pq}'").fetchall()}
        assert feature_idxs == set(_FEATURE_MAP.values())


def test_omits_optional_outputs_when_paths_unset(staging_inputs, tmp_path):
    """With no taxonomy / tree / jplace inputs, only the three required
    staging outputs are emitted (chunks is a directory of parts)."""
    outputs = _run(_inputs(**staging_inputs), tmp_path / "ws")
    staging = outputs["staging_dir"]
    assert (staging / "reference_sequences.parquet").exists()
    chunks_dir = staging / "reference_sequence_chunks"
    assert chunks_dir.is_dir()
    assert sorted(chunks_dir.glob("part_*.parquet")), "expected at least one part"
    assert (staging / "reference_membership.parquet").exists()
    assert not (staging / "reference_taxonomy.parquet").exists()
    assert not (staging / "reference_phylogeny.parquet").exists()
    assert not (staging / "reference_placements.parquet").exists()


# =============================================================================
# Optional inputs
# =============================================================================


@pytest.fixture
def taxonomy_path(tmp_path):
    """Full-coverage taxonomy: one row per test sequence (seq1-5)."""
    return _write_taxonomy_parquet(
        tmp_path / "taxonomy.parquet",
        [(name, _TAXONOMY_STRINGS[name]) for name in _TEST_SEQUENCES],
    )


def test_taxonomy_lifted_writer_keys_by_feature_idx(
    staging_inputs, taxonomy_path, tmp_path, caplog
):
    """Taxonomy writer JOINs taxonomy.feature_id → manifest.read_id →
    feature_idx, parses semicolon ranks, blanks NULL out. Full coverage
    (5/5 supplied) writes one row per feature and emits NO warning."""
    with caplog.at_level(logging.WARNING, logger=_REFERENCE_LOAD_LOGGER):
        outputs = _run(_inputs(**staging_inputs, taxonomy_path=taxonomy_path), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_taxonomy.parquet"
    assert pq.exists()
    with duckdb.connect(":memory:") as conn:
        # One row per feature — 1-1 at rest.
        assert conn.execute(f"SELECT count(*) FROM '{pq}'").fetchone()[0] == len(_FEATURE_MAP)
        # seq3 has empty ranks past class — they must come back NULL.
        seq3_fidx = _FEATURE_MAP[_HASHES["seq3"]]
        row = conn.execute(
            f"SELECT \"order\", family, strain FROM '{pq}' WHERE feature_idx = {seq3_fidx}"
        ).fetchone()
        assert row == (None, None, None)
        # seq4 carries a strain.
        seq4_fidx = _FEATURE_MAP[_HASHES["seq4"]]
        strain = conn.execute(
            f"SELECT strain FROM '{pq}' WHERE feature_idx = {seq4_fidx}"
        ).fetchone()[0]
        assert strain == "Methanobacterium formicicum DSM 2320"
    # Full coverage → no coverage-gap / stray / duplicate warnings.
    assert [r for r in caplog.records if r.name == _REFERENCE_LOAD_LOGGER] == []


def _taxonomy_warnings(caplog) -> list[str]:
    """Lower-cased WARNING messages emitted by the reference_load logger."""
    return [
        r.getMessage().lower()
        for r in caplog.records
        if r.name == _REFERENCE_LOAD_LOGGER and r.levelno == logging.WARNING
    ]


def test_unclassified_feature_recorded_as_null_rank_row(staging_inputs, tmp_path, caplog):
    """A feature with no supplied taxonomy is recorded at rest as a
    NULL-rank row (reference_taxonomy stays 1-1 with features), and the
    coverage gap is warned — not silently dropped (the old INNER JOIN)."""
    tax = _write_taxonomy_parquet(
        tmp_path / "taxonomy.parquet",
        [(name, _TAXONOMY_STRINGS[name]) for name in ("seq1", "seq2", "seq3", "seq4")],
    )
    with caplog.at_level(logging.WARNING, logger=_REFERENCE_LOAD_LOGGER):
        outputs = _run(_inputs(**staging_inputs, taxonomy_path=tax), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
        # One row per feature — the unclassified seq5 is present, not dropped.
        assert conn.execute(f"SELECT count(*) FROM '{pq}'").fetchone()[0] == len(_FEATURE_MAP)
        seq5_fidx = _FEATURE_MAP[_HASHES["seq5"]]
        row = conn.execute(
            'SELECT domain, phylum, class, "order", family, genus, species, strain'
            f" FROM '{pq}' WHERE feature_idx = {seq5_fidx}"
        ).fetchone()
        assert row == (None,) * 8
    assert any("unclassified" in w for w in _taxonomy_warnings(caplog))


def test_stray_taxonomy_rows_warn_and_are_dropped(staging_inputs, tmp_path, caplog):
    """Supplied taxonomy rows whose feature_id is not a sequence read_id
    (the ID-namespace-mismatch class) are dropped and warned loudly —
    the classified features are still written."""
    tax = _write_taxonomy_parquet(
        tmp_path / "taxonomy.parquet",
        [(name, _TAXONOMY_STRINGS[name]) for name in _TEST_SEQUENCES]
        + [("NZ_CP039371.1", "d__Bacteria; p__Bacillota; c__Bacilli; o__; f__; g__; s__")],
    )
    with caplog.at_level(logging.WARNING, logger=_REFERENCE_LOAD_LOGGER):
        outputs = _run(_inputs(**staging_inputs, taxonomy_path=tax), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
        # Only the 5 real features — the stray read_id has no feature_idx.
        assert conn.execute(f"SELECT count(*) FROM '{pq}'").fetchone()[0] == len(_FEATURE_MAP)
        seq1_fidx = _FEATURE_MAP[_HASHES["seq1"]]
        dom = conn.execute(f"SELECT domain FROM '{pq}' WHERE feature_idx = {seq1_fidx}").fetchone()[
            0
        ]
        assert dom == "Bacteria"
    warnings = _taxonomy_warnings(caplog)
    assert any("stray" in w or "unmatched" in w for w in warnings)


def test_duplicate_supplied_taxonomy_warns_and_stays_one_to_one(staging_inputs, tmp_path, caplog):
    """A supplied taxonomy with two rows for the same feature_id collapses
    to exactly one reference_taxonomy row (1-1 at rest) and warns."""
    tax = _write_taxonomy_parquet(
        tmp_path / "taxonomy.parquet",
        [(name, _TAXONOMY_STRINGS[name]) for name in _TEST_SEQUENCES]
        + [("seq1", _TAXONOMY_STRINGS["seq1"])],
    )
    with caplog.at_level(logging.WARNING, logger=_REFERENCE_LOAD_LOGGER):
        outputs = _run(_inputs(**staging_inputs, taxonomy_path=tax), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
        seq1_fidx = _FEATURE_MAP[_HASHES["seq1"]]
        assert (
            conn.execute(f"SELECT count(*) FROM '{pq}' WHERE feature_idx = {seq1_fidx}").fetchone()[
                0
            ]
            == 1
        )
        assert conn.execute(f"SELECT count(*) FROM '{pq}'").fetchone()[0] == len(_FEATURE_MAP)
    assert any("duplicate" in w for w in _taxonomy_warnings(caplog))


@pytest.mark.parametrize(
    ("bad_taxonomy", "match"),
    [
        ("d__A; p__B; c__C; o__D; f__E; g__F; s__G; t__H; x__I", "8 semicolon"),
        ("d__Bacteria; ; c__Bacilli", "blank fields"),
        ("p__Bacillota; d__Bacteria", "wrong rank prefix"),
    ],
)
def test_malformed_supplied_taxonomy_still_raises(staging_inputs, tmp_path, bad_taxonomy, match):
    """The format checks on *supplied* content stay hard ValueErrors —
    coverage warnings must not soften them."""
    tax = _write_taxonomy_parquet(tmp_path / "taxonomy.parquet", [("seq1", bad_taxonomy)])
    with pytest.raises(ValueError, match=match):
        _run(_inputs(**staging_inputs, taxonomy_path=tax), tmp_path / "ws")


_CHUNK_SIZE = 65_536


def _wrap_chunked_blob_parquet(path: Path, payload: bytes) -> Path:
    """Write a chunked-blob upload Parquet `(chunk_index INTEGER,
    chunk_data BLOB)` via DuckDB. Matches the CLI's DoPut wire shape
    for opaque binary inputs (Newick / jplace); reference_load stitches
    chunks back into a temp file via `_unwrap_chunks_to_temp_file`."""
    chunks = [
        (i // _CHUNK_SIZE, payload[i : i + _CHUNK_SIZE])
        for i in range(0, len(payload), _CHUNK_SIZE)
    ]
    if not chunks:
        chunks = [(0, b"")]
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TEMP TABLE wrapped (chunk_index INTEGER, chunk_data BLOB)")
        for idx, data in chunks:
            conn.execute("INSERT INTO wrapped VALUES (?, ?)", [idx, data])
        conn.execute(f"COPY wrapped TO '{path}' (FORMAT PARQUET)")
    return path


@pytest.fixture
def tree_path(tmp_path):
    """Wrap a Newick tree in the CLI's chunked DoPut shape. reference_load
    stitches chunks back into a temp `.nwk` file before passing to
    miint's `read_newick`."""
    nwk_text = b"((seq1:0.1,seq2:0.2):0.3,(seq3:0.4,(seq4:0.5,seq5:0.6):0.7):0.8);"
    return _wrap_chunked_blob_parquet(tmp_path / "tree_upload.parquet", nwk_text)


def test_phylogeny_lifted_writer_populates_tip_feature_idx(staging_inputs, tree_path, tmp_path):
    """Tip nodes whose name matches a manifest read_id carry feature_idx;
    internal nodes (NULL `is_tip`) and unmatched tips carry NULL."""
    outputs = _run(_inputs(**staging_inputs, tree_path=tree_path), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_phylogeny.parquet"
    assert pq.exists()
    with duckdb.connect(":memory:") as conn:
        tips_with_fidx = {
            r[0]
            for r in conn.execute(
                f"SELECT feature_idx FROM '{pq}' WHERE is_tip AND feature_idx IS NOT NULL"
            ).fetchall()
        }
        assert tips_with_fidx == set(_FEATURE_MAP.values())
        internal_with_fidx = conn.execute(
            f"SELECT count(*) FROM '{pq}' WHERE NOT is_tip AND feature_idx IS NOT NULL"
        ).fetchone()[0]
        assert internal_with_fidx == 0


@pytest.fixture
def jplace_path(tmp_path):
    """jplace input wrapped in the CLI's chunked DoPut shape.
    reference_load stitches chunks back to a temp `.jplace` file before
    passing to miint's `read_jplace`."""
    jplace_text = (
        b'{"version": 3, "tree": "((seq1:0.1{0},seq2:0.2{1}):0.3{2}):0.4{3};",'
        b' "placements": ['
        b'   {"p": [[5, -1.0, 0.9, 0.01, 0.02]], "nm": [["seq1", 1]]},'
        b'   {"p": [[7, -2.0, 0.8, 0.03, 0.04]], "nm": [["seq2", 1]]}'
        b" ],"
        b' "fields": ["edge_num", "likelihood", "like_weight_ratio",'
        b'            "distal_length", "pendant_length"],'
        b' "metadata": {}}'
    )
    return _wrap_chunked_blob_parquet(tmp_path / "jplace_upload.parquet", jplace_text)


def test_placements_lifted_writer_maps_fragment_to_feature_idx(
    staging_inputs, jplace_path, tmp_path
):
    """Placements writer maps `fragment` → manifest.read_id → feature_idx;
    rows whose fragment isn't in id_map are silently dropped (a jplace
    may carry fragments outside the current reference's mint scope)."""
    outputs = _run(_inputs(**staging_inputs, jplace_path=jplace_path), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_placements.parquet"
    assert pq.exists()
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT feature_idx, edge_num FROM '{pq}' ORDER BY feature_idx"
        ).fetchall()
        expected = sorted([(_FEATURE_MAP[_HASHES["seq1"]], 5), (_FEATURE_MAP[_HASHES["seq2"]], 7)])
        assert rows == expected


# =============================================================================
# Failure paths
# =============================================================================


def test_unmapped_sequence_hash_raises_value_error(staging_inputs, tmp_path):
    """If the manifest carries a hash that feature_map doesn't cover,
    re-keying would silently drop the row — surface as ValueError
    (BAD_INPUT one layer up via the framework dispatcher) instead."""
    bad_fm = tmp_path / "incomplete_fm.parquet"
    _write_parquet(
        bad_fm,
        "sequence_hash UUID, feature_idx BIGINT",
        # Only one of the five sequences mapped → four unmapped.
        [(str(_HASHES["seq1"]), 100)],
    )
    inputs = dict(staging_inputs)
    inputs["feature_map"] = bad_fm

    with pytest.raises(ValueError, match="unmapped sequence hash"):
        _run(_inputs(**inputs), tmp_path / "ws")


def test_missing_manifest_raises_file_not_found(staging_inputs, tmp_path):
    inputs = dict(staging_inputs)
    inputs["manifest"] = tmp_path / "does-not-exist.parquet"
    with pytest.raises(FileNotFoundError):
        _run(_inputs(**inputs), tmp_path / "ws")


def test_missing_optional_taxonomy_raises_file_not_found(staging_inputs, tmp_path):
    """When `taxonomy_path` is set but the file is missing, fail fast —
    a None vs missing-file distinction matters: None means the workflow
    didn't supply taxonomy at all; missing-file means it did but the
    staged input vanished. The runner expects loud failure on the
    latter."""
    with pytest.raises(FileNotFoundError):
        _run(
            _inputs(**staging_inputs, taxonomy_path=tmp_path / "missing.parquet"),
            tmp_path / "ws",
        )
