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
import json
import logging
from pathlib import Path
from uuid import UUID

import duckdb
import pytest
from qiita_common.chunking import reassemble_chunks_expr

_REFERENCE_LOAD_LOGGER = "qiita_compute_orchestrator.jobs.reference_load"


@pytest.fixture(autouse=True)
def _host_ram_backs_the_load_literal(monkeypatch):
    """Run `load` off SLURM on a host big enough for its memory literal.

    Off SLURM, `resolve_duckdb_memory_gb` bounds `load`'s DuckDB limit by
    detected RAM, and on a small runner that bound falls below what
    `read_jplace` needs: its macro passes `maximum_object_size=1000000000` to
    DuckDB's `read_json`
    (https://github.com/the-miint/duckdb-miint/blob/96630a5/src/include/miint_macros.hpp),
    which on DuckDB 1.5.4 asks for about 1.8 GiB even for a 327-byte jplace.
    Pinning RAM keeps these tests about what `load` writes, not about the
    machine they run on; the RAM bound has tests of its own."""
    monkeypatch.delenv("SLURM_MEM_PER_NODE", raising=False)
    monkeypatch.setattr("qiita_compute_orchestrator.miint.detected_ram_gb", lambda: 64)


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
            f"SELECT feature_idx, {reassemble_chunks_expr()}"
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


def test_chunks_batched_into_disjoint_feature_idx_ranges(staging_inputs, tmp_path, monkeypatch):
    """When features exceed the per-batch chunk budget the shared re-key writer
    (`_feature_load.write_feature_sequence_chunks`, which reference_load delegates
    to) emits multiple parts, each a DISJOINT, CONTIGUOUS feature_idx range — that
    per-part min/max is what drives DuckLake FILE-level pruning. Bin-packed
    ascending by feature_idx, the writer carries every feature's chunk_data through
    exactly once (no chunk lost or duplicated across the parts)."""
    import qiita_compute_orchestrator.jobs._feature_load as fl

    # 5 single-chunk features, budget of 2 chunks -> 3 parts (2 + 2 + 1).
    monkeypatch.setattr(fl, "CHUNK_BUDGET_PER_BATCH", 2)
    outputs = _run(_inputs(**staging_inputs), tmp_path / "ws")
    chunks_dir = outputs["staging_dir"] / "reference_sequence_chunks"
    parts = sorted(chunks_dir.glob("part_*.parquet"))
    assert len(parts) == 3, f"expected 3 parts at budget=2, got {len(parts)}"

    with duckdb.connect(":memory:") as conn:
        # Every part's feature_idx range is disjoint from the others.
        ranges = sorted(
            conn.execute(f"SELECT min(feature_idx), max(feature_idx) FROM '{part}'").fetchone()
            for part in parts
        )
        for (_lo1, hi1), (lo2, _hi2) in zip(ranges, ranges[1:]):
            assert hi1 < lo2, f"part feature_idx ranges overlap: {ranges}"

        # No chunk_data lost or duplicated: every feature reassembles to its
        # canonical form across the parts.
        parts_glob = str(chunks_dir / "part_*.parquet")
        rows = conn.execute(
            f"SELECT feature_idx, {reassemble_chunks_expr()}"
            " FROM read_parquet(?) GROUP BY feature_idx",
            [parts_glob],
        ).fetchall()
    fidx_to_canon = {_FEATURE_MAP[_HASHES[n]]: _CANON[n] for n in _TEST_SEQUENCES}
    assert {fidx: canon for fidx, canon in rows} == fidx_to_canon


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


def _load_warnings(caplog) -> list[str]:
    """Lower-cased WARNING messages emitted by the reference_load logger."""
    return [
        r.getMessage().lower()
        for r in caplog.records
        if r.name == _REFERENCE_LOAD_LOGGER and r.levelno == logging.WARNING
    ]


@pytest.mark.parametrize("seq5_rows", [[], [("seq5", None)]], ids=["no row", "a NULL taxonomy"])
def test_unclassified_feature_recorded_as_null_rank_row(
    staging_inputs, tmp_path, caplog, seq5_rows
):
    """A feature with no supplied taxonomy, whether it has no row or only a NULL one, is
    recorded at rest as a NULL-rank row (reference_taxonomy stays 1-1 with features),
    and the coverage gap is warned — not silently dropped (the old INNER JOIN)."""
    tax = _write_taxonomy_parquet(
        tmp_path / "taxonomy.parquet",
        [(name, _TAXONOMY_STRINGS[name]) for name in ("seq1", "seq2", "seq3", "seq4")] + seq5_rows,
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
    assert any(
        w.startswith("1 feature(s) have no supplied taxonomy") and "seq5" in w
        for w in _load_warnings(caplog)
    )


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
    warnings = _load_warnings(caplog)
    assert any(
        w.startswith("1 supplied taxonomy row(s)") and "nz_cp039371.1" in w for w in warnings
    )


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
    assert any(
        w.startswith("1 feature_id(s) have duplicate") and "seq1" in w
        for w in _load_warnings(caplog)
    )


def test_conflicting_lineages_on_one_feature_keep_the_smallest_read_id(
    staging_inputs, tmp_path, caplog
):
    """Two read_ids of one feature with different lineages: the feature takes the
    lexicographically smallest read_id's — not whichever row came first — and warns."""
    inputs = _with_twins(staging_inputs, tmp_path, {"aaa_twin_of_seq1": "seq1"})
    tax = _write_taxonomy_parquet(
        tmp_path / "taxonomy.parquet",
        [(name, _TAXONOMY_STRINGS[name]) for name in _TEST_SEQUENCES]
        + [("aaa_twin_of_seq1", _TAXONOMY_STRINGS["seq2"])],
    )
    with caplog.at_level(logging.WARNING, logger=_REFERENCE_LOAD_LOGGER):
        outputs = _run(_inputs(**inputs, taxonomy_path=tax), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_taxonomy.parquet"
    seq1_fidx = _FEATURE_MAP[_HASHES["seq1"]]
    with duckdb.connect(":memory:") as conn:
        assert conn.execute(f"SELECT count(*) FROM '{pq}'").fetchone()[0] == len(_FEATURE_MAP)
        genus = conn.execute(
            f"SELECT genus FROM '{pq}' WHERE feature_idx = {seq1_fidx}"
        ).fetchone()[0]
    assert genus == "Escherichia"
    [warning] = [w for w in _load_warnings(caplog) if "more than one distinct lineage" in w]
    assert "['aaa_twin_of_seq1', 'seq1']" in warning


def test_agreeing_lineages_on_one_feature_do_not_warn(staging_inputs, tmp_path, caplog):
    inputs = _with_twins(staging_inputs, tmp_path, {"aaa_twin_of_seq1": "seq1"})
    tax = _write_taxonomy_parquet(
        tmp_path / "taxonomy.parquet",
        [(name, _TAXONOMY_STRINGS[name]) for name in _TEST_SEQUENCES]
        + [("aaa_twin_of_seq1", _TAXONOMY_STRINGS["seq1"])],
    )
    with caplog.at_level(logging.WARNING, logger=_REFERENCE_LOAD_LOGGER):
        _run(_inputs(**inputs, taxonomy_path=tax), tmp_path / "ws")
    assert _load_warnings(caplog) == []


def test_a_null_lineage_on_the_smallest_read_id_does_not_displace_a_lineage(
    staging_inputs, tmp_path, caplog
):
    """A NULL taxonomy carries no lineage, so the feature takes the lineage that its
    other read_id carries, even though the read_id with the NULL taxonomy sorts first."""
    inputs = _with_twins(staging_inputs, tmp_path, {"aaa_twin_of_seq1": "seq1"})
    tax = _write_taxonomy_parquet(
        tmp_path / "taxonomy.parquet",
        [(name, _TAXONOMY_STRINGS[name]) for name in _TEST_SEQUENCES]
        + [("aaa_twin_of_seq1", None)],
    )
    with caplog.at_level(logging.WARNING, logger=_REFERENCE_LOAD_LOGGER):
        outputs = _run(_inputs(**inputs, taxonomy_path=tax), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_taxonomy.parquet"
    seq1_fidx = _FEATURE_MAP[_HASHES["seq1"]]
    with duckdb.connect(":memory:") as conn:
        genus = conn.execute(
            f"SELECT genus FROM '{pq}' WHERE feature_idx = {seq1_fidx}"
        ).fetchone()[0]
    assert genus == "Lactobacillus"
    assert _load_warnings(caplog) == []


def test_a_conflict_warning_names_only_read_ids_that_carry_a_lineage(
    staging_inputs, tmp_path, caplog
):
    """The read_id with the NULL taxonomy sorts first but carries no lineage, so the
    warning starts from seq1, whose lineage is the one kept."""
    inputs = _with_twins(
        staging_inputs, tmp_path, {"aaa_twin_of_seq1": "seq1", "zzz_twin_of_seq1": "seq1"}
    )
    tax = _write_taxonomy_parquet(
        tmp_path / "taxonomy.parquet",
        [(name, _TAXONOMY_STRINGS[name]) for name in _TEST_SEQUENCES]
        + [("aaa_twin_of_seq1", None), ("zzz_twin_of_seq1", _TAXONOMY_STRINGS["seq2"])],
    )
    with caplog.at_level(logging.WARNING, logger=_REFERENCE_LOAD_LOGGER):
        outputs = _run(_inputs(**inputs, taxonomy_path=tax), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_taxonomy.parquet"
    seq1_fidx = _FEATURE_MAP[_HASHES["seq1"]]
    with duckdb.connect(":memory:") as conn:
        genus = conn.execute(
            f"SELECT genus FROM '{pq}' WHERE feature_idx = {seq1_fidx}"
        ).fetchone()[0]
    assert genus == "Lactobacillus"
    [warning] = [w for w in _load_warnings(caplog) if "more than one distinct lineage" in w]
    assert "['seq1', 'zzz_twin_of_seq1']" in warning


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
    chunks back into a temp file via `resolve_blob_input`."""
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


def test_phylogeny_accepts_a_RAW_newick_on_the_local_path(staging_inputs, tmp_path):
    """The local ingest path DoPuts nothing — "no bytes cross the wire" — so
    `tree_path` arrives as the raw `.nwk` file itself, not a chunked-BLOB upload
    Parquet. `resolve_blob_input` sniffs which shape it got.

    Before it did, `reference_load` unconditionally `read_parquet()`'d the tree,
    so `local-reference-add` carrying a tree crashed with a Parquet error. The
    remote (wrapped) form is covered by the test above; this pins the other half,
    and the two together are what make the sniff meaningful."""
    raw_nwk = tmp_path / "tree.nwk"
    raw_nwk.write_text("((seq1:0.1,seq2:0.2):0.3,(seq3:0.4,(seq4:0.5,seq5:0.6):0.7):0.8);")

    outputs = _run(_inputs(**staging_inputs, tree_path=raw_nwk), tmp_path / "ws")
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


def test_phylogeny_mints_edge_id_when_the_tree_decorates_nothing(
    staging_inputs, tree_path, tmp_path
):
    """An undecorated Newick leaves `edge_id` NULL on every node, and an index built
    from such a tree numbers its own edges, which join back to nothing here. The
    loader supplies `node_index` instead — dense and unique, and carried through a
    placement build verbatim. See "Edge numbering" in
    `docs/architecture/reference-data.md`."""
    outputs = _run(_inputs(**staging_inputs, tree_path=tree_path), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_phylogeny.parquet"
    with duckdb.connect(":memory:") as conn:
        rows, nulls, differing = conn.execute(
            "SELECT count(*),"
            "       count(*) FILTER (WHERE edge_id IS NULL),"
            "       count(*) FILTER (WHERE edge_id IS DISTINCT FROM node_index)"
            f" FROM '{pq}'"
        ).fetchone()
    assert rows > 0
    assert nulls == 0, "an undecorated tree must not reach the lake with NULL edge_id"
    assert differing == 0, "the minted numbering is node_index"


def test_phylogeny_leaves_a_partially_decorated_tree_partial(staging_inputs, tmp_path):
    """A tree carrying jplace `{N}` decorations keeps exactly those numbers, and the
    nodes it left undecorated stay NULL. The mint is all-or-nothing per tree: filling
    those NULLs with `node_index` would put two numberings in one column, and nothing
    downstream could tell which one an edge came from."""
    raw_nwk = tmp_path / "decorated.nwk"
    # Every node but the root is decorated; the root carries no branch length and
    # so no `{N}`, which is what makes this tree partially decorated.
    raw_nwk.write_text(
        "((seq1:0.1{5},seq2:0.2{6}):0.3{7},"
        "(seq3:0.4{8},(seq4:0.5{9},seq5:0.6{10}):0.7{11}):0.8{12});"
    )

    outputs = _run(_inputs(**staging_inputs, tree_path=raw_nwk), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_phylogeny.parquet"
    with duckdb.connect(":memory:") as conn:
        decorated = [
            r[0]
            for r in conn.execute(
                f"SELECT edge_id FROM '{pq}' WHERE edge_id IS NOT NULL ORDER BY edge_id"
            ).fetchall()
        ]
        (still_null,) = conn.execute(
            f"SELECT count(*) FILTER (WHERE edge_id IS NULL) FROM '{pq}'"
        ).fetchone()
    assert decorated == [5, 6, 7, 8, 9, 10, 11, 12]
    assert still_null == 1, "the undecorated root must keep its NULL, not be minted"


# The tree every placement fixture is made against: the five test sequences, every
# node but the root decorated with a jplace `{N}` edge number, 0 to 7.
_JPLACE_TREE = (
    "((seq1:0.1{0},seq2:0.2{1}):0.3{2},(seq3:0.4{3},(seq4:0.5{4},seq5:0.6{5}):0.7{6}):0.8{7});"
)


@pytest.fixture
def jplace_path(tmp_path):
    """jplace input wrapped in the CLI's chunked DoPut shape.
    reference_load stitches chunks back to a temp `.jplace` file, which
    `load_jplace` then reads."""
    jplace_text = (
        b'{"version": 3, "tree": "' + _JPLACE_TREE.encode() + b'",'
        b' "placements": ['
        b'   {"p": [[5, -1.0, 0.9, 0.01, 0.02]], "nm": [["seq1", 1]]},'
        b'   {"p": [[7, -2.0, 0.8, 0.03, 0.04]], "nm": [["seq2", 1]]}'
        b" ],"
        b' "fields": ["edge_num", "likelihood", "like_weight_ratio",'
        b'            "distal_length", "pendant_length"],'
        b' "metadata": {}}'
    )
    return _wrap_chunked_blob_parquet(tmp_path / "jplace_upload.parquet", jplace_text)


def _raw_jplace(path: Path, placements: list[tuple[str, int]], *, tree: str = _JPLACE_TREE) -> Path:
    """A raw jplace (the local-path shape) on `tree`, placing each
    `(fragment, edge_num)`, with likelihoods that differ per row so a test can tell
    which row landed."""
    doc = {
        "tree": tree,
        "placements": [
            {"p": [[edge, -1.0 - i, 0.5, 0.01, 0.02]], "n": [name]}
            for i, (name, edge) in enumerate(placements)
        ],
        "fields": [
            "edge_num",
            "likelihood",
            "like_weight_ratio",
            "distal_length",
            "pendant_length",
        ],
        "version": 3,
    }
    path.write_text(json.dumps(doc))
    return path


def _with_twins(staging_inputs: dict, tmp_path: Path, twins: dict[str, str]) -> dict:
    """`staging_inputs` with more manifest read_ids, each `twin` in `twins` sharing the
    sequence of the read_id it maps to: several names for one feature, as a
    reverse-complement pair or a repeated record would be."""
    manifest = tmp_path / "manifest_with_twins.parquet"
    _write_parquet(
        manifest,
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [(name, str(_HASHES[name]), len(_TEST_SEQUENCES[name])) for name in _TEST_SEQUENCES]
        + [(twin, str(_HASHES[of]), len(_TEST_SEQUENCES[of])) for twin, of in twins.items()],
    )
    return {**staging_inputs, "manifest": manifest}


def _placements(pq: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT feature_idx, edge_num, likelihood FROM '{pq}' ORDER BY feature_idx"
        ).fetchall()


def test_placements_lifted_writer_maps_fragment_to_feature_idx(
    staging_inputs, jplace_path, tmp_path
):
    """Placements writer maps `fragment` → manifest.read_id → feature_idx."""
    outputs = _run(_inputs(**staging_inputs, jplace_path=jplace_path), tmp_path / "ws")
    pq = outputs["staging_dir"] / "reference_placements.parquet"
    assert pq.exists()
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT feature_idx, edge_num FROM '{pq}' ORDER BY feature_idx"
        ).fetchall()
        expected = sorted([(_FEATURE_MAP[_HASHES["seq1"]], 5), (_FEATURE_MAP[_HASHES["seq2"]], 7)])
        assert rows == expected


@pytest.mark.parametrize("part_max_bytes", [None, 100])
def test_placements_load_as_written_however_the_file_is_split(
    staging_inputs, tmp_path, monkeypatch, part_max_bytes
):
    """A jplace too big for one `read_jplace` call is split and read part by part
    (duckdb-miint#400). Whether it fits one part or takes one part per placement (100
    bytes), every placement lands as the file wrote it."""
    from qiita_compute_orchestrator.jobs import _jplace

    if part_max_bytes is not None:
        monkeypatch.setattr(_jplace, "_PART_MAX_BYTES", part_max_bytes)
    written = [(name, i) for i, name in enumerate(_CANON)]
    jplace = _raw_jplace(tmp_path / "in.jplace", written)
    rows = _placements(
        _run(_inputs(**staging_inputs, jplace_path=jplace), tmp_path / "ws")["staging_dir"]
        / "reference_placements.parquet"
    )
    assert rows == sorted(
        (_FEATURE_MAP[_HASHES[name]], edge, -1.0 - edge) for name, edge in written
    )


def test_placement_of_an_unknown_fragment_raises(staging_inputs, tmp_path):
    """A fragment that is no sequence's read_id cannot be keyed to a feature; dropping
    it would publish a placement set other than the one supplied."""
    jplace = _raw_jplace(tmp_path / "in.jplace", [("seq1", 0), ("not_a_read", 1)])
    with pytest.raises(
        ValueError, match=r"1 jplace fragment\(s\) match no sequence identifier.*not_a_read"
    ):
        _run(_inputs(**staging_inputs, jplace_path=jplace), tmp_path / "ws")
    assert not (tmp_path / "ws" / "reference_placements.parquet").exists()


def test_fragment_placed_twice_raises(staging_inputs, tmp_path):
    jplace = _raw_jplace(tmp_path / "in.jplace", [("seq1", 0), ("seq2", 1), ("seq1", 2)])
    with pytest.raises(ValueError, match=r"placed more than once.*seq1"):
        _run(_inputs(**staging_inputs, jplace_path=jplace), tmp_path / "ws")


def test_placement_on_an_edge_the_jplace_tree_lacks_raises(staging_inputs, tmp_path):
    """Every edge_num must be an edge of the jplace's own tree, whether or not this
    load carries a tree. `_JPLACE_TREE` numbers 0 to 7."""
    jplace = _raw_jplace(tmp_path / "in.jplace", [("seq1", 3), ("seq2", 999), ("seq3", 999)])
    with pytest.raises(
        ValueError, match=r"1 jplace edge_num\(s\) are not an edge of.*999 \(2 placements\)"
    ):
        _run(_inputs(**staging_inputs, jplace_path=jplace), tmp_path / "ws")


def test_a_jplace_tree_numbered_with_brackets_is_refused(staging_inputs, tmp_path):
    """`read_newick` takes `[N]` as a comment (duckdb-miint#419), so the load says the
    edges are unnumbered rather than that every edge_num is missing from the tree."""
    tree = _JPLACE_TREE.replace("{", "[").replace("}", "]")
    jplace = _raw_jplace(tmp_path / "in.jplace", [("seq1", 0)], tree=tree)
    with pytest.raises(ValueError, match=r"8 of the 8 edges in the jplace's tree carry no \{N\}"):
        _run(_inputs(**staging_inputs, jplace_path=jplace), tmp_path / "ws")


@pytest.mark.parametrize(
    "tree",
    [
        _JPLACE_TREE,
        # The same tree with labeled internal nodes and every node's children reversed:
        # neither changes what a placement on it means.
        "(((seq5:0.6{5},seq4:0.5{4})x:0.7{6},seq3:0.4{3})y:0.8{7},(seq2:0.2{1},seq1:0.1{0})z:0.3{2});",
    ],
)
def test_placements_load_with_the_jplaces_own_tree(staging_inputs, tmp_path, tree):
    nwk = tmp_path / "tree.nwk"
    nwk.write_text(tree)
    jplace = _raw_jplace(tmp_path / "in.jplace", [("seq1", 0), ("seq2", 7)])
    outputs = _run(_inputs(**staging_inputs, tree_path=nwk, jplace_path=jplace), tmp_path / "ws")
    assert [r[1] for r in _placements(outputs["staging_dir"] / "reference_placements.parquet")] == [
        0,
        7,
    ]


@pytest.mark.parametrize(
    ("tree", "why"),
    [
        # The same topology without its `{N}` decorations: the loader numbers it by
        # node_index, which is not the jplace's numbering.
        (
            "((seq1:0.1,seq2:0.2):0.3,(seq3:0.4,(seq4:0.5,seq5:0.6):0.7):0.8);",
            "undecorated",
        ),
        (
            _JPLACE_TREE.replace("seq1:", "tmp:")
            .replace("seq2:", "seq1:")
            .replace("tmp:", "seq2:"),
            "two tips swapped",
        ),
        (
            "((seq1:0.1{0},seq2:0.2{1}):0.3{2},(seq3:0.4{3},seq4:0.5{4}):0.7{6},seq5:0.6{5});",
            "another topology",
        ),
        (_JPLACE_TREE.replace("seq1:0.1{0}", "seq1:0.15{0}"), "a branch length changed"),
        # Every node of the jplace's tree, unchanged, plus one: only the node count
        # tells the two apart.
        (_JPLACE_TREE[: -len(");")] + ",extra:0.9{8});", "one extra tip"),
    ],
)
def test_a_tree_other_than_the_jplaces_is_refused(staging_inputs, tmp_path, tree, why):
    """A load carrying a tree and a jplace must carry the jplace's own tree; each case
    differs from it in one way a placement depends on."""
    nwk = tmp_path / "tree.nwk"
    nwk.write_text(tree)
    jplace = _raw_jplace(tmp_path / "in.jplace", [("seq1", 0), ("seq2", 1)])
    with pytest.raises(ValueError, match="not the jplace's tree"):
        _run(_inputs(**staging_inputs, tree_path=nwk, jplace_path=jplace), tmp_path / "ws")


def test_a_feature_placed_under_two_fragments_keeps_the_smallest(staging_inputs, tmp_path, caplog):
    """Two read_ids of one feature, each placed: the feature takes the lexicographically
    smallest fragment's placement — not whichever came first in the file — and warns."""
    inputs = _with_twins(staging_inputs, tmp_path, {"aaa_twin_of_seq1": "seq1"})
    jplace = _raw_jplace(
        tmp_path / "in.jplace", [("seq1", 0), ("seq2", 1), ("aaa_twin_of_seq1", 2)]
    )
    with caplog.at_level(logging.WARNING, logger=_REFERENCE_LOAD_LOGGER):
        outputs = _run(_inputs(**inputs, jplace_path=jplace), tmp_path / "ws")
    rows = _placements(outputs["staging_dir"] / "reference_placements.parquet")
    seq1_fidx = _FEATURE_MAP[_HASHES["seq1"]]
    assert [r for r in rows if r[0] == seq1_fidx] == [(seq1_fidx, 2, -3.0)]
    assert len(rows) == 2
    [warning] = [w for w in _load_warnings(caplog) if "more than one fragment" in w]
    assert "aaa_twin_of_seq1" in warning


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


# =============================================================================
# Annotations
# =============================================================================


def _annotation_fixtures(tmp_path, *, map_rows=None):
    """hash_sequences' annotation manifest + mint-annotation-features' annotation map.

    One interval: bases [3, 8) of seq3, sitting on seq3 as its parent. The map's
    `parent_feature_idx` is seq3's, and every identifier on the eventual lake row comes
    from the map — this job mints nothing.
    """
    parent = "seq3"
    interval_hash = _HASHES["seq1"]  # stand-in for the extracted interval's own hash

    manifest = tmp_path / "annotation_manifest.parquet"
    _write_parquet(
        manifest,
        "sequence_hash UUID, parent_sequence_hash UUID, parent_read_id VARCHAR, "
        "annotation_id VARCHAR, source VARCHAR, annotation_type VARCHAR, "
        "position BIGINT, stop_position BIGINT, strand VARCHAR, "
        "score DOUBLE, phase SMALLINT, attributes MAP(VARCHAR, VARCHAR)",
        [
            (
                str(interval_hash),
                str(_HASHES[parent]),
                parent,
                "insert_01",
                "syndna",
                "insert",
                3,
                8,
                "+",
                None,
                None,
                {"mass_ng": "0.5"},
            )
        ],
    )

    amap = tmp_path / "annotation_map.parquet"
    _write_parquet(
        amap,
        "annotation_idx BIGINT, feature_idx BIGINT, parent_feature_idx BIGINT, "
        "position BIGINT, stop_position BIGINT, annotation_type VARCHAR, strand VARCHAR",
        map_rows
        if map_rows is not None
        else [
            (9001, _FEATURE_MAP[interval_hash], _FEATURE_MAP[_HASHES[parent]], 3, 8, "insert", "+")
        ],
    )
    return {"annotation_manifest": manifest, "annotation_map": amap}


def test_annotation_row_carries_the_minted_annotation_idx(staging_inputs, tmp_path):
    """The lake row's identity is the annotation_idx the CONTROL PLANE minted, joined in
    off the annotation map on the natural key. Nothing here re-derives it, and the GFF3
    `ID` is carried as provenance only — it is not unique, so nothing may join on it."""
    outputs = _run(_inputs(**staging_inputs, **_annotation_fixtures(tmp_path)), tmp_path / "ws")
    out = outputs["staging_dir"] / "reference_annotation.parquet"
    with duckdb.connect(":memory:") as conn:
        cols = [
            c[0] for c in conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{out}')").fetchall()
        ]
        rows = conn.execute(f"SELECT * FROM read_parquet('{out}')").fetchall()

    (row,) = [dict(zip(cols, r, strict=True)) for r in rows]
    assert row["annotation_idx"] == 9001
    assert row["reference_idx"] == _REFERENCE_IDX
    assert row["parent_feature_idx"] == _FEATURE_MAP[_HASHES["seq3"]]
    assert row["annotation_id"] == "insert_01"
    assert (row["position"], row["stop_position"]) == (3, 8)
    assert row["attributes"] == {"mass_ng": "0.5"}


def test_annotation_row_with_no_minted_idx_raises(staging_inputs, tmp_path):
    """If the map and the manifest disagree about the natural key, the inner join would
    simply DROP the row and register a lake table quietly missing an annotation. That is
    the failure this raise exists to convert into a dead ticket — here the map claims a
    window the manifest never declared."""
    fixtures = _annotation_fixtures(
        tmp_path,
        map_rows=[
            (9001, 1, _FEATURE_MAP[_HASHES["seq3"]], 4, 9, "insert", "+")
        ],  # window off by one
    )
    with pytest.raises(ValueError, match="no minted annotation_idx"):
        _run(_inputs(**staging_inputs, **fixtures), tmp_path / "ws")


def test_annotation_manifest_without_map_is_refused(staging_inputs, tmp_path):
    """Supplying one without the other is a mis-wired workflow whose only symptom would
    be a silently EMPTY reference_annotation table."""
    fixtures = _annotation_fixtures(tmp_path)
    with pytest.raises(ValueError, match="must be supplied together"):
        _run(
            _inputs(**staging_inputs, annotation_manifest=fixtures["annotation_manifest"]),
            tmp_path / "ws",
        )
