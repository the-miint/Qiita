"""Real-miint smoke tests for `host_filter.execute` (seams NOT stubbed).

Builds a real rype `.ryxdi` and a real minimap2 `.mmi` from a synthetic host
contig, then runs `host_filter` end-to-end and asserts a known host read is
dropped while a clean read is kept. Pins the REAL `rype_classify` /
`align_minimap2` column + argument contracts (the thing the stubbed unit tests
can't verify) and the run-twice reproducibility of the surviving set.

The `write_reads` / `read_survivors` fixtures (tests/jobs/conftest.py) own the
reads.parquet schema. Runs against the team-mirror miint build (conftest sets
MIINT_EXTENSION_REPO).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
from qiita_common.duckdb_miint import miint_connect_config, miint_install_sql

# Structured synthetic host contig: a distinct motif tiled, ~4.8 kb — real
# (reproducible) minimizer/k-mer content for both tools. Deterministic, no RNG.
_HOST_CONTIG = "ACGTACGTGGCCTTAAACGTTGCA" * 200
# A host READ is a 200 bp slice of the contig (matches rype + minimap2).
_HOST_READ = _HOST_CONTIG[100:300]
# A clean read shares no motif with the host contig → neither tool flags it.
_CLEAN_READ = "GCGCATATCGCGTATAGCGCATAT" * 8


def _build_indexes(tmp_path: Path) -> tuple[Path, Path]:
    """Build a real `.ryxdi` and `.mmi` from `_HOST_CONTIG` via miint directly."""
    conn = duckdb.connect(":memory:", config=miint_connect_config())
    conn.execute(miint_install_sql())
    conn.execute("LOAD miint;")
    conn.execute(
        "CREATE TABLE hc AS SELECT CAST(1 AS BIGINT) feature_idx, "
        "CAST(0 AS INTEGER) chunk_index, ? chunk_data",
        [_HOST_CONTIG],
    )
    ryxdi = tmp_path / "host.ryxdi"
    status = conn.execute(
        "SELECT status FROM rype_index_create(?, ?, k := 64, w := 25, orient := TRUE)",
        ["hc", str(ryxdi)],
    ).fetchone()[0]
    assert status == "ok", f"rype index build failed: {status!r}"

    conn.execute("CREATE TABLE hs AS SELECT 'host1' read_id, ? sequence1", [_HOST_CONTIG])
    mmi = tmp_path / "host.mmi"
    ok = conn.execute(
        "SELECT success FROM save_minimap2_index(?, ?, preset := 'sr')", ["hs", str(mmi)]
    ).fetchone()[0]
    assert ok, "minimap2 index build failed"
    conn.close()
    return ryxdi, mmi


def test_host_filter_smoke_rype_only(tmp_path, write_reads, read_survivors):
    """Real rype_classify alone drops the host read, keeps the clean read —
    pins rype's id_column/threshold/sequence-column contract."""
    from qiita_compute_orchestrator.jobs import host_filter

    ryxdi, _ = _build_indexes(tmp_path)
    reads = write_reads(
        tmp_path / "reads.parquet",
        [(10, "host", _HOST_READ, None), (20, "clean", _CLEAN_READ, None)],
    )
    inputs = host_filter.Inputs(
        reads=reads, host_rype_path=ryxdi, prep_sample_idx=5, work_ticket_idx=1
    )
    out = asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))
    assert read_survivors(out["filtered_reads"]) == [20]


def test_host_filter_smoke_minimap2_only(tmp_path, write_reads, read_survivors):
    """Real align_minimap2 alone drops the host read, keeps the clean read —
    pins the query-table/index_path/preset contract and BIGINT read_id."""
    from qiita_compute_orchestrator.jobs import host_filter

    _, mmi = _build_indexes(tmp_path)
    reads = write_reads(
        tmp_path / "reads.parquet",
        [(10, "host", _HOST_READ, None), (20, "clean", _CLEAN_READ, None)],
    )
    inputs = host_filter.Inputs(
        reads=reads, host_minimap2_path=mmi, prep_sample_idx=5, work_ticket_idx=1
    )
    out = asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))
    assert read_survivors(out["filtered_reads"]) == [20]


def test_host_filter_smoke_both_pe_and_reproducible(tmp_path, write_reads, read_survivors):
    """Full two-stage path on paired-end reads: a host pair drops, a clean pair
    survives, the surviving set is identical across two independent runs — and a
    pair that is host ONLY in R2 (clean R1) still drops, proving the tools read
    `sequence2` natively rather than us flattening mates."""
    from qiita_compute_orchestrator.jobs import host_filter

    ryxdi, mmi = _build_indexes(tmp_path)
    reads = write_reads(
        tmp_path / "reads.parquet",
        [
            (10, "host_pair", _HOST_READ, _HOST_READ),
            (20, "clean_pair", _CLEAN_READ, _CLEAN_READ),
            (30, "r2_host_pair", _CLEAN_READ, _HOST_READ),  # host only in R2
        ],
    )
    inputs = host_filter.Inputs(
        reads=reads,
        host_rype_path=ryxdi,
        host_minimap2_path=mmi,
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out1 = asyncio.run(host_filter.execute(inputs, tmp_path / "ws1"))
    out2 = asyncio.run(host_filter.execute(inputs, tmp_path / "ws2"))
    # 10 (both host) and 30 (host in R2 only) drop; 20 (both clean) survives.
    assert read_survivors(out1["filtered_reads"]) == [20]
    assert read_survivors(out2["filtered_reads"]) == read_survivors(out1["filtered_reads"])
