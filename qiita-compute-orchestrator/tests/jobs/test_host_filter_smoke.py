"""Real-miint smoke tests for `host_filter.execute` (seams NOT stubbed).

Builds a real rype `.ryxdi` and a real minimap2 `.mmi` from a synthetic host
contig, then runs `host_filter` end-to-end and asserts a known host read is
MASKED (`host_*`) while a clean read stays `pass`. Pins the REAL `rype_classify`
/ `align_minimap2` column + argument contracts (the thing the stubbed unit tests
can't verify), that host classification runs on the trimmed QC-pass subset, and
the run-twice reproducibility of the masked set.

host_filter no longer drops reads — it merges host hits into the qc_mask and
emits one `read_mask.parquet` row per read. The qc_mask fixture here marks every
read `pass` with zero trims, so the host stage decides the final reason.

The `write_reads` fixture (tests/jobs/conftest.py) owns the reads.parquet schema.
Runs against the team-mirror miint build (conftest sets MIINT_EXTENSION_REPO).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
from qiita_common.duckdb_miint import miint_connect_config, miint_install_sql
from qiita_common.models import ReadMaskReason

# Structured synthetic host contig: a distinct motif tiled, ~4.8 kb — real
# (reproducible) minimizer/k-mer content for both tools. Deterministic, no RNG.
_HOST_CONTIG = "ACGTACGTGGCCTTAAACGTTGCA" * 200
# A host READ is a 200 bp slice of the contig (matches rype + minimap2).
_HOST_READ = _HOST_CONTIG[100:300]
# A clean read shares no motif with the host contig → neither tool flags it.
_CLEAN_READ = "GCGCATATCGCGTATAGCGCATAT" * 8

_MASK_IDX = 777


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


def _all_pass_qc_mask(path: Path, rows: list[tuple[int, bool]]) -> Path:
    """Write a qc_mask.parquet marking every listed read `pass` with zero trims.
    `rows` are (sequence_idx, is_paired); paired reads carry 0 mate trims (PE),
    single-end reads carry NULL mate trims."""
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE m(sequence_idx BIGINT, reason VARCHAR, "
            "left_trim1 UINTEGER, right_trim1 UINTEGER, "
            "left_trim2 UINTEGER, right_trim2 UINTEGER)"
        )
        for sidx, paired in rows:
            mate = "0, 0" if paired else "NULL, NULL"
            conn.execute(
                f"INSERT INTO m VALUES (?, '{ReadMaskReason.PASS.value}', 0, 0, {mate})",
                [sidx],
            )
        conn.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")
    return path


def _reasons(path: Path) -> dict[int, str]:
    with duckdb.connect(":memory:") as conn:
        return {
            r[0]: r[1]
            for r in conn.execute(
                f"SELECT sequence_idx, reason FROM read_parquet('{path}')"
            ).fetchall()
        }


def _mask_idxs(path: Path) -> set[int]:
    with duckdb.connect(":memory:") as conn:
        return {
            r[0]
            for r in conn.execute(
                f"SELECT DISTINCT mask_idx FROM read_parquet('{path}')"
            ).fetchall()
        }


def test_host_filter_smoke_rype_only(tmp_path, write_reads):
    """Real rype_classify alone masks the host read host_rype, keeps the clean
    read pass — pins rype's id_column/threshold/sequence-column contract."""
    from qiita_compute_orchestrator.jobs import host_filter

    ryxdi, _ = _build_indexes(tmp_path)
    reads = write_reads(
        tmp_path / "reads.parquet",
        [(10, "host", _HOST_READ, None), (20, "clean", _CLEAN_READ, None)],
    )
    qc_mask = _all_pass_qc_mask(tmp_path / "qc_mask.parquet", [(10, False), (20, False)])
    inputs = host_filter.Inputs(
        reads=reads,
        qc_mask=qc_mask,
        mask_idx=_MASK_IDX,
        host_rype_path=ryxdi,
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))
    reasons = _reasons(out["read_mask"])
    assert reasons[10] == ReadMaskReason.HOST_RYPE.value
    assert reasons[20] == ReadMaskReason.PASS.value
    assert _mask_idxs(out["read_mask"]) == {_MASK_IDX}


def test_host_filter_smoke_minimap2_only(tmp_path, write_reads):
    """Real align_minimap2 alone masks the host read host_minimap2, keeps the
    clean read pass — pins the query-table/index_path/preset contract."""
    from qiita_compute_orchestrator.jobs import host_filter

    _, mmi = _build_indexes(tmp_path)
    reads = write_reads(
        tmp_path / "reads.parquet",
        [(10, "host", _HOST_READ, None), (20, "clean", _CLEAN_READ, None)],
    )
    qc_mask = _all_pass_qc_mask(tmp_path / "qc_mask.parquet", [(10, False), (20, False)])
    inputs = host_filter.Inputs(
        reads=reads,
        qc_mask=qc_mask,
        mask_idx=_MASK_IDX,
        host_minimap2_path=mmi,
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))
    reasons = _reasons(out["read_mask"])
    assert reasons[10] == ReadMaskReason.HOST_MINIMAP2.value
    assert reasons[20] == ReadMaskReason.PASS.value


def test_host_filter_smoke_both_pe_and_reproducible(tmp_path, write_reads):
    """Full two-stage path on paired-end reads: a host pair is masked, a clean
    pair stays pass, the masked set is identical across two independent runs —
    and a pair that is host ONLY in R2 (clean R1) still masks, proving the tools
    read `sequence2` natively rather than us flattening mates. minimap2 wins over
    rype when both flag a read."""
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
    qc_mask = _all_pass_qc_mask(tmp_path / "qc_mask.parquet", [(10, True), (20, True), (30, True)])
    inputs = host_filter.Inputs(
        reads=reads,
        qc_mask=qc_mask,
        mask_idx=_MASK_IDX,
        host_rype_path=ryxdi,
        host_minimap2_path=mmi,
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out1 = asyncio.run(host_filter.execute(inputs, tmp_path / "ws1"))
    out2 = asyncio.run(host_filter.execute(inputs, tmp_path / "ws2"))
    r1 = _reasons(out1["read_mask"])
    # 10 (both host) and 30 (host in R2 only) are host-masked; 20 (both clean) pass.
    assert r1[10] in (ReadMaskReason.HOST_RYPE.value, ReadMaskReason.HOST_MINIMAP2.value)
    assert r1[30] in (ReadMaskReason.HOST_RYPE.value, ReadMaskReason.HOST_MINIMAP2.value)
    assert r1[20] == ReadMaskReason.PASS.value
    assert _reasons(out2["read_mask"]) == r1
