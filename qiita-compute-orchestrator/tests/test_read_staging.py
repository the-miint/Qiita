"""Unit tests for read_staging's per-slot DuckDB caps.

`per_slot_caps` splits a memory budget across `concurrency` in-flight
samples/runs. Under SLURM the budget is the cgroup; off SLURM the literal is a
ceiling bounded by detected host RAM, the same contract as
`resolve_duckdb_memory_gb` — these tests pin that bound (and its fail-soft).
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from qiita_compute_orchestrator import read_staging


def _caps(
    monkeypatch,
    ram: int | None,
    *,
    concurrency: int = 2,
    threads: int = 4,
    fallback_memory_gb: int = 7,
) -> tuple[int, int]:
    """per_slot_caps off SLURM with `ram` as the detected host RAM."""
    monkeypatch.delenv("SLURM_MEM_PER_NODE", raising=False)
    monkeypatch.setattr(read_staging, "detected_ram_gb", lambda: ram)
    return read_staging.per_slot_caps(
        concurrency, threads=threads, fallback_memory_gb=fallback_memory_gb
    )


def test_roomy_host_keeps_literal(monkeypatch):
    assert _caps(monkeypatch, 128) == (7, 4)


def test_small_host_gets_even_share_of_ram(monkeypatch):
    # 12 GB host, 2 slots x 4 threads: usable = 12 - headroom(8) = 6, so each
    # slot gets 6 // 2 = 3 instead of the 7 GB literal the host can't back.
    assert _caps(monkeypatch, 12) == (3, 4)


def test_tiny_host_floors_at_one(monkeypatch):
    # usable = 7 - headroom(8) = 1 → 1 // 2 = 0 → the same >= 1 floor resolve has.
    assert _caps(monkeypatch, 7) == (1, 4)


def test_detection_failure_keeps_literal(monkeypatch):
    assert _caps(monkeypatch, None) == (7, 4)


def test_under_slurm_detected_ram_is_irrelevant(monkeypatch):
    # The cgroup wins under SLURM: (48 - headroom(8)) // 2 = 21, and a 4 GB
    # detected-RAM reading must not shrink it.
    monkeypatch.setenv("SLURM_MEM_PER_NODE", str(48 * 1024))
    monkeypatch.setattr(read_staging, "detected_ram_gb", lambda: 4)
    assert read_staging.per_slot_caps(2, threads=4, fallback_memory_gb=7) == (21, 4)


# ---------------------------------------------------------------------------
# write_sorted_reads: the shared per-sample writer.
#   - default mode (sequence_index verbatim): ingest_reads / ingest_ena_reads,
#     whose intermediate is already per-sample.
#   - golay mode (ROW_NUMBER + per-sample filter): golay_demux, whose demux
#     intermediate is pooled and numbered across all samples.
# ---------------------------------------------------------------------------

_OUT_COLS = [
    "prep_sample_idx",
    "sequence_idx",
    "read_id",
    "sequence1",
    "qual1",
    "sequence2",
    "qual2",
]


def _write_source(path: Path, rows: list[tuple], columns: list[str]) -> None:
    """Write a tiny source parquet from python rows. The index columns are BIGINT
    (the writer does arithmetic on the local index); the rest VARCHAR."""
    col_defs = ", ".join(
        f"{c} BIGINT" if c in ("sequence_index", "prep_sample_idx") else f"{c} VARCHAR"
        for c in columns
    )
    with duckdb.connect() as conn:
        conn.execute(f"CREATE TABLE src ({col_defs})")
        placeholders = ", ".join("?" for _ in columns)
        conn.executemany(f"INSERT INTO src VALUES ({placeholders})", rows)
        conn.execute(f"COPY src TO '{path}' (FORMAT PARQUET)")


def _read_out(path: Path) -> list[dict]:
    with duckdb.connect() as conn:
        cur = conn.execute(
            f"SELECT {', '.join(_OUT_COLS)} FROM read_parquet('{path}') ORDER BY sequence_idx"
        )
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]


def test_write_sorted_reads_default_mode(tmp_path):
    """ingest shape: sequence_idx = sequence_index + start - 1, verbatim."""
    src = tmp_path / "intermediate.parquet"
    _write_source(
        src,
        [
            (1, "rA", "ACGT", "IIII", "TTTT", "JJJJ"),
            (2, "rB", "GGCC", "IIII", "AAAA", "JJJJ"),
            (3, "rC", "TTAA", "IIII", "CCCC", "JJJJ"),
        ],
        ["sequence_index", "read_id", "sequence1", "qual1", "sequence2", "qual2"],
    )
    out = tmp_path / "read.parquet"
    read_staging.write_sorted_reads(src, 42, 100, out, tmp_path, 1, 1)
    rows = _read_out(out)
    assert [r["sequence_idx"] for r in rows] == [100, 101, 102]
    assert all(r["prep_sample_idx"] == 42 for r in rows)
    assert [r["read_id"] for r in rows] == ["rA", "rB", "rC"]
    assert rows[0]["sequence2"] == "TTTT"  # R2 carries through
    assert not (tmp_path / "read.parquet.partial").exists()


def test_write_sorted_reads_golay_mode_renumbers_per_sample(tmp_path):
    """golay's pooled intermediate (numbered across ALL samples) is re-numbered
    densely 1..count per sample via ROW_NUMBER + a per-sample filter, regardless
    of the gappy global sequence_index."""
    src = tmp_path / "demuxed.parquet"
    _write_source(
        src,
        [
            (9, 1, "s9a", "AAAA", "IIII", None, None),
            (7, 2, "s7a", "CCCC", "IIII", None, None),
            (9, 3, "s9b", "GGGG", "IIII", None, None),
            (7, 4, "s7b", "TTTT", "IIII", None, None),
            (7, 5, "s7c", "ACAC", "IIII", None, None),
        ],
        [
            "prep_sample_idx",
            "sequence_index",
            "read_id",
            "sequence1",
            "qual1",
            "sequence2",
            "qual2",
        ],
    )
    out = tmp_path / "read.parquet"
    read_staging.write_sorted_reads(
        src,
        7,
        500,
        out,
        tmp_path,
        1,
        1,
        local_index_sql="ROW_NUMBER() OVER (ORDER BY sequence_index)",
        where_sql="prep_sample_idx = 7",
    )
    rows = _read_out(out)
    assert [r["read_id"] for r in rows] == ["s7a", "s7b", "s7c"]
    assert [r["sequence_idx"] for r in rows] == [500, 501, 502]
    assert all(r["prep_sample_idx"] == 7 for r in rows)


def test_hardlink_shares_inode(tmp_path):
    src = tmp_path / "durable.parquet"
    src.write_bytes(b"payload-bytes")
    dst = tmp_path / "register" / "1.parquet"
    dst.parent.mkdir()
    read_staging.hardlink(src, dst)
    assert dst.read_bytes() == b"payload-bytes"
    assert os.stat(src).st_ino == os.stat(dst).st_ino


def test_hardlink_replaces_existing_dst(tmp_path):
    src = tmp_path / "durable.parquet"
    src.write_bytes(b"new")
    dst = tmp_path / "1.parquet"
    dst.write_bytes(b"stale")
    read_staging.hardlink(src, dst)
    assert dst.read_bytes() == b"new"
    assert os.stat(src).st_ino == os.stat(dst).st_ino
