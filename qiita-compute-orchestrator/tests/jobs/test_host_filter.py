"""Isolated unit tests for `host_filter.execute` (build seams stubbed).

`host_filter` is a pure `reads.parquet -> filtered_reads.parquet` transform keyed
by the already-minted `sequence_idx`. It runs the host filter in two stages —
rype `rype_classify` FIRST, then minimap2 `align_minimap2` on rype's survivors —
and drops any read whose `sequence_idx` is flagged by either tool. Paired-end
mates share one `sequence_idx`, so flagging either mate drops the whole pair.

The real rype/minimap2 calls need the miint extension + real indexes, so they're
exercised in `test_host_filter_smoke.py`; here the seams (`_run_rype_classify`,
`_run_align_minimap2`) are stubbed and we assert the orchestration:

  - drop set = rype-flagged ∪ minimap2-flagged;
  - two-stage ordering: minimap2 only ever sees rype's survivors;
  - PE pair-drop: either mate (either tool) flagged → whole pair dropped;
  - pass-through when neither index is bound;
  - empty (but valid) output when every read is dropped;
  - a stray NULL in a host accumulator never wipes the output (NULL-safe anti-join);
  - the 6-column reads schema is preserved;
  - fail-fast on missing reads / empty `.ryxdi` / zero-byte `.mmi`.

The `write_reads` / `read_survivors` fixtures (tests/jobs/conftest.py) own the
fastq_to_parquet 6-col schema: `(sequence_idx BIGINT, read_id VARCHAR, sequence1
VARCHAR, qual1 UTINYINT[], sequence2 VARCHAR, qual2 UTINYINT[])`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest

from qiita_compute_orchestrator.read_count import ReadCount

# Independent oracle for the preserved output schema (deliberately NOT shared
# with the writer fixture, so a drift in either is caught).
_READS_SCHEMA = ["sequence_idx", "read_id", "sequence1", "qual1", "sequence2", "qual2"]


def _schema(path: Path) -> list[str]:
    with duckdb.connect(":memory:") as conn:
        return [
            d[0] for d in conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0").description
        ]


def _ryxdi(tmp_path: Path, name: str = "host.ryxdi") -> Path:
    """A non-empty `.ryxdi` directory (rype index is a DIRECTORY)."""
    d = tmp_path / name
    d.mkdir()
    (d / "manifest.toml").write_text("k=64\n")
    return d


def _mmi(tmp_path: Path, name: str = "host.mmi") -> Path:
    """A non-empty `.mmi` file (minimap2 index is a FILE)."""
    p = tmp_path / name
    p.write_bytes(b"MMI-bytes")
    return p


def test_host_filter_drops_rype_union_minimap2(tmp_path, monkeypatch, write_reads, read_survivors):
    """drop set = rype ∪ minimap2; minimap2 sees ONLY rype's survivors; the
    6-column schema is preserved on the survivors."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(
        tmp_path / "reads.parquet",
        [
            (10, "rA", "S1A", "S2A"),  # rype-flagged
            (20, "rB", "S1B", "S2B"),  # clean → survives
            (30, "rC", "S1C", "S2C"),  # minimap2-flagged
            (40, "rD", "S1D", None),  # single-end, clean → survives
        ],
    )

    seen: dict = {}

    def fake_rype(conn, index_path, sequence_table, dest_table, *, threshold):
        seen["threshold"] = threshold
        seen["rype_query_ids"] = sorted(
            r[0] for r in conn.execute(f"SELECT read_id FROM {sequence_table}").fetchall()
        )
        conn.execute(f"INSERT INTO {dest_table} VALUES (10)")

    def fake_mm2(conn, index_path, query_table, dest_table, *, preset):
        seen["preset"] = preset
        seen["mm2_query_ids"] = sorted(
            r[0] for r in conn.execute(f"SELECT DISTINCT read_id FROM {query_table}").fetchall()
        )
        conn.execute(f"INSERT INTO {dest_table} VALUES (30)")

    monkeypatch.setattr(host_filter, "_run_rype_classify", fake_rype)
    monkeypatch.setattr(host_filter, "_run_align_minimap2", fake_mm2)

    inputs = host_filter.Inputs(
        reads=reads,
        host_rype_path=_ryxdi(tmp_path),
        host_minimap2_path=_mmi(tmp_path),
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))

    assert read_survivors(out["filtered_reads"]) == [20, 40]
    assert _schema(out["filtered_reads"]) == _READS_SCHEMA
    # Quality-filtered read count (#141) over the survivors: 2 rows (20 PE, 40
    # SE) → count(*)=2 + count(sequence2)=1 = 3 reads r1r2; layout 'paired'.
    rc = ReadCount.model_validate_json(out["quality_filtered_read_count"].read_text())
    assert (rc.read_pairs, rc.read_count_r1r2, rc.layout) == (2, 3, "paired")
    # rype saw one query row per pair (both mates ride sequence1/sequence2; the
    # tools handle PE natively — no unrolling).
    assert seen["rype_query_ids"] == [10, 20, 30, 40]
    assert seen["threshold"] == host_filter._RYPE_THRESHOLD
    # Two-stage: minimap2's query is rype's survivors — 10 excluded.
    assert 10 not in seen["mm2_query_ids"]
    assert seen["mm2_query_ids"] == [20, 30, 40]
    assert seen["preset"] == host_filter._MINIMAP2_PRESET


def test_host_filter_pe_pair_drop_either_mate(tmp_path, monkeypatch, write_reads, read_survivors):
    """A pair is dropped when EITHER mate is host (rype here): R1-host and
    R2-host pairs both go; the all-clean pair survives."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(
        tmp_path / "reads.parquet",
        [
            (60, "p60", "HOST", "CLEAN"),  # R1 host
            (61, "p61", "CLEAN", "HOST"),  # R2 host
            (62, "p62", "CLEAN", "CLEAN"),  # both clean → survives
        ],
    )

    def fake_rype(conn, index_path, sequence_table, dest_table, *, threshold):
        # rype reads BOTH mates natively, so flag the pair when EITHER mate
        # carries the host motif (sequence1=R1, sequence2=R2 in one query row).
        conn.execute(
            f"INSERT INTO {dest_table} "
            f"SELECT read_id FROM {sequence_table} "
            "WHERE sequence1 = 'HOST' OR sequence2 = 'HOST'"
        )

    monkeypatch.setattr(host_filter, "_run_rype_classify", fake_rype)

    inputs = host_filter.Inputs(
        reads=reads,
        host_rype_path=_ryxdi(tmp_path),
        host_minimap2_path=None,
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))
    assert read_survivors(out["filtered_reads"]) == [62]


def test_host_filter_minimap2_only(tmp_path, monkeypatch, write_reads, read_survivors):
    """minimap2 alone (rype unbound): the survivors relation is all mates (no
    rype prefilter), and the COPY drops the minimap2-flagged sequence_idx."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(tmp_path / "reads.parquet", [(10, "a", "S1", "S2"), (20, "b", "S1", "S2")])

    def boom(*a, **k):  # rype must NOT be invoked when its path is unbound
        raise AssertionError("rype seam called with no rype index bound")

    def fake_mm2(conn, index_path, query_table, dest_table, *, preset):
        # rype didn't run, so the survivors query is the full mate set.
        assert sorted(
            r[0] for r in conn.execute(f"SELECT DISTINCT read_id FROM {query_table}").fetchall()
        ) == [10, 20]
        conn.execute(f"INSERT INTO {dest_table} VALUES (10)")

    monkeypatch.setattr(host_filter, "_run_rype_classify", boom)
    monkeypatch.setattr(host_filter, "_run_align_minimap2", fake_mm2)

    inputs = host_filter.Inputs(
        reads=reads,
        host_rype_path=None,
        host_minimap2_path=_mmi(tmp_path),
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))
    assert read_survivors(out["filtered_reads"]) == [20]


def test_host_filter_passthrough_when_no_indexes(
    tmp_path, monkeypatch, write_reads, read_survivors
):
    """Neither index bound (host filtering disabled) → every read passes through,
    schema intact, neither seam invoked."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(tmp_path / "reads.parquet", [(10, "a", "S1", "S2"), (20, "b", "S1", None)])

    def boom(*a, **k):
        raise AssertionError("a filter seam was called in pass-through mode")

    monkeypatch.setattr(host_filter, "_run_rype_classify", boom)
    monkeypatch.setattr(host_filter, "_run_align_minimap2", boom)

    inputs = host_filter.Inputs(reads=reads, prep_sample_idx=5, work_ticket_idx=1)
    out = asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))
    assert read_survivors(out["filtered_reads"]) == [10, 20]
    assert _schema(out["filtered_reads"]) == _READS_SCHEMA


def test_host_filter_empty_output_when_all_dropped(
    tmp_path, monkeypatch, write_reads, read_survivors
):
    """A fully host-contaminated sample is valid: an empty (0-row) but
    well-formed filtered_reads.parquet, no special error."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(tmp_path / "reads.parquet", [(10, "a", "S1", "S2"), (20, "b", "S1", "S2")])

    def fake_rype(conn, index_path, sequence_table, dest_table, *, threshold):
        conn.execute(f"INSERT INTO {dest_table} SELECT DISTINCT read_id FROM {sequence_table}")

    monkeypatch.setattr(host_filter, "_run_rype_classify", fake_rype)

    inputs = host_filter.Inputs(
        reads=reads, host_rype_path=_ryxdi(tmp_path), prep_sample_idx=5, work_ticket_idx=1
    )
    out = asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))
    assert out["filtered_reads"].exists()
    assert read_survivors(out["filtered_reads"]) == []
    assert _schema(out["filtered_reads"]) == _READS_SCHEMA


def test_host_filter_null_in_accumulator_does_not_wipe(
    tmp_path, monkeypatch, write_reads, read_survivors
):
    """A stray NULL in a host accumulator must NOT collapse the whole output —
    the anti-join is NULL-safe (a `NOT IN` over a NULL-containing set would
    silently drop every read). The real (non-NULL) hit still drops; the rest
    survive."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(
        tmp_path / "reads.parquet",
        [(10, "a", "S1", "S2"), (20, "b", "S1", "S2"), (30, "c", "S1", None)],
    )

    def fake_rype(conn, index_path, sequence_table, dest_table, *, threshold):
        # A genuine host hit (10) plus a stray NULL sequence_idx.
        conn.execute(f"INSERT INTO {dest_table} VALUES (10), (NULL)")

    monkeypatch.setattr(host_filter, "_run_rype_classify", fake_rype)

    inputs = host_filter.Inputs(
        reads=reads, host_rype_path=_ryxdi(tmp_path), prep_sample_idx=5, work_ticket_idx=1
    )
    out = asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))
    # 10 dropped (real hit); 20, 30 survive — the NULL did not wipe everything.
    assert read_survivors(out["filtered_reads"]) == [20, 30]


def test_host_filter_missing_reads_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import host_filter

    inputs = host_filter.Inputs(
        reads=tmp_path / "nope.parquet", prep_sample_idx=5, work_ticket_idx=1
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))


def test_host_filter_empty_ryxdi_raises(tmp_path, write_reads):
    """An empty `.ryxdi` directory (no index content) is fail-fast, not a
    silent no-op classify."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(tmp_path / "reads.parquet", [(10, "a", "S1", "S2")])
    empty = tmp_path / "empty.ryxdi"
    empty.mkdir()

    inputs = host_filter.Inputs(
        reads=reads, host_rype_path=empty, prep_sample_idx=5, work_ticket_idx=1
    )
    with pytest.raises((ValueError, FileNotFoundError)):
        asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))


def test_host_filter_zero_byte_mmi_raises(tmp_path, write_reads):
    """A zero-byte `.mmi` is a broken index — fail fast."""
    from qiita_compute_orchestrator.jobs import host_filter

    reads = write_reads(tmp_path / "reads.parquet", [(10, "a", "S1", "S2")])
    mmi = tmp_path / "empty.mmi"
    mmi.write_bytes(b"")

    inputs = host_filter.Inputs(
        reads=reads, host_minimap2_path=mmi, prep_sample_idx=5, work_ticket_idx=1
    )
    with pytest.raises((ValueError, FileNotFoundError)):
        asyncio.run(host_filter.execute(inputs, tmp_path / "ws"))
