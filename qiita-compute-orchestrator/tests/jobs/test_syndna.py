"""Unit tests for `syndna.execute` (the minimap2 align seam stubbed).

`syndna` is the FIRST step of the read-mask chain: it ALIGNS the RAW reads against
a spike-in minimap2 index and emits a PARTIAL mask (the 6-column qc_mask shape)
marking hits `spikein_syndna`, everything else `pass`. The real `align_minimap2`
needs the miint extension and a built `.mmi`, so it is stubbed here and exercised
(identity threshold included) in test_syndna_smoke.py.

Asserted here:
  - hits become `spikein_syndna`, everything else `pass`;
  - the output is a partial mask (6 columns, all trims zero — SynDNA does not
    trim), under the `partial_mask` binding — NOT the final read_mask;
  - the mate-trim columns follow the read_mask convention (NULL single-end,
    0 paired) so both-mates counting stays correct downstream;
  - a missing or empty `.mmi` fails fast.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest
from qiita_common.models import ReadMaskReason

_PASS = ReadMaskReason.PASS.value
_SPIKEIN = ReadMaskReason.SPIKEIN_SYNDNA.value


def _write_reads(path: Path, rows: list[tuple[int, str, str | None]]) -> Path:
    """(sequence_idx, sequence1, sequence2|None); no quals needed for classify."""
    values = ", ".join(
        "(CAST(5 AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
        "CAST(NULL AS UTINYINT[]), CAST(? AS VARCHAR), CAST(NULL AS UTINYINT[]))"
        for _ in rows
    )
    params: list = []
    for sidx, s1, s2 in rows:
        params.extend([sidx, f"r{sidx}", s1, s2])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _rows(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT sequence_idx, reason FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


def _index(tmp_path: Path) -> Path:
    """A minimap2 index is a single non-empty `.mmi` FILE (not a rype directory)."""
    f = tmp_path / "syndna.mmi"
    f.write_bytes(b"\x00mmi")
    return f


def _run(tmp_path, reads, index):
    from qiita_compute_orchestrator.jobs import syndna

    return asyncio.run(
        syndna.execute(
            syndna.Inputs(reads=reads, syndna_minimap2_path=index, work_ticket_idx=1),
            tmp_path / "ws",
        )
    )


def _stub_hits(monkeypatch, hits: list[int]):
    """Stub align_minimap2: insert the flagged sequence_idx set directly.

    The real seam applies the identity floor itself (see `_MIN_IDENTITY`), so what
    reaches `dest_table` is already the >= threshold set — the stub therefore models
    the POST-threshold hits. The threshold arithmetic is pinned in the smoke test
    against a real `.mmi`."""
    from qiita_compute_orchestrator.jobs import syndna

    def fake(conn, index_path, query_table, dest_table, *, preset, min_identity):
        # Only reads visible in the query view may be flagged — mirrors the real
        # function, which aligns exactly that relation (here: every raw read).
        assert preset == syndna._MM2_PRESET
        assert min_identity == syndna._MIN_IDENTITY
        visible = {r[0] for r in conn.execute(f"SELECT read_id FROM {query_table}").fetchall()}
        for sidx in hits:
            assert sidx in visible, f"stub flagged {sidx}, not in the query view"
            conn.execute(f"INSERT INTO {dest_table} VALUES (?)", [sidx])

    monkeypatch.setattr(syndna, "_run_align_minimap2", fake)


def test_syndna_marks_hits_and_passes_the_rest(tmp_path, monkeypatch):
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", None), (2, "TTTT", None)])
    _stub_hits(monkeypatch, [1])
    out = _run(tmp_path, reads, _index(tmp_path))
    assert set(out) == {"partial_mask"}
    assert _rows(out["partial_mask"]) == [(1, _SPIKEIN), (2, _PASS)]


def test_syndna_emits_a_zero_trim_partial_mask(tmp_path, monkeypatch):
    """Six columns, all trims zero (SynDNA does not trim); single-end leaves the
    mate trims NULL. This is the shape qc / lima_mask consume."""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", None)])
    _stub_hits(monkeypatch, [1])
    out = _run(tmp_path, reads, _index(tmp_path))
    with duckdb.connect(":memory:") as conn:
        cols = [
            d[0]
            for d in conn.execute(
                f"SELECT * FROM read_parquet('{out['partial_mask']}') LIMIT 0"
            ).description
        ]
        row = conn.execute(f"SELECT * FROM read_parquet('{out['partial_mask']}')").fetchone()
    assert cols == [
        "sequence_idx",
        "reason",
        "left_trim1",
        "right_trim1",
        "left_trim2",
        "right_trim2",
    ]
    assert row == (1, _SPIKEIN, 0, 0, None, None)


def test_syndna_rejects_a_paired_end_read_set(tmp_path, monkeypatch):
    """syndna is the FIRST step, so it is where the read set first meets a
    long-read-only seam. A PE set must be rejected HERE — before a full minimap2 pass
    that lima_export/qc would only reject at the next step anyway. The gates are
    client-supplied, so this is reachable: nothing cross-validates `syndna_enabled`
    against the pool's platform."""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", "TTTT")])
    _stub_hits(monkeypatch, [])
    with pytest.raises(ValueError, match="paired-end"):
        _run(tmp_path, reads, _index(tmp_path))


def test_syndna_no_hits_marks_everything_pass(tmp_path, monkeypatch):
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", None), (2, "TTTT", None)])
    _stub_hits(monkeypatch, [])
    out = _run(tmp_path, reads, _index(tmp_path))
    assert _rows(out["partial_mask"]) == [(1, _PASS), (2, _PASS)]


def test_syndna_missing_index_raises(tmp_path, monkeypatch):
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", None)])
    _stub_hits(monkeypatch, [])
    with pytest.raises(FileNotFoundError, match="syndna_minimap2_path"):
        _run(tmp_path, reads, tmp_path / "nope.mmi")


def test_syndna_empty_index_raises(tmp_path, monkeypatch):
    """A zero-byte .mmi would align nothing and silently report zero spike-ins for a
    sample that has them — which the cell-count model would then divide by."""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGT", None)])
    _stub_hits(monkeypatch, [])
    empty = tmp_path / "empty.mmi"
    empty.write_bytes(b"")
    with pytest.raises(ValueError, match="non-empty .mmi"):
        _run(tmp_path, reads, empty)


def test_syndna_emits_exactly_one_row_per_read(tmp_path, monkeypatch):
    """THE BIJECTION, pinned at the producer. `qc` / `lima_export` / `lima_mask` all
    JOIN their incoming mask against the reads, so a missing row silently DROPS a read
    (under-reporting the sample's `raw` total) and a duplicate fans the join out and
    double-counts. syndna guarantees one row per read by construction — `reads LEFT
    JOIN hits` over a DISTINCT hit set — and this is what holds that guarantee, so the
    consumers do not each re-check it at runtime (see jobs/_partial_mask)."""
    rows = [(i, "ACGT" if i % 2 else "TTTT", None) for i in range(1, 8)]
    reads = _write_reads(tmp_path / "reads.parquet", rows)
    _stub_hits(monkeypatch, [2, 4])
    out = _run(tmp_path, reads, _index(tmp_path))
    with duckdb.connect(":memory:") as conn:
        n, distinct = conn.execute(
            f"SELECT count(*), count(DISTINCT sequence_idx) "
            f"FROM read_parquet('{out['partial_mask']}')"
        ).fetchone()
    assert n == distinct == len(rows)


def test_syndna_trims_are_always_zero_so_they_cannot_exceed_the_read(tmp_path, monkeypatch):
    """The other invariant the consumers rely on rather than re-check: an incoming
    mask's trims fit inside its read. SynDNA does not trim, so its trims are literal
    zeros — pinned here so a future edit cannot quietly introduce a trim."""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, "ACGTACGT", None), (2, "TT", None)])
    _stub_hits(monkeypatch, [1])
    out = _run(tmp_path, reads, _index(tmp_path))
    with duckdb.connect(":memory:") as conn:
        assert conn.execute(
            f"SELECT count(*) FROM read_parquet('{out['partial_mask']}') "
            "WHERE left_trim1 <> 0 OR right_trim1 <> 0"
        ).fetchone() == (0,)
