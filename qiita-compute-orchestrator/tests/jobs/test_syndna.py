"""Unit tests for `syndna.execute` (the rype classify seam stubbed).

`syndna` extends host_filter's read_mask: it classifies the still-`pass` reads
against a spike-in rype index and marks the hits `spikein_syndna`, retaining their
rows so the counts survive. The real `rype_classify` needs the miint extension and
a built `.ryxdi`, so it is stubbed here and exercised in test_syndna_smoke.py.

Asserted here:
  - only `reason='pass'` rows are classified; an earlier step's verdict (qc_*,
    host_*, twist_no_adaptor) is never overwritten;
  - hits become `spikein_syndna`, non-hits keep their reason, trims ride through;
  - per-spike-in counts are emitted, keyed per prep_sample, one row per read even
    when a read matches several spike-ins (best-scoring bucket wins);
  - `spikein_counts.parquet` lives OUTSIDE the staging dir register-files globs;
  - a missing or empty `.ryxdi` fails fast.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest
from qiita_common.models import ReadMaskReason

_PASS = ReadMaskReason.PASS.value
_SPIKEIN = ReadMaskReason.SPIKEIN_SYNDNA.value


def _write_reads(path: Path, rows: list[tuple[int, int, str]]) -> Path:
    """(prep_sample_idx, sequence_idx, sequence1); single-end, no quals needed."""
    values = ", ".join(
        "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
        "CAST(NULL AS UTINYINT[]), CAST(NULL AS VARCHAR), CAST(NULL AS UTINYINT[]))"
        for _ in rows
    )
    params: list = []
    for ps, sidx, seq in rows:
        params.extend([ps, sidx, f"r{sidx}", seq])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _write_mask(path: Path, rows: list[tuple[int, int, int, str]]) -> Path:
    """(mask_idx, prep_sample_idx, sequence_idx, reason) — host_filter's 8-col shape."""
    values = ", ".join(
        "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), "
        "CAST(0 AS UINTEGER), CAST(0 AS UINTEGER), "
        "CAST(NULL AS UINTEGER), CAST(NULL AS UINTEGER))"
        for _ in rows
    )
    params: list = []
    for mask_idx, ps, sidx, reason in rows:
        params.extend([mask_idx, ps, sidx, reason])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "mask_idx, prep_sample_idx, sequence_idx, reason, "
            "left_trim1, right_trim1, left_trim2, right_trim2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _rows(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT sequence_idx, reason FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


def _counts(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT prep_sample_idx, spikein, read_count FROM read_parquet('{path}') "
            "ORDER BY prep_sample_idx, spikein"
        ).fetchall()


def _index(tmp_path: Path) -> Path:
    d = tmp_path / "syndna.ryxdi"
    d.mkdir()
    (d / "index.bin").write_text("x")
    return d


def _run(tmp_path, reads, mask, index):
    from qiita_compute_orchestrator.jobs import syndna

    return asyncio.run(
        syndna.execute(
            syndna.Inputs(reads=reads, read_mask=mask, syndna_rype_path=index, work_ticket_idx=1),
            tmp_path / "ws",
        )
    )


def _stub_hits(monkeypatch, hits: list[tuple[int, str]]):
    """Stub rype_classify: insert (sequence_idx, spikein) directly."""
    from qiita_compute_orchestrator.jobs import syndna

    def fake(conn, index_path, sequence_table, dest_table, *, threshold):
        # Only reads visible in the query view may be flagged — mirrors the real
        # function, which classifies exactly that relation.
        visible = {r[0] for r in conn.execute(f"SELECT read_id FROM {sequence_table}").fetchall()}
        for sidx, spikein in hits:
            assert sidx in visible, f"stub flagged {sidx}, not in the query view"
            conn.execute(f"INSERT INTO {dest_table} VALUES (?, ?)", [sidx, spikein])

    monkeypatch.setattr(syndna, "_run_rype_classify", fake)


def test_syndna_marks_hits_and_leaves_others(tmp_path, monkeypatch):
    reads = _write_reads(tmp_path / "reads.parquet", [(5, 1, "ACGT"), (5, 2, "TTTT")])
    mask = _write_mask(tmp_path / "mask.parquet", [(9, 5, 1, _PASS), (9, 5, 2, _PASS)])
    _stub_hits(monkeypatch, [(1, "77")])
    out = _run(tmp_path, reads, mask, _index(tmp_path))
    assert _rows(out["read_mask"]) == [(1, _SPIKEIN), (2, _PASS)]


@pytest.mark.parametrize(
    "prior",
    [
        ReadMaskReason.QC_TOO_SHORT.value,
        ReadMaskReason.HOST_RYPE.value,
        ReadMaskReason.TWIST_NO_ADAPTOR.value,
    ],
)
def test_syndna_never_classifies_a_non_pass_read(tmp_path, monkeypatch, prior):
    """An earlier step's verdict survives: the query view is `reason='pass'` only,
    and the merge falls through to `ELSE m.reason`."""
    reads = _write_reads(tmp_path / "reads.parquet", [(5, 1, "ACGT")])
    mask = _write_mask(tmp_path / "mask.parquet", [(9, 5, 1, prior)])
    _stub_hits(monkeypatch, [])  # the stub asserts read 1 is not even visible
    out = _run(tmp_path, reads, mask, _index(tmp_path))
    assert _rows(out["read_mask"]) == [(1, prior)]
    assert _counts(out["spikein_counts"]) == []


def test_syndna_emits_per_spikein_counts_keyed_per_prep_sample(tmp_path, monkeypatch):
    """A bare total would not serve the cell-count model; the bucket_name a
    `bucket_per_feature` index assigns is the spike-in's feature_idx."""
    reads = _write_reads(
        tmp_path / "reads.parquet",
        [(5, 1, "ACGT"), (5, 2, "ACGT"), (5, 3, "TTTT"), (6, 4, "ACGT")],
    )
    mask = _write_mask(
        tmp_path / "mask.parquet",
        [(9, 5, 1, _PASS), (9, 5, 2, _PASS), (9, 5, 3, _PASS), (9, 6, 4, _PASS)],
    )
    _stub_hits(monkeypatch, [(1, "77"), (2, "77"), (4, "88")])
    out = _run(tmp_path, reads, mask, _index(tmp_path))
    assert _counts(out["spikein_counts"]) == [(5, "77", 2), (6, "88", 1)]
    # read 3 was never a spike-in.
    assert _rows(out["read_mask"]) == [(1, _SPIKEIN), (2, _SPIKEIN), (3, _PASS), (4, _SPIKEIN)]


def test_spikein_counts_live_outside_the_register_files_staging_dir(tmp_path, monkeypatch):
    """register-files globs EVERY *.parquet in the staging dir it is handed; a
    counts parquet beside read_mask.parquet would be loaded into the read_mask
    DuckLake table."""
    reads = _write_reads(tmp_path / "reads.parquet", [(5, 1, "ACGT")])
    mask = _write_mask(tmp_path / "mask.parquet", [(9, 5, 1, _PASS)])
    _stub_hits(monkeypatch, [(1, "77")])
    out = _run(tmp_path, reads, mask, _index(tmp_path))
    staging = Path(out["read_mask_staging_dir"])
    assert sorted(p.name for p in staging.glob("*.parquet")) == ["read_mask.parquet"]
    assert Path(out["spikein_counts"]).parent != staging


def test_syndna_shadows_host_filters_binding_names(tmp_path, monkeypatch):
    """The step must emit the SAME names host_filter does, so persist-read-metrics
    and register-files pick up the EXTENDED mask when syndna runs."""
    reads = _write_reads(tmp_path / "reads.parquet", [(5, 1, "ACGT")])
    mask = _write_mask(tmp_path / "mask.parquet", [(9, 5, 1, _PASS)])
    _stub_hits(monkeypatch, [])
    out = _run(tmp_path, reads, mask, _index(tmp_path))
    assert {"read_mask", "read_mask_staging_dir"} <= set(out)


def test_syndna_missing_index_raises(tmp_path, monkeypatch):
    reads = _write_reads(tmp_path / "reads.parquet", [(5, 1, "ACGT")])
    mask = _write_mask(tmp_path / "mask.parquet", [(9, 5, 1, _PASS)])
    _stub_hits(monkeypatch, [])
    with pytest.raises(FileNotFoundError, match="syndna_rype_path"):
        _run(tmp_path, reads, mask, tmp_path / "nope.ryxdi")


def test_syndna_empty_index_raises(tmp_path, monkeypatch):
    """An empty .ryxdi would classify nothing and silently report zero spike-ins."""
    reads = _write_reads(tmp_path / "reads.parquet", [(5, 1, "ACGT")])
    mask = _write_mask(tmp_path / "mask.parquet", [(9, 5, 1, _PASS)])
    _stub_hits(monkeypatch, [])
    empty = tmp_path / "empty.ryxdi"
    empty.mkdir()
    with pytest.raises(ValueError, match="empty directory"):
        _run(tmp_path, reads, mask, empty)


def test_syndna_preserves_mask_idx_and_trims(tmp_path, monkeypatch):
    reads = _write_reads(tmp_path / "reads.parquet", [(5, 1, "ACGT")])
    mask = _write_mask(tmp_path / "mask.parquet", [(9, 5, 1, _PASS)])
    _stub_hits(monkeypatch, [(1, "77")])
    out = _run(tmp_path, reads, mask, _index(tmp_path))
    with duckdb.connect(":memory:") as conn:
        row = conn.execute(
            "SELECT mask_idx, prep_sample_idx, left_trim1, right_trim1, left_trim2, right_trim2 "
            f"FROM read_parquet('{out['read_mask']}')"
        ).fetchone()
    assert row == (9, 5, 0, 0, None, None)
