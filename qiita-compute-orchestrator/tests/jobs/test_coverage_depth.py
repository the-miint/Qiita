"""coverage_depth job: the plumbing around the (separately smoke-tested) arithmetic.

The depth SQL is pinned against the real miint build in `test_coverage_depth_smoke`. What
is exercised here is everything the job does AROUND it — pulling the windows over Flight,
the sample set, the failure modes — with the Flight seam stubbed, as the shard-builder
tests stub it.
"""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path

import duckdb
import pytest

from qiita_compute_orchestrator.jobs import coverage_depth

PREP_SAMPLE_IDX = 7
REFERENCE_IDX = 3
COVERAGE_IDX = 42
PARENT = 100  # the plasmid
INSERT_A = 200
INSERT_B = 201

PARENT_LEN = 17_263
WIN_A = (7_360, 9_907)  # the insert: 2547 bp
WIN_B = (12_000, 12_500)  # a second interval, 500 bp, with no reads


def _write_alignment(path: Path, rows: list[tuple]) -> Path:
    """(prep_sample_idx, sequence_idx, parent_feature_idx, flags, position, stop, cigar)"""
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE a (prep_sample_idx BIGINT, sequence_idx BIGINT, "
            "parent_feature_idx BIGINT, flags USMALLINT, position BIGINT, "
            "stop_position BIGINT, cigar VARCHAR)"
        )
        for r in rows:
            conn.execute("INSERT INTO a VALUES (?, ?, ?, ?, ?, ?, ?)", list(r))
        conn.execute(f"COPY a TO '{path}' (FORMAT PARQUET)")
    return path


def _stub_flight(monkeypatch, *, annotations: list[tuple], sequences: list[tuple]):
    """Stub the Flight stream: serve `reference_annotation` / `reference_sequences` from
    in-memory rows instead of the data plane."""

    @contextlib.asynccontextmanager
    async def fake(conn, *, reference_idx, table, feature_idx=None, relation):
        assert reference_idx == REFERENCE_IDX
        if table == "reference_annotation":
            conn.execute(
                f"CREATE OR REPLACE TABLE {relation} (feature_idx BIGINT, "
                "parent_feature_idx BIGINT, position BIGINT, stop_position BIGINT)"
            )
            for r in annotations:
                conn.execute(f"INSERT INTO {relation} VALUES (?, ?, ?, ?)", list(r))
        elif table == "reference_sequences":
            conn.execute(
                f"CREATE OR REPLACE TABLE {relation} (feature_idx BIGINT, "
                "sequence_hash VARCHAR, sequence_length_bp BIGINT)"
            )
            for r in sequences:
                conn.execute(f"INSERT INTO {relation} VALUES (?, 'x', ?)", list(r))
        else:  # pragma: no cover - a new table would be a wiring mistake
            raise AssertionError(f"unexpected table {table!r}")
        yield relation

    monkeypatch.setattr(coverage_depth, "open_reference_table_stream", fake)


def _run(tmp_path, alignment: Path) -> dict:
    return asyncio.run(
        coverage_depth.execute(
            coverage_depth.Inputs(
                alignment=alignment,
                reference_idx=REFERENCE_IDX,
                coverage_idx=COVERAGE_IDX,
                prep_sample_idx=PREP_SAMPLE_IDX,
                work_ticket_idx=1,
            ),
            tmp_path / "ws",
        )
    )


def _rows(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT coverage_idx, prep_sample_idx, feature_idx, covered_bases, "
            f"       feature_length, occurrences, mean_depth FROM read_parquet('{path}') "
            "ORDER BY feature_idx"
        ).fetchall()


_ANNOTATIONS = [(INSERT_A, PARENT, *WIN_A), (INSERT_B, PARENT, *WIN_B)]
_SEQUENCES = [(PARENT, PARENT_LEN)]


def test_emits_a_dense_feature_table_with_the_coverage_idx(tmp_path, monkeypatch):
    """One row per (sample, feature) — including the feature with no reads. A feature table
    must distinguish 'measured, and it was zero' from 'not measured'."""
    _stub_flight(monkeypatch, annotations=_ANNOTATIONS, sequences=_SEQUENCES)
    # One read covering the whole of insert A; nothing on insert B.
    aln = _write_alignment(
        tmp_path / "aln.parquet",
        [(PREP_SAMPLE_IDX, 1, PARENT, 0, WIN_A[0], WIN_A[1], "2547=")],
    )
    out = _run(tmp_path, aln)
    assert set(out) == {"coverage", "coverage_staging_dir"}
    # The basename IS the DuckLake table name — register-files maps it.
    assert out["coverage"].name == "coverage.parquet"

    rows = _rows(out["coverage"])
    assert rows == [
        (COVERAGE_IDX, PREP_SAMPLE_IDX, INSERT_A, 2547, 2547, 1, 1.0),
        (COVERAGE_IDX, PREP_SAMPLE_IDX, INSERT_B, 0, 500, 1, 0.0),
    ]


def test_a_sample_with_no_alignment_still_gets_zero_rows(tmp_path, monkeypatch):
    """The sample set comes from the TICKET, not from the alignment. A spike-in that failed
    to amplify produces no alignment rows at all, and it must still be reported as zero —
    otherwise it is indistinguishable from a sample that was never measured."""
    _stub_flight(monkeypatch, annotations=_ANNOTATIONS, sequences=_SEQUENCES)
    empty = _write_alignment(tmp_path / "empty.parquet", [])

    rows = _rows(_run(tmp_path, empty)["coverage"])
    assert [(r[2], r[3], r[6]) for r in rows] == [(INSERT_A, 0, 0.0), (INSERT_B, 0, 0.0)]
    assert all(r[1] == PREP_SAMPLE_IDX for r in rows)


def test_the_gate_is_applied_here_not_upstream(tmp_path, monkeypatch):
    """The alignment arrives UNGATED (mapped-primary only) and this job applies the
    identity + aligned-fraction gate — so the definition lives in one place. A read that is
    only 25% aligned must contribute nothing."""
    _stub_flight(monkeypatch, annotations=_ANNOTATIONS, sequences=_SEQUENCES)
    aln = _write_alignment(
        tmp_path / "aln.parquet",
        [
            # 500 bp aligned out of a 2000 bp read -> aligned fraction 0.25, below 0.90.
            (PREP_SAMPLE_IDX, 1, PARENT, 0, WIN_A[0], WIN_A[0] + 500, "1500S500="),
        ],
    )
    rows = _rows(_run(tmp_path, aln)["coverage"])
    by_feature = {r[2]: r[3] for r in rows}
    assert by_feature[INSERT_A] == 0, "a 25%-aligned read must be rejected by the gate here"


def test_a_reference_with_no_annotations_is_refused(tmp_path, monkeypatch):
    """An unannotated reference cannot be quantified per-interval. Failing is the only
    honest outcome: an empty feature table is indistinguishable from 'every insert had zero
    coverage', which is a real and very different finding."""
    _stub_flight(monkeypatch, annotations=[], sequences=_SEQUENCES)
    aln = _write_alignment(tmp_path / "aln.parquet", [])
    with pytest.raises(ValueError, match="no annotated intervals"):
        _run(tmp_path, aln)


def test_a_parent_with_no_sequence_length_is_refused(tmp_path, monkeypatch):
    """compute_coverage_depth sizes its per-base array to the parent's length. A missing
    length would silently truncate the array and lose every base past it, so the reference
    disagreeing with itself is a hard failure."""
    _stub_flight(monkeypatch, annotations=_ANNOTATIONS, sequences=[])  # no lengths
    aln = _write_alignment(tmp_path / "aln.parquet", [])
    with pytest.raises(ValueError, match="no sequence length"):
        _run(tmp_path, aln)
