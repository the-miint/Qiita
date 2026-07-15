"""Tests for `estimate_feature_table.execute` — the metagenomic OGU feature-table job.

The two data-plane streams (`open_alignment_stream`,
`open_reference_sequences_stream`) are faked from local Parquet so no live data
plane is needed — but the analytic itself runs against REAL miint
(`genome_coverage` + `woltka_ogu`): the correctness of that analytic IS the
point of this job, and both functions are cheap on synthetic data. The conftest
stages miint into `MIINT_EXTENSION_DIRECTORY` (mirror by default;
`MIINT_EXTENSION_REPO` overrides to a local build), exactly as a native job LOADs
it at runtime.

Two sections:
  1. orchestration/schema — stream scoping, output plumbing, tmp cleanup, Inputs
     validation, the coverage filter, and the no-genome / empty-result edges;
  2. real-miint correctness ("smoke") — a single synthetic cohort that pins every
     load-bearing semantic in one expected table: cohort-POOLED coverage
     (retain/drop across samples), a read on two contigs of ONE genome counted
     once, a read across TWO genomes split 0.5/0.5, and a multi-contig genome
     whose FULL length (incl. an unaligned contig) is the coverage denominator.

This is the suite that confirms the miint id-type fix: every id column is passed
as its native BIGINT with NO `::VARCHAR` casts. If miint rejects that, these
fail loudly (a mirror without the fix) rather than being papered over.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Parquet writers (correctly-typed to mirror the real DuckLake / resolver output)
# ---------------------------------------------------------------------------


def _write_alignment_parquet(path: Path, rows: list[tuple[int, int, int, int, int, int]]) -> Path:
    """The 6-column alignment slice the DP DoGet projects: (prep_sample_idx,
    sequence_idx, feature_idx, flags, position, stop_position)."""
    with duckdb.connect(":memory:") as conn:
        values = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS BIGINT), "
            "CAST(? AS USMALLINT), CAST(? AS BIGINT), CAST(? AS BIGINT))"
            for _ in rows
        )
        params = [x for r in rows for x in r]
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) "
            'AS t(prep_sample_idx, sequence_idx, feature_idx, flags, "position", stop_position)) '
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _write_lengths_parquet(path: Path, rows: list[tuple[int, int]]) -> Path:
    """The reference_sequences projection this job reads: (feature_idx,
    sequence_length_bp)."""
    with duckdb.connect(":memory:") as conn:
        values = ", ".join("(CAST(? AS BIGINT), CAST(? AS BIGINT))" for _ in rows)
        params = [x for r in rows for x in r]
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t(feature_idx, sequence_length_bp)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _write_map_parquet(path: Path, rows: list[tuple[int, int]]) -> Path:
    """The resolver-staged feature->genome map: (feature_idx, genome_idx) int64,
    exactly as `export_member_genome` writes it."""
    with duckdb.connect(":memory:") as conn:
        values = ", ".join("(CAST(? AS BIGINT), CAST(? AS BIGINT))" for _ in rows)
        params = [x for r in rows for x in r]
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t(feature_idx, genome_idx)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


# ---------------------------------------------------------------------------
# Stream fakes — register the local Parquet as a ONE-SHOT Arrow reader, exactly
# as the real seams do (`do_get().to_reader()` -> `conn.register`). One-shot on
# purpose: a real Flight stream cannot be re-scanned, so if a future edit
# accidentally references a streamed relation twice in one query, these fakes
# surface it (the second scan is empty) instead of masking it with a replayable
# view. The captured dict records the scope args the seam was asked for.
# ---------------------------------------------------------------------------


def _fake_alignment_stream(parquet: Path, captured: dict):
    @asynccontextmanager
    async def fake(conn, *, work_ticket_idx, relation="alignment"):
        captured["work_ticket_idx"] = work_ticket_idx
        conn.register(relation, pq.read_table(str(parquet)).to_reader())
        try:
            yield relation
        finally:
            conn.unregister(relation)

    return fake


def _fake_lengths_stream(parquet: Path, captured: dict):
    @asynccontextmanager
    async def fake(conn, *, reference_idx, relation="reference_lengths"):
        captured["reference_idx"] = reference_idx
        conn.register(relation, pq.read_table(str(parquet)).to_reader())
        try:
            yield relation
        finally:
            conn.unregister(relation)

    return fake


def _install_fakes(m, monkeypatch, *, alignment_parquet, lengths_parquet) -> dict:
    """Patch both stream seams and return the captured-scope dict shared by them."""
    captured: dict = {}
    monkeypatch.setattr(
        m, "open_alignment_stream", _fake_alignment_stream(alignment_parquet, captured)
    )
    monkeypatch.setattr(
        m, "open_reference_sequences_stream", _fake_lengths_stream(lengths_parquet, captured)
    )
    return captured


def _read_ogu(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT prep_sample_idx, genome_idx, value FROM read_parquet('{path}') ORDER BY 1, 2"
        ).fetchall()


def _columns(path: Path) -> list[str]:
    with duckdb.connect(":memory:") as conn:
        desc = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    return [d[0] for d in desc]


def _run(m, *, tmp_path, monkeypatch, alignment, lengths, mapping, threshold, ref_idx=7, wt_idx=42):
    """Seed the three Parquet inputs, install the stream fakes, run execute, and
    return (output_map, captured_scope)."""
    align_pq = _write_alignment_parquet(tmp_path / "alignment.parquet", alignment)
    lengths_pq = _write_lengths_parquet(tmp_path / "lengths.parquet", lengths)
    map_pq = _write_map_parquet(tmp_path / "map.parquet", mapping)
    captured = _install_fakes(
        m, monkeypatch, alignment_parquet=align_pq, lengths_parquet=lengths_pq
    )
    inputs = m.Inputs(
        reference_idx=ref_idx,
        work_ticket_idx=wt_idx,
        coverage_threshold=threshold,
        genome_map_path=map_pq,
    )
    out = asyncio.run(m.execute(inputs, tmp_path / "ws"))
    return out, captured


# ---------------------------------------------------------------------------
# Orchestration / schema
# ---------------------------------------------------------------------------


def test_execute_streams_scopes_writes_and_schema(tmp_path, monkeypatch):
    """Streams are scoped to work_ticket_idx (alignment) and reference_idx
    (lengths); the output is `ogu_table.parquet` under the workspace, returned
    under the `ogu_table` key, with schema (prep_sample_idx, genome_idx, value)."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    # Two single-mapped reads, one genome (200) at one contig (20), plenty of
    # coverage; threshold trivially met.
    out, captured = _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        alignment=[(1, 1, 20, 0, 0, 50), (1, 2, 20, 0, 50, 100)],
        lengths=[(20, 100)],
        mapping=[(20, 200)],
        threshold=0.01,
        ref_idx=7,
        wt_idx=42,
    )

    assert captured["work_ticket_idx"] == 42
    assert captured["reference_idx"] == 7

    out_path = out["ogu_table"]
    assert out_path == tmp_path / "ws" / "ogu_table.parquet"
    assert out_path.is_file()

    assert _columns(out_path) == ["prep_sample_idx", "genome_idx", "value"]
    # Two reads, both to genome 200 -> value 2.0 for one sample.
    assert _read_ogu(out_path) == [(1, 200, 2.0)]


def test_output_is_parquet_v2_zstd(tmp_path, monkeypatch):
    """Repo convention: result Parquet is v2 + zstd (PARQUET_OPTS)."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    out, _ = _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        alignment=[(1, 1, 20, 0, 0, 50)],
        lengths=[(20, 100)],
        mapping=[(20, 200)],
        threshold=0.01,
    )
    with duckdb.connect(":memory:") as conn:
        comps = {
            r[0]
            for r in conn.execute(
                f"SELECT DISTINCT compression FROM parquet_metadata('{out['ogu_table']}')"
            ).fetchall()
        }
    assert comps == {"ZSTD"}


def test_cleans_duckdb_tmp(tmp_path, monkeypatch):
    """The DuckDB spill dir under the workspace is removed after the run."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        alignment=[(1, 1, 20, 0, 0, 50)],
        lengths=[(20, 100)],
        mapping=[(20, 200)],
        threshold=0.01,
    )
    assert not (tmp_path / "ws" / ".duckdb_tmp").exists()


def test_below_threshold_genome_is_dropped(tmp_path, monkeypatch):
    """A genome whose pooled coverage is below the threshold is excluded from the
    table (even though the read still 'assigns' to it in woltka terms)."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    # 10 bp covered of a 10000 bp genome = 0.1% < 1%.
    out, _ = _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        alignment=[(1, 1, 20, 0, 0, 10)],
        lengths=[(20, 10000)],
        mapping=[(20, 200)],
        threshold=0.01,
    )
    assert _read_ogu(out["ogu_table"]) == []


def test_no_genome_feature_is_ignored(tmp_path, monkeypatch):
    """An alignment to a feature with no genome (absent from the map — e.g. a 16S
    record) is dropped by the inner join, not an error."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    out, _ = _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        # feature 20 -> genome 200 (mapped); feature 999 -> no genome.
        alignment=[(1, 1, 20, 0, 0, 50), (1, 2, 999, 0, 0, 50)],
        lengths=[(20, 100)],
        mapping=[(20, 200)],
        threshold=0.01,
    )
    assert _read_ogu(out["ogu_table"]) == [(1, 200, 1.0)]


def test_empty_result_writes_valid_empty_table(tmp_path, monkeypatch):
    """No genome meeting the threshold is a valid (empty) table, not a failure —
    the OGU table is computed on demand and an empty cohort result is legitimate."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    out, _ = _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        alignment=[(1, 1, 20, 0, 0, 10)],
        lengths=[(20, 10000)],
        mapping=[(20, 200)],
        threshold=0.99,
    )
    out_path = out["ogu_table"]
    assert out_path.is_file()
    assert _columns(out_path) == ["prep_sample_idx", "genome_idx", "value"]
    assert _read_ogu(out_path) == []


def test_no_mapped_features_writes_valid_empty_table(tmp_path, monkeypatch):
    """When NO alignment maps to a genome (all feature_idx absent from the map),
    `ogu_input` is empty — `woltka_ogu` rejects an all-NULL sample_id source, so
    the job must short-circuit to a valid 0-row table rather than crash."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    out, _ = _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        # Every aligned feature (998, 999) is absent from the map -> ogu_input
        # is empty. genome 200 is mapped but nothing aligns to it.
        alignment=[(1, 1, 998, 0, 0, 50), (1, 2, 999, 0, 0, 50)],
        lengths=[(20, 100)],
        mapping=[(20, 200)],
        threshold=0.01,
    )
    out_path = out["ogu_table"]
    assert out_path.is_file()
    assert _columns(out_path) == ["prep_sample_idx", "genome_idx", "value"]
    assert _read_ogu(out_path) == []


def test_dropped_genome_renormalizes_survivor_before_woltka(tmp_path, monkeypatch):
    """A read hitting a SURVIVING genome and a DROPPED genome renormalizes to 1.0 on
    the survivor. Non-surviving genomes are removed from woltka's INPUT (not its
    output), so the read maps to a single unique reference and is counted whole —
    filtering woltka's output instead would strand it at 0.5."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    out, _ = _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        # read 1 hits feature 20 (genome 200) AND feature 30 (genome 300).
        alignment=[(1, 1, 20, 0, 0, 60), (1, 1, 30, 0, 0, 30)],
        # genome 200: 60/100 = 60% -> survives; genome 300: 30/100000 = 0.03% -> dropped.
        lengths=[(20, 100), (30, 100000)],
        mapping=[(20, 200), (30, 300)],
        threshold=0.01,
    )
    # Renormalized to the survivor: 1.0, not 0.5.
    assert _read_ogu(out["ogu_table"]) == [(1, 200, 1.0)]


def test_threshold_zero_admits_all_and_skips_coverage(tmp_path, monkeypatch):
    """coverage_threshold == 0 admits every genome with any alignment and SKIPS the
    coverage calc entirely — the reference-lengths stream (its only input) is never
    opened. The same 0.1%-coverage genome `test_below_threshold_genome_is_dropped`
    drops at 1% is RETAINED here."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    out, captured = _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        alignment=[(1, 1, 20, 0, 0, 10)],  # 10 bp of a 10000 bp genome = 0.1%
        lengths=[(20, 10000)],  # present but never read — the lengths stream is skipped
        mapping=[(20, 200)],
        threshold=0.0,
    )
    # The lengths stream (the sole coverage-calc input) is never opened at threshold 0.
    assert "reference_idx" not in captured
    assert _read_ogu(out["ogu_table"]) == [(1, 200, 1.0)]


@pytest.mark.parametrize("bad", [-0.1, 1.1])
def test_inputs_coverage_threshold_bounds(bad, tmp_path):
    """coverage_threshold is a proportion in [0, 1]; out-of-range is rejected."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    with pytest.raises(ValueError):
        m.Inputs(
            reference_idx=1,
            work_ticket_idx=1,
            coverage_threshold=bad,
            genome_map_path=tmp_path / "m",
        )


# ---------------------------------------------------------------------------
# Real-miint correctness ("smoke") — one synthetic cohort pinning every semantic.
#
# Genomes / contigs (lengths):
#   G100: f10(1000)+f11(1000)          -> 2000  (multi-contig)
#   G200: f20(10000)                   -> 10000 (pooled-coverage RETAIN)
#   G300: f30(10000)                   -> 10000 (pooled-coverage DROP)
#   G400: f40(1000)+f41(3000 unaligned)-> 4000  (two-genome split; unaligned len)
#   G500: f50(1000)                    -> 1000  (two-genome split)
#   G600: f60(1000)+f61(2000 unaligned)-> 3000  (unaligned-length DROP)
# ---------------------------------------------------------------------------

_SMOKE_MAP = [
    (10, 100),
    (11, 100),
    (20, 200),
    (30, 300),
    (40, 400),
    (41, 400),
    (50, 500),
    (60, 600),
    (61, 600),
]
_SMOKE_LENGTHS = [
    (10, 1000),
    (11, 1000),
    (20, 10000),
    (30, 10000),
    (40, 1000),
    (41, 3000),
    (50, 1000),
    (60, 1000),
    (61, 2000),
]
_SMOKE_ALIGNMENT = [
    # (prep_sample_idx, sequence_idx, feature_idx, flags, position, stop_position)
    (1, 1, 10, 0, 0, 500),
    (1, 1, 11, 0, 0, 500),  # one read, two contigs of G100
    (1, 2, 20, 0, 0, 60),  # G200 sample 1 (0.6%)
    (1, 3, 30, 0, 0, 30),  # G300 sample 1
    (1, 4, 40, 0, 0, 500),
    (1, 4, 50, 0, 0, 500),  # one read, two genomes G400+G500
    (1, 7, 60, 0, 0, 20),  # G600: 20bp of 3000 -> 0.667%
    (2, 5, 20, 0, 60, 120),  # G200 sample 2 (extends -> pooled 1.2%)
    (2, 6, 30, 0, 30, 50),  # G300 sample 2 (pooled 0.5%)
]


def test_smoke_full_ogu_table(tmp_path, monkeypatch):
    """The whole recipe against real miint, one expected table proving:

    (i)   POOLED coverage: G200 is 0.6%+0.6% across two samples in extending
          regions -> 1.2% pooled -> RETAINED; G300 is 0.5% pooled -> DROPPED.
    (ii)  A read hitting two contigs of ONE genome (G100) is counted ONCE (1.0).
    (iii) A read hitting TWO genomes (G400, G500) contributes 0.5 to each.
    (iv)  G600 is DROPPED: 20/3000 = 0.67% (full length, incl. the unaligned
          f61) < 1%; on aligned-length-only (20/1000 = 2%) it would survive, so
          its absence proves the unaligned contig is in the denominator.
    """
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    out, _ = _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        alignment=_SMOKE_ALIGNMENT,
        lengths=_SMOKE_LENGTHS,
        mapping=_SMOKE_MAP,
        threshold=0.01,
    )
    assert _read_ogu(out["ogu_table"]) == [
        (1, 100, 1.0),  # (ii) two contigs, one read, one genome -> once
        (1, 200, 1.0),  # (i)  retained (pooled 1.2%)
        (1, 400, 0.5),  # (iii) two-genome split
        (1, 500, 0.5),  # (iii) two-genome split
        (2, 200, 1.0),  # (i)  retained, sample 2
        # absent: G300 (dropped 0.5%), G600 (dropped 0.67% via unaligned length)
    ]
