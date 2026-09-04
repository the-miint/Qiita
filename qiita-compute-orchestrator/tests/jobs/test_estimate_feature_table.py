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
from qiita_common import analytic

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
    async def fake(conn, *, work_ticket_idx, columns, relation="alignment"):
        captured["work_ticket_idx"] = work_ticket_idx
        captured["columns"] = list(columns)
        # PROJECT, exactly as the real DoGet does — the data plane streams the
        # signed columns and nothing else. A fixture wide enough to satisfy a
        # SELECT the job never asked for would hide precisely the drift this
        # projection exists to prevent, so `.select()` raises here instead.
        table = pq.read_table(str(parquet)).select(list(columns))
        conn.register(relation, table.to_reader())
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
    assert captured["columns"] == list(analytic.ALIGNMENT_COLUMNS)

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


def test_partial_output_removed_on_failure(tmp_path, monkeypatch):
    """A failure mid-compute removes any partial COPY output, so the SLURM launcher's
    manifest walker (which runs after execute()) can't promote a half-written file as
    the result — the `success`/`finally: unlink` guard the sibling jobs use."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    align_pq = _write_alignment_parquet(tmp_path / "alignment.parquet", [(1, 1, 20, 0, 0, 50)])
    lengths_pq = _write_lengths_parquet(tmp_path / "lengths.parquet", [(20, 100)])
    map_pq = _write_map_parquet(tmp_path / "map.parquet", [(20, 200)])
    _install_fakes(m, monkeypatch, alignment_parquet=align_pq, lengths_parquet=lengths_pq)

    def boom(conn, *, coverage_threshold, out_path, combined):
        out_path.write_bytes(b"partial")  # a half-written COPY output
        raise RuntimeError("compute blew up")

    monkeypatch.setattr(m, "_write_ogu_table", boom)

    inputs = m.Inputs(
        reference_idx=7, work_ticket_idx=42, coverage_threshold=0.01, genome_map_path=map_pq
    )
    with pytest.raises(RuntimeError, match="compute blew up"):
        asyncio.run(m.execute(inputs, tmp_path / "ws"))
    assert not (tmp_path / "ws" / m.OGU_TABLE_FILENAME).exists()


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


def test_job_asks_for_exactly_the_columns_it_binds(tmp_path, monkeypatch):
    """The requested projection and the bound SELECT list are ONE list.

    They used to be two hand-written copies in different components — the data
    plane's hardcoded projection and this job's CREATE TABLE — with no way for
    either to see the other. Now the job owns the list and the data plane serves
    what it signed, so the only thing left to pin is that the job does not
    reintroduce the split by hardcoding a second copy in its SQL.

    The stream fake projects to the requested columns (see `_fake_alignment_stream`),
    so a SELECT naming anything the job did not ask for fails to bind — which
    makes every other test in this module a check on this property too.
    """
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    _, captured = _run(
        m,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        alignment=[(1, 1, 20, 0, 0, 50)],
        lengths=[(20, 100)],
        mapping=[(20, 200)],
        threshold=0.01,
    )

    assert captured["columns"] == list(analytic.ALIGNMENT_COLUMNS)
    # This recipe never reads `cigar`, and leaving it out is most of what the
    # projection buys — the one regression a future edit is likeliest to add.
    assert "cigar" not in captured["columns"]


# ---------------------------------------------------------------------------
# The combined (inverted open reference) path: two arms, one woltka pass.
#
# What is pinned HERE is this driver's WIRING — that both arms' inputs are fetched
# with the right scope and handed to the shared analytic in the right order, and
# that absent a de novo map nothing about the reference-only job changes. The
# analytic's own rules (precedence, the sample-keyed map join, the length dedupe)
# are pinned against real miint in
# `qiita-common/tests/analytic/test_behaviour_miint.py`; re-asserting them here
# would be a second copy that drifts.
# ---------------------------------------------------------------------------


def _write_denovo_map_parquet(path: Path, rows: list[tuple[int, int, int]]) -> Path:
    """The resolver-staged de novo map: (prep_sample_idx, feature_idx, genome_idx),
    as `export_assembly_member_genome` writes it — three columns, because the sample
    is part of the join key on this arm."""
    with duckdb.connect(":memory:") as conn:
        values = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS BIGINT))" for _ in rows
        )
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) "
            f"AS t(prep_sample_idx, feature_idx, genome_idx)) TO '{path}' (FORMAT PARQUET)",
            [x for r in rows for x in r],
        )
    return path


def _write_denovo_quality_parquet(
    path: Path, rows: list[tuple[int, int, float | None, float | None]]
) -> Path:
    """The resolver-staged de novo quality: (prep_sample_idx, genome_idx,
    completeness, contamination), as `_write_denovo_genome_quality` writes it.

    The scores are nullable, so the fixtures carry a NULL row: a genome CheckM did
    not score is an ordinary state, not an edge case.
    """
    with duckdb.connect(":memory:") as conn:
        values = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS DOUBLE), CAST(? AS DOUBLE))"
            for _ in rows
        )
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) "
            f"AS t(prep_sample_idx, genome_idx, completeness, contamination)) "
            f"TO '{path}' (FORMAT PARQUET)",
            [x for r in rows for x in r],
        )
    return path


def _fake_two_arm_alignment_stream(reference_parquet: Path, denovo_parquet: Path, captured: dict):
    """The alignment seam for a combined run: ONE mint route, asked twice, with the
    `denovo` flag picking the arm.

    Records the flags in call order, so a driver that minted the reference arm twice
    — the mistake the CP's own 422 exists to prevent — surfaces as `[False, False]`
    rather than as a table that merely looks small.
    """

    @asynccontextmanager
    async def fake(conn, *, work_ticket_idx, columns, relation="alignment", denovo=False):
        captured.setdefault("arms", []).append(denovo)
        captured["work_ticket_idx"] = work_ticket_idx
        table = pq.read_table(str(denovo_parquet if denovo else reference_parquet))
        conn.register(relation, table.select(list(columns)).to_reader())
        try:
            yield relation
        finally:
            conn.unregister(relation)

    return fake


def _fake_assembled_sequence_stream(per_sample: dict[int, Path], captured: dict):
    """The de novo lengths seam: run-scoped, so ONE call per cohort sample.

    Records which samples it was asked for, which is the property that matters here
    — the job derives that list from the staged map rather than from a second input,
    so a sample the map never named must not be asked for at all (the data plane
    would 404 on a run that legitimately produced nothing).
    """

    @asynccontextmanager
    async def fake(conn, *, prep_sample_idx, processing_idx, relation="assembled_lengths"):
        captured.setdefault("lengths_samples", []).append(prep_sample_idx)
        captured["lengths_processing_idx"] = processing_idx
        conn.register(relation, pq.read_table(str(per_sample[prep_sample_idx])).to_reader())
        try:
            yield relation
        finally:
            conn.unregister(relation)

    return fake


def _run_combined(m, *, tmp_path, monkeypatch, threshold=0.01, denovo_map=None, quality=None):
    """Run `execute` over a two-arm fixture and return (output_map, captured).

    Contig 50 is BOTH a reference sequence of genome 300 and a contig each sample
    assembled — the shared-feature case the content-addressed `feature_idx` creates,
    and the one that makes the arms' maps disagree about which genome it belongs to.
    Read 3 is placed by both arms; read 6 is sample 2's own placement on the same
    contig.
    """
    reference_pq = _write_alignment_parquet(
        tmp_path / "ref_align.parquet",
        [
            (1, 1, 10, 0, 0, 500),
            (1, 3, 50, 0, 0, 500),
            (2, 4, 10, 0, 0, 500),
        ],
    )
    denovo_pq = _write_alignment_parquet(
        tmp_path / "dn_align.parquet",
        [(1, 3, 50, 0, 0, 500), (2, 6, 50, 0, 0, 500)],
    )
    ref_lengths_pq = _write_lengths_parquet(tmp_path / "ref_len.parquet", [(10, 1000), (50, 1000)])
    map_pq = _write_map_parquet(tmp_path / "map.parquet", [(10, 100), (50, 300)])
    denovo_map_pq = _write_denovo_map_parquet(
        tmp_path / "dn_map.parquet",
        [(1, 50, 900), (2, 50, 901)] if denovo_map is None else denovo_map,
    )
    # One scored genome and one unscored, which is the mixed state a real run is in.
    denovo_quality_pq = _write_denovo_quality_parquet(
        tmp_path / "dn_quality.parquet",
        [(1, 900, 95.0, 1.0), (2, 901, None, None)] if quality is None else quality,
    )
    # Contig 50 is on BOTH samples' length streams — one content-addressed feature,
    # streamed once per sample, which is what the roll-up's dedupe exists for.
    per_sample = {
        1: _write_lengths_parquet(tmp_path / "dn_len_1.parquet", [(50, 1000)]),
        2: _write_lengths_parquet(tmp_path / "dn_len_2.parquet", [(50, 1000)]),
    }

    captured: dict = {}
    monkeypatch.setattr(
        m,
        "open_alignment_stream",
        _fake_two_arm_alignment_stream(reference_pq, denovo_pq, captured),
    )
    monkeypatch.setattr(
        m, "open_reference_sequences_stream", _fake_lengths_stream(ref_lengths_pq, captured)
    )
    monkeypatch.setattr(
        m, "open_assembled_sequence_stream", _fake_assembled_sequence_stream(per_sample, captured)
    )
    inputs = m.Inputs(
        reference_idx=7,
        work_ticket_idx=42,
        coverage_threshold=threshold,
        genome_map_path=map_pq,
        denovo_genome_map_path=denovo_map_pq,
        denovo_processing_idx=11,
        denovo_genome_quality_path=denovo_quality_pq,
    )
    out = asyncio.run(m.execute(inputs, tmp_path / "ws"))
    return out, captured


def test_combined_run_mints_both_arms_and_counts_each_read_once(tmp_path, monkeypatch):
    """The driver's whole job in one assertion. Read 3 is placed by both arms and
    lands on the de novo side; reads 1 and 4 are reference-only remainders; read 6 is
    sample 2's own. Genome 300 is absent — contig 50 was its only aligned contig and
    precedence took its only read.

    The counts themselves come from the shared analytic, so what this pins is that
    the job fed it two arms rather than one twice.
    """
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    out, captured = _run_combined(m, tmp_path=tmp_path, monkeypatch=monkeypatch)
    assert captured["arms"] == [False, True], "the reference arm first, then the de novo one"
    assert _read_ogu(out["ogu_table"]) == [
        (1, 100, 1.0),  # read 1
        (1, 900, 1.0),  # read 3, won from the reference arm
        (2, 100, 1.0),  # read 4
        (2, 901, 1.0),  # read 6, against ITS sample's genome
    ]


def test_combined_run_reads_lengths_only_for_the_samples_the_map_named(tmp_path, monkeypatch):
    """The cohort for the lengths comes from the STAGED MAP, not from a second
    input. A sample that assembled nothing is absent from the map, and asking the
    data plane for its contigs would 404 on a run that legitimately produced none —
    so the map is the only list, read at the run the resolver derived.
    """
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    _out, captured = _run_combined(
        m, tmp_path=tmp_path, monkeypatch=monkeypatch, denovo_map=[(1, 50, 900)]
    )
    assert captured["lengths_samples"] == [1], "sample 2 is not in the map, so it is not asked for"
    assert captured["lengths_processing_idx"] == 11


def test_the_staged_quality_relation_keeps_the_unscored_genomes(tmp_path):
    """Round-trip the resolver's Parquet through the staging SQL the job runs.

    The two halves are pinned apart — the CP suite asserts what lands in the Parquet,
    `test_stage` asserts the SQL's shape — and this is where they meet: the columns
    the resolver writes are the columns the CTAS projects, and a genome with NULL
    scores survives as a row rather than being dropped somewhere between them.
    """
    quality_pq = _write_denovo_quality_parquet(
        tmp_path / "q.parquet", [(1, 900, 95.0, 1.0), (2, 901, None, None)]
    )
    with duckdb.connect(":memory:") as conn:
        conn.execute(analytic.denovo_genome_quality_table_sql(f"read_parquet('{quality_pq}')"))
        rows = conn.execute(
            f"SELECT prep_sample_idx, genome_id, completeness, contamination "
            f"FROM {analytic.DENOVO_GENOME_QUALITY_TABLE} ORDER BY genome_id"
        ).fetchall()
    # `genome_idx` in, `genome_id` out, and the unscored genome is still here.
    assert rows == [(1, 900, 95.0, 1.0), (2, 901, None, None)]


def test_a_combined_ticket_without_the_quality_path_fails_loud(tmp_path, monkeypatch):
    """The map and the quality are bound by ONE resolver pass, so a combined ticket
    carrying the map without the quality is a broken binding, not a lighter request.

    The alternative — skipping the relation — does not fail either: it leaves every
    assembled genome looking unscored, which is what a run CheckM legitimately scored
    nothing in also looks like.
    """
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    inputs = m.Inputs(
        reference_idx=7,
        work_ticket_idx=42,
        coverage_threshold=0.01,
        genome_map_path=_write_map_parquet(tmp_path / "map.parquet", [(10, 100)]),
        denovo_genome_map_path=_write_denovo_map_parquet(
            tmp_path / "dn_map.parquet", [(1, 50, 900)]
        ),
        denovo_processing_idx=11,
    )
    with pytest.raises(ValueError, match="denovo_genome_quality_path"):
        asyncio.run(m.execute(inputs, tmp_path / "ws"))


def test_a_reference_only_ticket_needs_no_quality(tmp_path, monkeypatch):
    """The mirror of the check above: absent the de novo arm there is nothing to
    score, so the unbound quality path is the normal state rather than a gap."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    inputs = m.Inputs(
        reference_idx=7,
        work_ticket_idx=42,
        coverage_threshold=0.01,
        genome_map_path=_write_map_parquet(tmp_path / "map.parquet", [(10, 100)]),
    )
    assert inputs.denovo_genome_quality_path is None


def test_a_zero_threshold_skips_both_arms_lengths(tmp_path, monkeypatch):
    """The lengths feed only the coverage calc, on both arms alike. At 0 there is no
    survivor set to build, so neither stream is opened — and precedence still has to
    hold without them, being the only thing left standing between the arms."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    out, captured = _run_combined(m, tmp_path=tmp_path, monkeypatch=monkeypatch, threshold=0.0)
    assert "reference_idx" not in captured, "the reference lengths stream stayed shut"
    assert "lengths_samples" not in captured, "and so did every de novo one"
    values = {(s, g): v for s, g, v in _read_ogu(out["ogu_table"])}
    assert (1, 300) not in values, "read 3 still went to the de novo arm"
    assert values[(1, 900)] == 1.0


def test_a_reference_only_run_asks_for_no_denovo_arm(tmp_path, monkeypatch):
    """The control. Without `denovo_genome_map_path` the job mints ONE alignment
    ticket, on the reference arm, and never reaches the assembly seam — so a
    reference-only ticket is dispatched exactly as it was before the arm existed."""
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    captured: dict = {}
    align_pq = _write_alignment_parquet(tmp_path / "alignment.parquet", [(1, 1, 10, 0, 0, 500)])
    lengths_pq = _write_lengths_parquet(tmp_path / "lengths.parquet", [(10, 1000)])
    map_pq = _write_map_parquet(tmp_path / "map.parquet", [(10, 100)])
    monkeypatch.setattr(
        m, "open_alignment_stream", _fake_two_arm_alignment_stream(align_pq, align_pq, captured)
    )
    monkeypatch.setattr(
        m, "open_reference_sequences_stream", _fake_lengths_stream(lengths_pq, captured)
    )
    monkeypatch.setattr(
        m, "open_assembled_sequence_stream", _fake_assembled_sequence_stream({}, captured)
    )
    out = asyncio.run(
        m.execute(
            m.Inputs(
                reference_idx=7,
                work_ticket_idx=42,
                coverage_threshold=0.01,
                genome_map_path=map_pq,
            ),
            tmp_path / "ws",
        )
    )
    assert captured["arms"] == [False]
    assert "lengths_samples" not in captured
    assert _read_ogu(out["ogu_table"]) == [(1, 100, 1.0)]


@pytest.mark.parametrize("threshold", [0.01, 0.0])
def test_a_denovo_map_without_its_run_fails_loudly(tmp_path, monkeypatch, threshold):
    """The two de novo fields are separately optional on the wire but are one fact —
    the resolver binds both or neither. Skipping the lengths instead of raising does
    not fail either: it leaves every qiita genome with no denominator, drops them all
    at the coverage filter, and returns a reference-only table under the combined
    name.

    Parametrized over the threshold because the check's one USE is behind the
    coverage-filter branch: at 0 the lengths are never staged, so a guard placed at
    that use would let the broken binding through in exactly the case that needs no
    lengths — and still produce a table.
    """
    from qiita_compute_orchestrator.jobs import estimate_feature_table as m

    align_pq = _write_alignment_parquet(tmp_path / "alignment.parquet", [(1, 1, 10, 0, 0, 500)])
    lengths_pq = _write_lengths_parquet(tmp_path / "lengths.parquet", [(10, 1000)])
    map_pq = _write_map_parquet(tmp_path / "map.parquet", [(10, 100)])
    denovo_map_pq = _write_denovo_map_parquet(tmp_path / "dn_map.parquet", [(1, 50, 900)])
    captured: dict = {}
    monkeypatch.setattr(
        m, "open_alignment_stream", _fake_two_arm_alignment_stream(align_pq, align_pq, captured)
    )
    monkeypatch.setattr(
        m, "open_reference_sequences_stream", _fake_lengths_stream(lengths_pq, captured)
    )
    with pytest.raises(ValueError, match="denovo_processing_idx"):
        asyncio.run(
            m.execute(
                m.Inputs(
                    reference_idx=7,
                    work_ticket_idx=42,
                    coverage_threshold=threshold,
                    genome_map_path=map_pq,
                    denovo_genome_map_path=denovo_map_pq,
                ),
                tmp_path / "ws",
            )
        )
