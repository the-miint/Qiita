"""Isolated unit tests for `ingest_ena_reads.execute` — the download-and-store
step of the download-ena-study workflow.

Calls `execute()` directly (not through LocalBackend) so failures point at the
ingest loop, not framework wiring. `_stage_run_reads` (the whole
`read_ena_sequences` seam — metadata resolution + the actual HTTP download)
is MONKEYPATCHED throughout: this suite never touches live ENA and needs no
real miint extension. Covers:

  - Happy path: paired- and single-end runs both parse into the durable
    per-run `read.parquet` (staged copy) AND the register part
    (hardlinked, same inode), with one mint per run at the exact read count.
  - Idempotent re-run: a run whose durable copy already exists is skipped (no
    re-mint) but its register part is re-linked; a stale `.partial` sentinel
    from a crashed prior attempt does NOT satisfy the skip.
  - Fail-loud fetch-warning check: a `miint_warnings()` message containing
    "skip" (a genuine ENA-side skip/truncation) fails the run permanently
    (BAD_INPUT), even when rows were returned; a clean zero-row result with
    NO explanatory warning also fails permanently — unlike `ingest_reads`,
    there is no legitimate "empty well" reading for an ENA run.
  - Raised `duckdb.Error` classification: a transport/network-shaped message
    classifies EXTERNAL_FETCH_TRANSIENT (retriable); an md5-verification
    failure (miint's default-on download integrity check) classifies
    BAD_INPUT (permanent) unless it also carries a transient marker
    (ordering); anything else classifies BAD_INPUT (permanent).
  - Range reuse wiring: a mint 409 reads back and reuses an in-flight
    ticket's existing range (the shared `mint_or_reuse_sequence_range` path,
    exercised here only to prove the wiring, not re-testing its full matrix —
    see `test_ingest_reads.py` for that).

mint_sequence_range / get_sequence_range are monkey-patched so no live CP is
needed, mirroring test_ingest_reads.py.
"""

from __future__ import annotations

import asyncio
import os

import duckdb
import pytest
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind

import qiita_compute_orchestrator.jobs.ingest_ena_reads as ingest_module
import qiita_compute_orchestrator.sequence_range_retry as retry_module
from qiita_compute_orchestrator.jobs.ingest_ena_reads import Inputs, execute
from qiita_compute_orchestrator.sequence_range import (
    MintedSequenceRange,
    SequenceRangeAlreadyExists,
)

_WORK_TICKET_IDX = 1


def _run(inputs: Inputs, workspace) -> dict:
    return asyncio.run(execute(inputs, workspace))


@pytest.fixture
def fake_mint(monkeypatch):
    """Replace mint_sequence_range with a recorder. Returns the list of
    (prep_sample_idx, count) calls; each mint starts at a per-sample base so
    the written sequence_idx values are visible and distinct across runs."""
    calls: list[tuple[int, int]] = []

    async def _fake(*, http, prep_sample_idx, count, work_ticket_idx):
        calls.append((prep_sample_idx, count))
        base = 1000 * prep_sample_idx
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=base,
            sequence_idx_stop=base + count - 1,
        )

    monkeypatch.setattr(retry_module, "mint_sequence_range", _fake)
    return calls


def _write_intermediate(
    path,
    rows: list[tuple[int, str, str, list[int] | None, str | None, list[int] | None]],
) -> None:
    """Write the `_stage_run_reads` intermediate shape: `(sequence_index,
    read_id, sequence1, qual1, sequence2, qual2)` — miint's 1-based per-run
    row index, no run/sample/experiment accession columns (dropped by the
    explicit projection). `rows` are (sequence_index, read_id, sequence1,
    qual1, sequence2, qual2); qual1/qual2 are `list[int] | None`. An empty
    `rows` writes a schema-correct 0-row Parquet (a bare `VALUES` clause with
    no rows is not valid SQL)."""
    with duckdb.connect(":memory:") as conn:
        if not rows:
            conn.execute(
                "COPY (SELECT CAST(NULL AS BIGINT) AS sequence_index,"
                " CAST(NULL AS VARCHAR) AS read_id, CAST(NULL AS VARCHAR) AS sequence1,"
                " CAST(NULL AS UTINYINT[]) AS qual1, CAST(NULL AS VARCHAR) AS sequence2,"
                " CAST(NULL AS UTINYINT[]) AS qual2 WHERE false) "
                f"TO '{path}' (FORMAT PARQUET)"
            )
            return
        values = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
            "CAST(? AS UTINYINT[]), CAST(? AS VARCHAR), CAST(? AS UTINYINT[]))"
            for _ in rows
        )
        params: list = []
        for sidx, rid, s1, q1, s2, q2 in rows:
            params.extend([sidx, rid, s1, q1, s2, q2])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) "
            "AS t(sequence_index, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )


def _write_run_map(path, roster: list[tuple[int, str]]) -> None:
    """Write the `(prep_sample_idx, ena_run_accession)` roster Parquet the
    runner materializes for the step."""
    rows = ", ".join(f"({idx}, '{acc}')" for idx, acc in roster)
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES " + rows + ") AS t(prep_sample_idx, ena_run_accession)) "
            f"TO '{path}' (FORMAT parquet)"
        )


def _durable_rows(staging_root, prep_sample_idx) -> list[tuple]:
    path = compute_reads_staging_path(staging_root, prep_sample_idx)
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT prep_sample_idx, sequence_idx, sequence1 "
            f"FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


def _inputs(tmp_path, roster: list[tuple[int, str]]) -> Inputs:
    run_map = tmp_path / "run_map.parquet"
    _write_run_map(run_map, roster)
    return Inputs(
        run_map=run_map,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=5,
        sequencing_run_idx=3,
        work_ticket_idx=_WORK_TICKET_IDX,
    )


def _fake_stage_run_reads_factory(by_run: dict[str, tuple[list[tuple], list[str]]]):
    """Build a `_stage_run_reads`-shaped fake keyed by run_accession. Each
    entry is `(rows, warnings)`; `rows` are the `_write_intermediate` row
    tuples. Ignores `download_method` / duckdb_tmp / memory_gb / threads —
    the seam under test only cares about accession -> (count, warnings)."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        rows, warnings = by_run[run_accession]
        _write_intermediate(intermediate_path, rows)
        return len(rows), warnings

    return _fake


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_ingests_every_run_once_paired_and_single(fake_mint, monkeypatch, tmp_path):
    """Two runs — one paired-end, one single-end — each mint once at the exact
    read count and land in both the durable staging copy and the hardlinked
    register part."""
    fake = _fake_stage_run_reads_factory(
        {
            "ERR001": (
                [
                    (1, "r1", "ACGT", [40, 40, 40, 40], "TTTT", [40, 40, 40, 40]),
                    (2, "r2", "GGGG", [40, 40, 40, 40], "CCCC", [40, 40, 40, 40]),
                ],
                [],
            ),
            "ERR002": (
                [(1, "s1", "AAAA", [40, 40, 40, 40], None, None)],
                [],
            ),
        }
    )
    monkeypatch.setattr(ingest_module, "_stage_run_reads", fake)
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002")])

    outputs = _run(inputs, tmp_path / "ws")

    assert sorted(fake_mint) == [(10, 2), (11, 1)]
    assert _durable_rows(inputs.reads_staging_root, 10) == [
        (10, 10000, "ACGT"),
        (10, 10001, "GGGG"),
    ]
    assert _durable_rows(inputs.reads_staging_root, 11) == [(11, 11000, "AAAA")]
    register_dir = outputs["read_staging_dir"] / "read"
    for idx in (10, 11):
        part = register_dir / f"{idx}.parquet"
        durable = compute_reads_staging_path(inputs.reads_staging_root, idx)
        assert part.exists() and part.stat().st_ino == durable.stat().st_ino


def test_concurrent_pool_ingests_every_run_above_cap(fake_mint, monkeypatch, tmp_path):
    """A pool larger than the concurrency cap ingests every run: the bounded
    asyncio.gather fan-out mints once per run (with the exact count) and
    writes each durable copy + register part."""
    n = ingest_module._CONCURRENCY + 2
    by_run = {
        f"ERR{i:03d}": ([(j + 1, f"r{i}_{j}", "ACGT", None, None, None) for j in range(i)], [])
        for i in range(1, n + 1)
    }
    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake_stage_run_reads_factory(by_run))
    inputs = _inputs(tmp_path, [(i, f"ERR{i:03d}") for i in range(1, n + 1)])

    outputs = _run(inputs, tmp_path / "ws")

    assert sorted(fake_mint) == [(i, i) for i in range(1, n + 1)]
    register_dir = outputs["read_staging_dir"] / "read"
    for i in range(1, n + 1):
        assert compute_reads_staging_path(inputs.reads_staging_root, i).exists()
        assert (register_dir / f"{i}.parquet").exists()


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_rerun_skips_already_ingested(fake_mint, monkeypatch, tmp_path):
    """Idempotent: a second run over a run whose durable copy exists does NOT
    re-fetch or re-mint, but still re-creates its register part (the
    workspace is fresh)."""
    calls: list[str] = []

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        calls.append(run_accession)
        _write_intermediate(intermediate_path, [(1, "r1", "ACGT", None, None, None)])
        return 1, []

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    _run(inputs, tmp_path / "ws1")
    assert calls == ["ERR001"]
    assert fake_mint == [(10, 1)]

    outputs = _run(inputs, tmp_path / "ws2")
    # No second fetch or mint — the durable copy already exists.
    assert calls == ["ERR001"]
    assert fake_mint == [(10, 1)]
    assert (outputs["read_staging_dir"] / "read" / "10.parquet").exists()


def test_stale_partial_does_not_count_as_ingested(fake_mint, monkeypatch, tmp_path):
    """A `.partial` left by a crashed prior attempt must NOT satisfy the
    idempotency skip — only the atomically-published durable read.parquet
    does."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        _write_intermediate(intermediate_path, [(1, "r1", "ACGT", None, None, None)])
        return 1, []

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])
    durable = compute_reads_staging_path(inputs.reads_staging_root, 10)
    durable.parent.mkdir(parents=True)
    (durable.parent / f"{durable.name}.partial").write_text("truncated")

    _run(inputs, tmp_path / "ws")

    assert fake_mint == [(10, 1)]
    assert durable.exists()
    assert not (durable.parent / f"{durable.name}.partial").exists()


# ---------------------------------------------------------------------------
# Fail-loud on a silent skip / partial download
# ---------------------------------------------------------------------------


def test_skip_warning_is_permanent_bad_input(fake_mint, monkeypatch, tmp_path):
    """A miint_warnings() entry mentioning a skip (even alongside returned
    rows — a mid-stream truncation) fails the run permanently: the data is
    incomplete and must never be silently registered."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        _write_intermediate(intermediate_path, [(1, "r1", "ACGT", None, None, None)])
        return 1, [
            "read_ena_sequences: WARNING: run 'ERR001' failed mid-stream after "
            "emitting 1 read(s) (connection reset); skipping remainder — "
            "downstream sees partial data for this run"
        ]

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert exc.value.transient is False
    assert "ERR001" in exc.value.reason
    assert fake_mint == []  # never reached the mint — failed before it


def test_retrying_warning_alone_does_not_fail(fake_mint, monkeypatch, tmp_path):
    """A "... retrying..." warning with NO "skip" substring means miint's
    internal retry self-healed — the run is complete and must NOT be treated
    as a failure."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        _write_intermediate(intermediate_path, [(1, "r1", "ACGT", None, None, None)])
        return 1, [
            "read_ena_sequences: warning: run 'ERR001' failed to open "
            "(connection reset), retrying..."
        ]

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    _run(inputs, tmp_path / "ws")
    assert fake_mint == [(10, 1)]


def test_zero_reads_with_no_warning_is_permanent_bad_input(fake_mint, monkeypatch, tmp_path):
    """Unlike ingest_reads' empty-well case, an ENA run producing zero reads
    with NO explanatory miint_warnings() entry is anomalous, not a legitimate
    empty result — fail loud rather than silently register nothing."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        _write_intermediate(intermediate_path, [])
        return 0, []

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "zero reads" in exc.value.reason
    assert fake_mint == []


# ---------------------------------------------------------------------------
# Raised duckdb.Error classification
# ---------------------------------------------------------------------------


def test_transient_fetch_error_is_retriable(fake_mint, monkeypatch, tmp_path):
    """A raised duckdb.Error whose message is transport/network-shaped
    classifies EXTERNAL_FETCH_TRANSIENT (retriable) — miint's own internal
    retry never ran (e.g. metadata resolution failed outright)."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise duckdb.IOException(
            "read_ena_sequences: Connection timed out while resolving metadata"
        )

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert exc.value.transient is True
    assert "ERR001" in exc.value.reason


def test_format_fetch_error_is_permanent(fake_mint, monkeypatch, tmp_path):
    """A raised duckdb.Error whose message is NOT network-shaped (a
    format/parse failure) classifies BAD_INPUT (permanent) — the same input
    would fail the same way on retry."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise duckdb.IOException("read_ena_sequences: malformed FASTQ record in run 'ERR001'")

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert exc.value.transient is False


def test_md5_mismatch_fetch_error_is_permanent(fake_mint, monkeypatch, tmp_path):
    """A raised duckdb.Error shaped like miint's md5-verification failure
    (contains "md5", no transient marker) classifies BAD_INPUT (permanent) —
    a corrupted download fails identically on retry, so it must not be
    misclassified as a transient network blip."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise duckdb.IOException(
            "read_ena_sequences: md5 mismatch for 'ERR001 ftp://...': ENA reported "
            "abc123 but downloaded bytes hash to def456"
        )

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert exc.value.transient is False
    assert "md5" in exc.value.reason.lower()


def test_md5_error_with_transient_marker_still_classifies_transient(
    fake_mint, monkeypatch, tmp_path
):
    """Ordering regression: the transient-marker check runs BEFORE the md5
    branch, so a (hypothetical) error mentioning both md5 and a transient
    marker still classifies EXTERNAL_FETCH_TRANSIENT — a network blip that
    happened to occur during md5-tap streaming is still retriable."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise duckdb.IOException(
            "read_ena_sequences: md5 verification aborted: connection reset by peer"
        )

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert exc.value.transient is True


# ---------------------------------------------------------------------------
# Range reuse — wiring only (see test_ingest_reads.py for the full matrix)
# ---------------------------------------------------------------------------


def test_reuses_existing_range_on_mint_conflict(monkeypatch, tmp_path):
    """Durable absent + mint 409s ⇒ read the existing range back and reuse
    its start, proving `mint_or_reuse_sequence_range` is wired into this job
    exactly as it is into ingest_reads."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        _write_intermediate(
            intermediate_path,
            [
                (1, "r1", "ACGT", None, None, None),
                (2, "r2", "TTTT", None, None, None),
            ],
        )
        return 2, []

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)

    async def _conflict(*, http, prep_sample_idx, count, work_ticket_idx):
        raise SequenceRangeAlreadyExists(prep_sample_idx, count)

    monkeypatch.setattr(retry_module, "mint_sequence_range", _conflict)

    async def _existing(*, http, prep_sample_idx):
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=5000,
            sequence_idx_stop=5001,
            minted_by_work_ticket_idx=_WORK_TICKET_IDX,
            minted_by_work_ticket_state="processing",
        )

    monkeypatch.setattr(retry_module, "get_sequence_range", _existing)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    _run(inputs, tmp_path / "ws")

    assert _durable_rows(inputs.reads_staging_root, 10) == [
        (10, 5000, "ACGT"),
        (10, 5001, "TTTT"),
    ]


# ---------------------------------------------------------------------------
# Empty / unreadable roster
# ---------------------------------------------------------------------------


def test_empty_run_map_raises_value_error(tmp_path):
    """An empty run_map.parquet is a resolver/dispatch bug (the CP's
    _stage_ena_run_roster already fails loud on an empty pool before staging
    this file), not a legitimate empty ticket — ValueError -> BAD_INPUT via
    the framework dispatcher."""
    run_map = tmp_path / "run_map.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES (1, 'x')) "
            "AS t(prep_sample_idx, ena_run_accession) WHERE false) "
            f"TO '{run_map}' (FORMAT parquet)"
        )
    inputs = Inputs(
        run_map=run_map,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=5,
        sequencing_run_idx=3,
        work_ticket_idx=_WORK_TICKET_IDX,
    )
    with pytest.raises(ValueError, match="run_map is empty"):
        _run(inputs, tmp_path / "ws")


@pytest.mark.skipif(
    os.environ.get("QIITA_ENA_LIVE_SMOKE") != "1",
    reason="hits live ENA + real miint; opt in with QIITA_ENA_LIVE_SMOKE=1",
)
def test_ingests_a_real_public_ena_run_live_smoke(fake_mint, tmp_path):
    """Live smoke: a real, small public ENA run downloads end-to-end through
    the UNMOCKED `_stage_run_reads` seam (real miint + real network). Opt-in
    via `QIITA_ENA_LIVE_SMOKE=1` (mirrors `test_assembly_hash.py`'s
    `QIITA_ASSEMBLY_STRESS` gate) so plain `make test` / `uv run pytest` never
    depends on network access — this component's Makefile target runs bare
    `pytest` with no `-m` deselection, unlike qiita-control-plane's `-m 'not
    db'` / the integration suite's `-m 'not system'`, so an unconditional
    `@pytest.mark.system` here would NOT be excluded by `make test`.

    Accession: DRR037815 -- verified via the ENA Portal API
    (`filereport?accession=DRR037815&result=read_run&fields=run_accession,
    library_layout,fastq_bytes,instrument_platform,instrument_model,
    fastq_ftp,first_public`) to be SINGLE-layout, `instrument_platform=
    ILLUMINA` (MiSeq), `fastq_bytes=1774` (~1.7 KB gzipped -- about as small
    as a real public Illumina run gets), `first_public=2016-04-19` (public
    for a decade, so not a to-be-embargoed/withdrawn upload), with a live
    `fastq_ftp` (confirmed reachable: an HTTPS HEAD against
    `ftp.sra.ebi.ac.uk/vol1/fastq/DRR037/DRR037815/DRR037815.fastq.gz`
    returned 200 with `Content-Length: 1774`, matching fastq_bytes exactly).
    A DDBJ-submitted run (DRR prefix), but ENA mirrors and serves it exactly
    like a native ERR/SRR run through the same Portal API and FTP layout
    `read_ena_sequences` reads -- there is no smaller/more-stable
    ENA-servable Illumina run readily discoverable via the Portal
    `search` endpoint (a `base_count<200000` sweep surfaced only DRR-prefixed
    hits in this size class)."""
    inputs = _inputs(tmp_path, [(10, "DRR037815")])
    outputs = _run(inputs, tmp_path / "ws")
    assert fake_mint and fake_mint[0][0] == 10
    assert (outputs["read_staging_dir"] / "read" / "10.parquet").exists()
