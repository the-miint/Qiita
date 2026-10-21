"""Isolated unit tests for `ingest_ena_reads.execute` — the download-and-store
step of the download-ena-study workflow.

Calls `execute()` directly (not through LocalBackend) so failures point at the
ingest loop, not framework wiring. The `_stage_run_reads` seam (metadata
resolution + HTTP download) and mint_sequence_range / get_sequence_range are
monkeypatched throughout, so this suite never touches live ENA or a real CP.
"""

from __future__ import annotations

import asyncio
import json
import os

import duckdb
import pytest
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import FailureType, WorkTicketFailureStage

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
    """Write the `_stage_run_reads` intermediate shape `(sequence_index, read_id,
    sequence1, qual1, sequence2, qual2)`. An empty `rows` writes a schema-correct
    0-row Parquet (a bare `VALUES` clause with no rows is not valid SQL)."""
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


def _write_ena_run_map(path, roster: list[tuple[int, str]]) -> None:
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
    ena_run_map = tmp_path / "ena_run_map.parquet"
    _write_ena_run_map(ena_run_map, roster)
    return Inputs(
        ena_run_map=ena_run_map,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=5,
        sequencing_run_idx=3,
        work_ticket_idx=_WORK_TICKET_IDX,
    )


def _fake_stage_run_reads_factory(by_run: dict[str, tuple[list[tuple], list[str]]]):
    """Build a `_stage_run_reads`-shaped fake keyed by run_accession; each entry
    is `(rows, warnings)`."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        rows, warnings = by_run[run_accession]
        _write_intermediate(intermediate_path, rows)
        return len(rows), warnings

    return _fake


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_ingests_every_run_once_paired_and_single(fake_mint, monkeypatch, tmp_path):
    """Two runs (paired- and single-end) each mint once at the exact read count
    and land in both the durable staging copy and the hardlinked register part."""
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
    fan-out mints once per run and writes each durable copy + register part."""
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
    """Idempotent: a second run whose durable copy exists does NOT re-fetch or
    re-mint, but still re-creates its register part (the workspace is fresh)."""
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
    idempotency skip — only the atomically-published durable read.parquet does."""

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


def test_mid_stream_skip_warning_is_retriable(fake_mint, monkeypatch, tmp_path):
    """A skip warning (even alongside returned rows -- a mid-stream truncation)
    fails the step retriably, and the incomplete read set is never registered."""

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
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert exc.value.transient is True
    assert "ERR001" in exc.value.reason
    assert "re-run" in exc.value.reason
    assert fake_mint == []  # never reached the mint — failed before it


def test_skip_warning_without_transport_wording_is_retriable(fake_mint, monkeypatch, tmp_path):
    """Truncation inside a gzip member has no transient marker in its text, so the
    skip itself decides the kind."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        _write_intermediate(intermediate_path, [(1, "r1", "ACGT", None, None, None)])
        return 1, [
            "read_ena_sequences: WARNING: run 'ERR001' failed mid-stream after "
            "emitting 1 read(s) (unexpected end of file); skipping remainder"
        ]

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert fake_mint == []


def test_open_failure_skip_warning_is_retriable(fake_mint, monkeypatch, tmp_path):
    """A run that failed to open and was skipped (zero rows) reports the skip, not
    'zero reads', and is retriable."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        return 0, [
            "read_ena_sequences: warning: run 'ERR001' failed on retry (HTTP 404), skipping",
            "read_ena_sequences: WARNING: 1 run(s) skipped due to download errors: ERR001",
        ]

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert "zero reads" not in exc.value.reason
    assert "re-run" not in exc.value.reason
    assert fake_mint == []


def test_retrying_warning_alone_does_not_fail(fake_mint, monkeypatch, tmp_path):
    """A "retrying..." warning with NO "skip" substring means miint's internal
    retry self-healed — the run is complete and must NOT be treated as a failure."""

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


def test_md5_verification_skipped_warning_does_not_fail(fake_mint, monkeypatch, tmp_path):
    """miint's "md5 verification skipped" warning reports a complete run it could
    not verify, not missing data, so it must not trip the skip check."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        _write_intermediate(intermediate_path, [(1, "r1", "ACGT", None, None, None)])
        return 1, [
            "read_ena_sequences: WARNING: md5 verification skipped for run 'ERR001' — "
            "file 'https://ftp.sra.ebi.ac.uk/vol1/ERR001.fastq' is not gzip-compressed; "
            "md5 verification only supports the gzip transport basis"
        ]

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    _run(inputs, tmp_path / "ws")
    assert fake_mint == [(10, 1)]


def test_zero_reads_with_no_warning_is_permanent_bad_input(fake_mint, monkeypatch, tmp_path):
    """Unlike ingest_reads' empty-well case, an ENA run producing zero reads with
    NO explanatory warning is anomalous, not a legitimate empty result — fail
    loud rather than silently register nothing."""

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
    """A raised duckdb.Error whose message is transport/network-shaped classifies
    EXTERNAL_FETCH_TRANSIENT (retriable)."""

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
    """A raised duckdb.Error that is NOT network-shaped (a format/parse failure)
    classifies BAD_INPUT (permanent) — retry would fail the same way."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise duckdb.IOException("read_ena_sequences: malformed FASTQ record in run 'ERR001'")

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert exc.value.transient is False


def test_md5_mismatch_fetch_error_is_retriable(fake_mint, monkeypatch, tmp_path):
    """miint cannot tell a transfer truncated on a member boundary from a digest
    that really disagrees, so an md5 mismatch is retried; the reason tells the
    operator how to check the digest if it keeps failing."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise duckdb.IOException(
            "read_ena_sequences: md5 mismatch for 'ERR001 ftp://...': ENA reported "
            "abc123 but downloaded bytes hash to def456"
        )

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert exc.value.transient is True
    assert "fastq_md5" in exc.value.reason.lower()
    assert "ena portal api" in exc.value.reason.lower()
    assert "retried" not in exc.value.reason


def test_md5_error_with_transient_marker_still_classifies_transient(
    fake_mint, monkeypatch, tmp_path
):
    """Ordering regression: the transient-marker check runs BEFORE the md5
    branch, so an error mentioning both still classifies
    EXTERNAL_FETCH_TRANSIENT."""

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
    assert "fastq_md5" not in exc.value.reason.lower()


def _http_error(status: int, message: str) -> duckdb.HTTPException:
    """A duckdb.HTTPException as httpfs and miint's ENA client raise it: with the
    response's status in `status_code`, which a constructed one lacks."""
    error = duckdb.HTTPException(message)
    error.status_code = status
    return error


@pytest.mark.parametrize(
    "error",
    [
        # ENA's metadata API, after miint's own retries
        _http_error(
            500,
            "HTTP Error: ENA request failed with HTTP 500 (URL: "
            "https://www.ebi.ac.uk/ena/portal/api/search?result=read_run)",
        ),
        _http_error(
            429,
            "HTTP Error: ENA request failed with HTTP 429 (URL: "
            "https://www.ebi.ac.uk/ena/portal/api/search?result=read_run)",
        ),
        # the file server, through httpfs, when a run's file is opened
        _http_error(
            503,
            "HTTP Error: Request returned HTTP 503 for HTTP HEAD to "
            "'https://ftp.sra.ebi.ac.uk/vol1/fastq/ERR001/ERR001.fastq.gz'",
        ),
        _http_error(
            408,
            "HTTP Error: Request returned HTTP 408 for HTTP HEAD to "
            "'https://ftp.sra.ebi.ac.uk/vol1/fastq/ERR001/ERR001.fastq.gz'",
        ),
        # no usable HTTP status
        _http_error(
            0,
            "HTTP Error: HTTP GET error on "
            "'https://ftp.sra.ebi.ac.uk/vol1/fastq/ERR001/ERR001.fastq.gz' (HTTP 0)",
        ),
        duckdb.IOException(
            "IO Error: Could not connect to server error for HTTP HEAD to "
            "'https://ftp.sra.ebi.ac.uk/vol1/fastq/ERR001/ERR001.fastq.gz'"
        ),
        # ENA's "Not all results may have been written" answer, which miint
        # quotes from duckdb-miint#310 on, after its own retries
        duckdb.IOException(
            "IO Error: ENA request failed after 4 attempts: the last response was "
            'incomplete, ENA wrote "ERROR occurred. Not all results may have been '
            'written." in place of the search results (URL: '
            "https://www.ebi.ac.uk/ena/portal/api/search)"
        ),
    ],
)
def test_unavailable_server_fetch_error_is_retriable(fake_mint, monkeypatch, tmp_path, error):
    """ENA or its file server answering with a failure a later attempt can clear
    is retried, and reported as a transient fetch error."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise error

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert exc.value.transient is True
    assert ingest_module.TRANSIENT_FETCH_ERROR_TEXT in exc.value.reason


@pytest.mark.parametrize(
    "first_error",
    [
        "unexpected end of stream",
        # a network word in the transfer's own error must not make the run read
        # as ENA being unreachable
        "connection reset by peer",
    ],
)
def test_mid_stream_fetch_error_is_retried_as_partial_data(
    fake_mint, monkeypatch, tmp_path, first_error
):
    """A run that broke mid-download is retried, as it is when miint skips it,
    and its reason says the data is partial, not that ENA could not be reached."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise duckdb.IOException(
            "IO Error: read_ena_sequences: every run failed (1): ERR001; "
            f"first error: {first_error}"
        )

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert exc.value.transient is True
    assert ingest_module.TRANSIENT_FETCH_ERROR_TEXT not in exc.value.reason
    assert ingest_module._PARTIAL_RUN_TEXT in exc.value.reason


def test_not_found_fetch_error_is_permanent(fake_mint, monkeypatch, tmp_path):
    """A run whose file is missing is missing on every attempt, so a raised 404
    fails the ticket at once instead of using up its retries."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise _http_error(
            404,
            "HTTP Error: HTTP GET error on "
            "'https://ftp.sra.ebi.ac.uk/vol1/fastq/ERR001/ERR001.fastq.gz' "
            "(HTTP 404 Not Found)",
        )

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert exc.value.transient is False


# ---------------------------------------------------------------------------
# Several failed runs in one pool
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("permanent_first", [True, False])
def test_transient_failure_wins_over_permanent_in_any_roster_order(
    fake_mint, monkeypatch, tmp_path, permanent_first
):
    """A transient run decides the kind, so the ticket retries first whatever
    the roster order. The reason still names every failed run."""
    bad = "ERR001" if permanent_first else "ERR002"

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        if run_accession == bad:
            raise duckdb.InvalidInputException("read_ena_sequences: malformed FASTQ record")
        raise duckdb.IOException("read_ena_sequences: md5 mismatch for run: expected a got b")

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert exc.value.transient is True
    assert "ERR001" in exc.value.reason
    assert "ERR002" in exc.value.reason


def test_transient_failures_name_every_failed_run(fake_mint, monkeypatch, tmp_path):
    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise duckdb.IOException("read_ena_sequences: md5 mismatch for run: expected a got b")

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert "ERR001" in exc.value.reason
    assert "ERR002" in exc.value.reason


def test_many_failed_runs_cap_the_combined_reason(fake_mint, monkeypatch, tmp_path):
    """Dozens of failed runs must not produce a reason with one entry per run."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise duckdb.IOException("read_ena_sequences: md5 mismatch for run: expected a got b")

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    accessions = [f"ERR{i:03d}" for i in range(1, 41)]
    inputs = _inputs(tmp_path, [(i, acc) for i, acc in enumerate(accessions, start=10)])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    reason = exc.value.reason
    assert reason.startswith("40 runs failed: ")
    assert "ERR001" in reason and "ERR003" in reason
    assert "and 37 more failed runs: " in reason
    assert "ERR004" in reason and "ERR023" in reason
    assert "ERR024" not in reason and "ERR040" not in reason
    assert reason.endswith("…")


def test_deciding_transient_run_keeps_its_full_reason_in_a_capped_reason(
    fake_mint, monkeypatch, tmp_path
):
    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        if run_accession == "ERR009":
            raise duckdb.IOException("read_ena_sequences: md5 mismatch for run: expected a got b")
        raise duckdb.InvalidInputException("read_ena_sequences: malformed FASTQ record")

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    accessions = [f"ERR{i:03d}" for i in range(1, 11)]
    inputs = _inputs(tmp_path, [(i, acc) for i, acc in enumerate(accessions, start=10)])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT
    assert "ENA run ERR009: downloaded bytes don't match" in exc.value.reason
    assert "md5 mismatch" in exc.value.reason


def test_every_run_failed_permanently_raises_permanent(fake_mint, monkeypatch, tmp_path):
    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        raise duckdb.InvalidInputException("read_ena_sequences: malformed FASTQ record")

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert not (tmp_path / "ws" / ingest_module.RUN_OUTCOMES_FILENAME).exists()


# ---------------------------------------------------------------------------
# A pool with some permanently failed runs lands the rest
# ---------------------------------------------------------------------------

_GOOD_ROWS = [(1, "r1", "ACGT", None, None, None)]


def _read_run_outcomes(outputs) -> list[dict]:
    return json.loads(outputs["run_outcomes"].read_text())


def _fake_with_failing_run(bad_run: str, failure):
    """A `_stage_run_reads` fake where `bad_run` fails as `failure` (a raised
    exception, or a `(count, warnings)` return) and every other run is good."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        if run_accession != bad_run:
            _write_intermediate(intermediate_path, _GOOD_ROWS)
            return 1, []
        if isinstance(failure, BaseException):
            raise failure
        _write_intermediate(intermediate_path, [])
        return failure

    return _fake


@pytest.mark.parametrize(
    "failure",
    [
        duckdb.IOException("read_ena_sequences: malformed FASTQ record in run 'ERR001'"),
        duckdb.IOException(
            "IO Error: read_ena_sequences: no sequence data found for the provided accession(s)"
        ),
        _http_error(
            404, "HTTP Error: HTTP GET error on 'https://ftp.example/x.fastq.gz' (HTTP 404)"
        ),
        (0, []),
    ],
    ids=["format-error", "no-sequence-data", "http-404", "zero-reads"],
)
def test_permanently_failed_run_does_not_block_the_good_runs(
    fake_mint, monkeypatch, tmp_path, failure
):
    """The good run is minted, stored and registered; the permanent one is only
    listed in `run_outcomes`, with no mint and no register part."""
    monkeypatch.setattr(
        ingest_module, "_stage_run_reads", _fake_with_failing_run("ERR001", failure)
    )
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002")])

    outputs = _run(inputs, tmp_path / "ws")

    assert fake_mint == [(11, 1)]
    register_dir = outputs["read_staging_dir"] / "read"
    assert [p.name for p in register_dir.iterdir()] == ["11.parquet"]
    (failed,) = _read_run_outcomes(outputs)
    assert failed["prep_sample_idx"] == 10
    assert failed["ena_run_accession"] == "ERR001"
    assert failed["failure_type"] == FailureType.PERMANENT.value
    assert "ERR001" in failed["failure_reason"]


@pytest.mark.parametrize(
    "failure",
    [
        duckdb.OutOfMemoryException(
            "Out of Memory Error: failed to allocate data of size 16.0 MiB (1.0 GiB/1.0 GiB used)"
        ),
        duckdb.IOException("IO Error: No space left on device writing the intermediate Parquet"),
        _http_error(403, "HTTP Error: HTTP GET error on 'https://ftp.example/x' (HTTP 403)"),
    ],
    ids=["oom", "local-io", "http-403"],
)
def test_unrecognised_error_fails_the_step_beside_a_good_run(
    fake_mint, monkeypatch, tmp_path, failure
):
    """The raw error propagates, not a BackendFailure, so a Slurm OOM traceback is
    still upgraded to OOM_KILLED."""
    monkeypatch.setattr(
        ingest_module, "_stage_run_reads", _fake_with_failing_run("ERR001", failure)
    )
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002")])

    with pytest.raises(type(failure)) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value is failure
    assert not (tmp_path / "ws" / ingest_module.RUN_OUTCOMES_FILENAME).exists()


def test_transient_failure_wins_over_an_unrecognised_error(fake_mint, monkeypatch, tmp_path):
    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        if run_accession == "ERR001":
            raise duckdb.OutOfMemoryException("Out of Memory Error: failed to allocate data")
        if run_accession == "ERR002":
            raise duckdb.IOException("read_ena_sequences: md5 mismatch for run: expected a got b")
        _write_intermediate(intermediate_path, _GOOD_ROWS)
        return 1, []

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002"), (12, "ERR003")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT


def test_run_outcomes_is_a_top_level_json_file(fake_mint, monkeypatch, tmp_path):
    """register-files would load a top-level `.parquet` as its own table, so the
    outcome list must not be one."""
    monkeypatch.setattr(
        ingest_module,
        "_stage_run_reads",
        _fake_with_failing_run("ERR001", duckdb.IOException("malformed FASTQ record")),
    )
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002")])

    outputs = _run(inputs, tmp_path / "ws")

    assert (
        outputs["run_outcomes"] == outputs["read_staging_dir"] / ingest_module.RUN_OUTCOMES_FILENAME
    )
    assert not list(outputs["read_staging_dir"].glob("*.parquet"))


def test_run_outcomes_is_empty_when_every_run_is_stored(fake_mint, monkeypatch, tmp_path):
    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake_with_failing_run("none", (0, [])))
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002")])

    outputs = _run(inputs, tmp_path / "ws")

    assert _read_run_outcomes(outputs) == []


def test_transient_failure_beside_a_stored_run_retries_the_step(fake_mint, monkeypatch, tmp_path):
    """A stored run does not let a transient failure land: the step fails so the
    ticket retries, and no outcome file is written."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        if run_accession == "ERR001":
            raise duckdb.IOException("read_ena_sequences: md5 mismatch for run: expected a got b")
        _write_intermediate(intermediate_path, _GOOD_ROWS)
        return 1, []

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.transient is True
    assert not (tmp_path / "ws" / ingest_module.RUN_OUTCOMES_FILENAME).exists()


def test_transient_and_permanent_failures_beside_a_stored_run_retry_first(
    fake_mint, monkeypatch, tmp_path
):
    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        if run_accession == "ERR001":
            raise duckdb.InvalidInputException("read_ena_sequences: malformed FASTQ record")
        if run_accession == "ERR002":
            raise duckdb.IOException("read_ena_sequences: md5 mismatch for run: expected a got b")
        _write_intermediate(intermediate_path, _GOOD_ROWS)
        return 1, []

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002"), (12, "ERR003")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT


def test_permanent_mint_failure_fails_the_step_beside_a_good_run(monkeypatch, tmp_path):
    """Only fetch failures are landable. A permanent failure in the mint is a
    fault in the ticket, not a bad run, so it fails the whole step."""

    def _fake(run_accession, download_method, intermediate_path, duckdb_tmp, memory_gb, threads):
        _write_intermediate(intermediate_path, _GOOD_ROWS)
        return 1, []

    monkeypatch.setattr(ingest_module, "_stage_run_reads", _fake)

    async def _mint(http, prep_sample_idx, count, *, work_ticket_idx, step_name):
        if prep_sample_idx == 10:
            raise BackendFailure(
                kind=FailureKind.UNKNOWN_PERMANENT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason="mint refused",
            )
        return 1

    monkeypatch.setattr(ingest_module, "mint_or_reuse_sequence_range", _mint)
    inputs = _inputs(tmp_path, [(10, "ERR001"), (11, "ERR002")])

    with pytest.raises(BackendFailure, match="mint refused"):
        _run(inputs, tmp_path / "ws")
    assert not (tmp_path / "ws" / ingest_module.RUN_OUTCOMES_FILENAME).exists()


# ---------------------------------------------------------------------------
# Range reuse — wiring only (see test_ingest_reads.py for the full matrix)
# ---------------------------------------------------------------------------


def test_reuses_existing_range_on_mint_conflict(monkeypatch, tmp_path):
    """Durable absent + mint 409s ⇒ read the existing range back and reuse its
    start, proving `mint_or_reuse_sequence_range` is wired into this job."""

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
    """An empty ena_run_map.parquet is a resolver/dispatch bug, not a legitimate
    empty ticket — ValueError -> BAD_INPUT via the framework dispatcher."""
    ena_run_map = tmp_path / "ena_run_map.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES (1, 'x')) "
            "AS t(prep_sample_idx, ena_run_accession) WHERE false) "
            f"TO '{ena_run_map}' (FORMAT parquet)"
        )
    inputs = Inputs(
        ena_run_map=ena_run_map,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=5,
        sequencing_run_idx=3,
        work_ticket_idx=_WORK_TICKET_IDX,
    )
    with pytest.raises(ValueError, match="ena_run_map is empty"):
        _run(inputs, tmp_path / "ws")


@pytest.mark.skipif(
    os.environ.get("QIITA_ENA_LIVE_SMOKE") != "1",
    reason="hits live ENA + real miint; opt in with QIITA_ENA_LIVE_SMOKE=1",
)
def test_ingests_a_real_public_ena_run_live_smoke(fake_mint, tmp_path):
    """Live smoke: a real, small public ENA run downloads end-to-end through the
    UNMOCKED `_stage_run_reads` seam (real miint + real network). Opt-in via
    `QIITA_ENA_LIVE_SMOKE=1` because this component's Makefile runs bare `pytest`
    with no `-m` deselection, so an unconditional `@pytest.mark.system` would NOT
    be excluded by `make test`.

    Accession DRR037815: SINGLE-layout ILLUMINA, fastq_bytes=1774 (~1.7 KB
    gzipped, about as small as a real public Illumina run gets), public since
    2016. DDBJ-submitted (DRR prefix), but ENA mirrors and serves it exactly like
    a native ERR/SRR run."""
    inputs = _inputs(tmp_path, [(10, "DRR037815")])
    outputs = _run(inputs, tmp_path / "ws")
    assert fake_mint and fake_mint[0][0] == 10
    assert (outputs["read_staging_dir"] / "read" / "10.parquet").exists()
