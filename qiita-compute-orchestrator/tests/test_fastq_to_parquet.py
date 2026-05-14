"""Isolated unit tests for `fastq_to_parquet.execute`.

Calls `execute()` directly (not through LocalBackend / run_native_job)
so failures here point at the conversion logic, not framework wiring.
The full-stack happy path lives in
`tests/integration/test_native_step_smoke.py`; this file covers
branches that path doesn't exercise:

  - FASTA input (no quality scores) writes NULL into the quality column.
  - Empty input writes an empty (header-only) Parquet rather than
    failing.
  - Missing input path raises FileNotFoundError (the framework
    dispatcher maps that to BackendFailure(BAD_INPUT) one layer up).

The happy-path case round-trips a small FASTQ to verify column shape
and duplicate-preservation under direct invocation; the smoke test
covers the same shape via the full stack.

All tests need the miint extension available — set
MIINT_EXTENSION_REPO if your host installs from the team mirror.
"""

from __future__ import annotations

import asyncio

import duckdb
import pytest

from qiita_compute_orchestrator.jobs.fastq_to_parquet import Inputs, execute


def _run(inputs: Inputs, workspace) -> dict:
    """Drive the coroutine synchronously so tests stay sync-styled.
    Mirrors the run_native_job → execute boundary without dragging in
    the dispatcher's BackendFailure wrapping."""
    return asyncio.run(execute(inputs, workspace))


def _read_parquet(path) -> list[tuple]:
    """Materialize reads.parquet into a list of tuples, ordered by
    read_id so assertions don't depend on row-group iteration order
    (the file is already sorted by read_id, but DuckDB scans in
    physical order)."""
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT read_id, sequence, quality, sequence_length"
            f" FROM read_parquet('{path}') ORDER BY read_id"
        ).fetchall()


def test_execute_writes_reads_parquet_for_fastq(tmp_path):
    """Happy path under direct invocation: a 3-read FASTQ with two
    identical sequences round-trips faithfully — both duplicates appear
    as separate rows (no dedup) and the qual1 column comes back as a
    phred-decoded UTINYINT[]."""
    fastq = tmp_path / "in.fastq"
    fastq.write_text(
        "@r1\nACGT\n+\n!!!!\n"  # quality "!" * 4 → phred [0, 0, 0, 0]
        "@r2\nTGCA\n+\n####\n"  # quality "#" * 4 → phred [2, 2, 2, 2]
        "@r3\nACGT\n+\n$$$$\n"  # duplicate of r1's sequence
    )

    outputs = _run(
        Inputs(fastq_path=fastq, sequenced_sample_idx=1, work_ticket_idx=1),
        tmp_path / "ws",
    )
    parquet = outputs["reads"]
    assert parquet.name == "reads.parquet"
    assert parquet.exists()

    rows = _read_parquet(parquet)
    assert len(rows) == 3
    # Duplicate sequences kept, both r1 and r3 appear.
    by_id = {r[0]: r for r in rows}
    assert by_id["r1"][1] == "ACGT"
    assert by_id["r3"][1] == "ACGT"
    # Quality is UTINYINT[] (phred-decoded), not the ASCII string.
    assert by_id["r1"][2] == [0, 0, 0, 0]
    assert by_id["r2"][2] == [2, 2, 2, 2]
    # sequence_length is BIGINT — fixture is uniformly 4 bp.
    assert {r[3] for r in rows} == {4}


def test_execute_handles_fasta_with_null_quality(tmp_path):
    """FASTA input has no quality line — the Parquet must write NULL
    into the quality column for every row. Confirms the FASTA branch
    on miint's read_fastx and that the output's quality column is
    nullable end-to-end."""
    fasta = tmp_path / "in.fasta"
    fasta.write_text(">r1\nACGT\n>r2\nTGCA\n")

    outputs = _run(
        Inputs(fastq_path=fasta, sequenced_sample_idx=1, work_ticket_idx=1),
        tmp_path / "ws",
    )
    parquet = outputs["reads"]

    rows = _read_parquet(parquet)
    assert len(rows) == 2
    # Every quality value is None — FASTA has no quality scores.
    assert all(r[2] is None for r in rows)
    # Sequences still round-trip.
    by_id = {r[0]: r for r in rows}
    assert by_id["r1"][1] == "ACGT"
    assert by_id["r2"][1] == "TGCA"


def test_execute_handles_empty_input(tmp_path):
    """An empty input file must produce an empty (header-only) Parquet
    rather than raising. The job pre-allocates the workspace and writes
    via DuckDB COPY; an empty `read_fastx(...)` yields zero rows and
    the COPY still emits a valid Parquet — assert both."""
    empty = tmp_path / "empty.fastq"
    empty.write_text("")

    outputs = _run(
        Inputs(fastq_path=empty, sequenced_sample_idx=1, work_ticket_idx=1),
        tmp_path / "ws",
    )
    parquet = outputs["reads"]
    assert parquet.exists()
    # Empty file produces zero rows but a valid Parquet — DuckDB can
    # still describe + count it.
    with duckdb.connect(":memory:") as conn:
        n = conn.execute(f"SELECT count(*) FROM read_parquet('{parquet}')").fetchone()[0]
        # Schema is still the four-column shape even with zero rows so
        # downstream consumers don't see a different schema for empty
        # samples.
        cols = [
            r[0]
            for r in conn.execute(
                f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{parquet}'))"
            ).fetchall()
        ]
    assert n == 0
    assert cols == ["read_id", "sequence", "quality", "sequence_length"]


def test_execute_raises_file_not_found(tmp_path):
    """A missing fastq_path raises FileNotFoundError from `execute`
    itself — the framework dispatcher (run_native_job) is responsible
    for mapping that to BackendFailure(BAD_INPUT) one layer up. Test
    the raw raise here so the dispatcher's mapping test stays
    independent."""
    missing = tmp_path / "does-not-exist.fastq"

    with pytest.raises(FileNotFoundError, match="FASTQ file not found"):
        _run(
            Inputs(fastq_path=missing, sequenced_sample_idx=1, work_ticket_idx=1),
            tmp_path / "ws",
        )
