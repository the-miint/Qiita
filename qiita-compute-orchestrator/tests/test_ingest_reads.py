"""Isolated unit tests for `ingest_reads.execute` — the pool-level read
storage step of the bcl-convert workflow.

Calls `execute()` directly (not through LocalBackend) so failures point at the
ingest loop, not framework wiring. Covers the branches the split introduced:

  - Happy path: each pool sample's FASTQ is parsed once, a range minted, and the
    full reads written to BOTH the durable staged copy
    (compute_reads_staging_path) and the register part (read/<idx>.parquet,
    hardlinked to the durable copy).
  - Empty well: a zero-record FASTQ is skipped (no mint, no reads), not an error.
  - Missing required R1: collected and the step fails BAD_INPUT.
  - Idempotent re-run: a sample whose durable copy already exists is skipped (no
    re-mint) but its register part is re-linked.
  - All-empty pool: StepNoData (the whole ticket is no-data).

mint_sequence_range is monkey-patched so no live CP is needed. miint must be
available (set MIINT_EXTENSION_REPO for the team mirror).
"""

from __future__ import annotations

import asyncio
import gzip

import duckdb
import pytest
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData

import qiita_compute_orchestrator.jobs.ingest_reads as ingest_module
from qiita_compute_orchestrator.jobs.ingest_reads import Inputs, execute
from qiita_compute_orchestrator.sequence_range import MintedSequenceRange


def _run(inputs: Inputs, workspace) -> dict:
    return asyncio.run(execute(inputs, workspace))


@pytest.fixture
def fake_mint(monkeypatch):
    """Replace mint_sequence_range with a recorder. Returns the list of
    (prep_sample_idx, count) calls; each mint starts at a per-sample base so the
    written sequence_idx values are visible and distinct across samples."""
    calls: list[tuple[int, int]] = []

    async def _fake(*, http, prep_sample_idx, count):
        calls.append((prep_sample_idx, count))
        base = 1000 * prep_sample_idx
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=base,
            sequence_idx_stop=base + count - 1,
        )

    monkeypatch.setattr(ingest_module, "mint_sequence_range", _fake)
    return calls


def _write_fastq_gz(path, records: list[tuple[str, str]]) -> None:
    """Write a gzipped FASTQ with the given (read_id, sequence) records (constant
    quality). An empty list writes a valid-but-empty .gz (an empty well)."""
    body = "".join(f"@{rid}\n{seq}\n+\n{'I' * len(seq)}\n" for rid, seq in records)
    path.write_bytes(gzip.compress(body.encode()))


def _seed_convert_dir(tmp_path, samples: dict[str, list[tuple[str, str]]]):
    """Lay out a bcl-convert ConvertJob dir: one R1 .fastq.gz per pool_item_id
    nested under a Sample_Project subdir (mirrors --bcl-sampleproject-subdirectories)."""
    convert_dir = tmp_path / "ConvertJob"
    proj = convert_dir / "MyProject"
    proj.mkdir(parents=True)
    for item_id, records in samples.items():
        _write_fastq_gz(proj / f"{item_id}_S1_L001_R1_001.fastq.gz", records)
    return convert_dir


def _write_sample_map(path, roster: list[tuple[int, str]]) -> None:
    """Write the `(prep_sample_idx, pool_item_id)` roster Parquet the runner
    materializes for the step."""
    rows = ", ".join(f"({idx}, '{item}')" for idx, item in roster)
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES " + rows + ") AS t(prep_sample_idx, pool_item_id)) "
            f"TO '{path}' (FORMAT parquet)"
        )


def _durable_rows(staging_root, prep_sample_idx) -> list[tuple]:
    path = compute_reads_staging_path(staging_root, prep_sample_idx)
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT prep_sample_idx, sequence_idx, sequence1 "
            f"FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


def _inputs(tmp_path, convert_dir, roster) -> Inputs:
    sample_map = tmp_path / "sample_map.parquet"
    _write_sample_map(sample_map, roster)
    return Inputs(
        convert_dir=convert_dir,
        sample_map=sample_map,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=5,
        sequencing_run_idx=3,
        work_ticket_idx=1,
    )


def test_ingests_every_sample_once(fake_mint, tmp_path):
    """Two samples → two mints, durable copies under compute_reads_staging_path,
    and register parts hardlinked to them (same inode)."""
    convert_dir = _seed_convert_dir(
        tmp_path, {"10": [("a", "ACGT"), ("b", "TTTT")], "11": [("c", "GGGG")]}
    )
    inputs = _inputs(tmp_path, convert_dir, [(10, "10"), (11, "11")])

    outputs = _run(inputs, tmp_path / "ws")

    # One mint per sample, with the exact read count.
    assert sorted(fake_mint) == [(10, 2), (11, 1)]
    # Durable copies carry the scope column and the minted sequence_idx range.
    assert _durable_rows(inputs.reads_staging_root, 10) == [
        (10, 10000, "ACGT"),
        (10, 10001, "TTTT"),
    ]
    assert _durable_rows(inputs.reads_staging_root, 11) == [(11, 11000, "GGGG")]
    # register part hardlinked to the durable copy (same inode).
    register_dir = outputs["read_staging_dir"] / "read"
    for idx in (10, 11):
        part = register_dir / f"{idx}.parquet"
        durable = compute_reads_staging_path(inputs.reads_staging_root, idx)
        assert part.exists() and part.stat().st_ino == durable.stat().st_ino


def test_empty_well_is_skipped(fake_mint, tmp_path):
    """A zero-record FASTQ is an empty well: no mint, no reads — but the pool
    still succeeds via its non-empty samples."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")], "11": []})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10"), (11, "11")])

    outputs = _run(inputs, tmp_path / "ws")

    assert fake_mint == [(10, 1)]  # only the non-empty well minted
    assert compute_reads_staging_path(inputs.reads_staging_root, 10).exists()
    assert not compute_reads_staging_path(inputs.reads_staging_root, 11).exists()
    assert not (outputs["read_staging_dir"] / "read" / "11.parquet").exists()


def test_missing_required_r1_fails_bad_input(fake_mint, tmp_path):
    """A roster sample with no R1 FASTQ on disk is a broken pool: BAD_INPUT,
    naming the offending sample."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10"), (99, "99")])

    with pytest.raises(BackendFailure) as exc:
        _run(inputs, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "99" in str(exc.value)


def test_rerun_skips_already_ingested(fake_mint, tmp_path):
    """Idempotent: a second run over a sample whose durable copy exists does NOT
    re-mint, but still re-creates its register part (the workspace is fresh)."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])

    _run(inputs, tmp_path / "ws1")
    assert fake_mint == [(10, 1)]

    outputs = _run(inputs, tmp_path / "ws2")
    # No second mint — the durable copy already exists.
    assert fake_mint == [(10, 1)]
    # The fresh workspace still gets the register part (re-linked from durable).
    assert (outputs["read_staging_dir"] / "read" / "10.parquet").exists()


def test_stale_partial_does_not_count_as_ingested(fake_mint, tmp_path):
    """A `.partial` left by a crashed prior attempt must NOT satisfy the
    idempotency skip — only the atomically-published durable read.parquet does.
    Otherwise a truncated write would be registered as the full read set."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [("a", "ACGT")]})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10")])
    # Simulate a crash mid-COPY: a partial sentinel exists, the durable does not.
    durable = compute_reads_staging_path(inputs.reads_staging_root, 10)
    durable.parent.mkdir(parents=True)
    (durable.parent / f"{durable.name}.partial").write_text("truncated")

    _run(inputs, tmp_path / "ws")

    # The sample was (re-)ingested — the partial did not short-circuit it.
    assert fake_mint == [(10, 1)]
    assert durable.exists()
    assert not (durable.parent / f"{durable.name}.partial").exists()


def test_all_empty_pool_is_no_data(fake_mint, tmp_path):
    """Every well empty → StepNoData (no reads to register at all)."""
    convert_dir = _seed_convert_dir(tmp_path, {"10": [], "11": []})
    inputs = _inputs(tmp_path, convert_dir, [(10, "10"), (11, "11")])

    with pytest.raises(StepNoData):
        _run(inputs, tmp_path / "ws")
    assert fake_mint == []
