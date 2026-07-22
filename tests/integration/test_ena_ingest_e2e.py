"""Integration test: prove ENA-fetched reads land in DuckLake `read` through
the EXISTING tail — `ingest_ena_reads.execute()` -> the real `register-files`
action primitive -> the real data-plane `register_files` DoAction -> the real
DuckLake catalog. No product/data-plane code changes accompany this file (see
the reconciled parity-verification plan): the verdict is that the tail already
works, and this test is the proof.

**What's exercised (Option A: call execute() directly, then drive the REAL
register-files tail).**

  - Two sequenced prep_samples are seeded (`seed_biosample_with_sequenced_
    prep_sample`), one destined paired-end, one single-end, and a `run_map`
    roster of `(prep_sample_idx, ena_run_accession)` is staged exactly as the
    runner would (mirrors `qiita-compute-orchestrator/tests/
    test_ingest_ena_reads.py`'s `_write_run_map`).
  - `ingest_ena_reads._stage_run_reads` — the WHOLE `read_ena_sequences` seam
    (ENA metadata resolution + the HTTP download) — is monkeypatched to write
    a small ENA-shaped intermediate Parquet inline via a DuckDB COPY, keyed by
    `sequence_index, read_id, sequence1, qual1, sequence2, qual2` (the exact
    columns/order `_stage_run_reads` itself copies). No network, no
    checked-in fixture, no `QIITA_ENA_LIVE_SMOKE` opt-in.
  - `sequence_range_retry.mint_sequence_range` is monkeypatched to call
    `qiita.mint_sequence_range(...)` directly via `postgres_pool` (verbatim
    from `test_native_step_smoke.py`) so the CP-minted `sequence_idx` range is
    REAL (not a fake counter) without an in-process HTTP control plane.
  - `ingest_ena_reads.execute(inputs, workspace)` runs UNMOCKED beyond the two
    seams above — real `write_sorted_reads`/`hardlink` (`..read_staging`), real
    atomic-publish-then-hardlink pipeline.
  - The real `register-files` YAML entry (parsed from the shipped
    `workflows/download-ena-study/1.0.0.yaml`, mirroring
    `test_read_mask_e2e.py`'s `_entry_by_name` pattern) is driven through the
    real `qiita_control_plane.runner._run_action_primitive`, which calls the
    real `LIBRARY[LibraryPrimitive.REGISTER_FILES]` DoAction against the real
    data-plane process and real DuckLake catalog (`data_plane` /
    `ducklake_connect` fixtures).

**AMENDMENT 2 — what the "identical to the bcl-convert/fastq-to-parquet path"
parity claim rests on.** `ingest_ena_reads` and `ingest_reads` (bcl-convert's
read-storage step) share `qiita_compute_orchestrator.read_staging.
write_sorted_reads` / `hardlink` verbatim (see that module's docstring) — the
parity claim is CODE IDENTITY, proven by that shared import, not by an
existing bcl-convert-path baseline test (none exists today for EITHER path;
`test_native_step_smoke.py` covers `fastq_to_parquet`, a different native
step, not `ingest_reads`). This file is the first end-to-end proof that a
`read_staging`-produced part lands correctly via `register-files`, for either
producer. Separately, the mode-0o440 Parquet-output guarantee is a structural
property of the SLURM entrypoint (`jobs/__main__.py`'s `_chmod`, run after a
real `sbatch`), not of `execute()` called directly as this test does (the
LocalBackend path) — it is asserted by inspection of that code, not exercised
here.

**AMENDMENT 3 — real ENA fetch is out of scope.** The real
`read_ena_sequences`/miint ENA network fetch is deliberately NOT exercised
(monkeypatched at the `_stage_run_reads` seam) — proving THAT path is
the download job's own unit-test job (`qiita-compute-orchestrator/tests/
test_ingest_ena_reads.py`, plus its opt-in `QIITA_ENA_LIVE_SMOKE=1` live
smoke) and the md5-verification escalation's. This file's scope is
narrower and complementary: prove the storage TAIL from a staged intermediate
onward, the one seam that job's suite does not reach (it never touches a real
data plane / DuckLake).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
import yaml
from qiita_common.actions import ActionDefinition, WorkflowAction
from qiita_common.api_paths import LOOPBACK_HOST

from conftest import ducklake_connect

_DOWNLOAD_ENA_STUDY_YAML_PATH = (
    Path(__file__).parent.parent.parent
    / "workflows"
    / "download-ena-study"
    / "1.0.0.yaml"
)

# The DuckLake `read` table's 7-column shape (qiita-data-plane/src/ducklake.rs
# `ensure_read_tables`), asserted the same way test_native_step_smoke.py
# asserts the Parquet a native step writes: (column_name, column_type) pairs
# in DuckDB DESCRIBE order.
READ_TABLE_SCHEMA = [
    ("prep_sample_idx", "BIGINT"),
    ("sequence_idx", "BIGINT"),
    ("read_id", "VARCHAR"),
    ("sequence1", "VARCHAR"),
    ("qual1", "UTINYINT[]"),
    ("sequence2", "VARCHAR"),
    ("qual2", "UTINYINT[]"),
]


def _download_ena_study_action_entries():
    """Parse the shipped download-ena-study YAML and return its `action:`
    WorkflowAction entries in declared order — same pattern as
    test_read_mask_e2e.py's `_read_mask_action_entries`, so a future reorder
    of this workflow's tail is reflected automatically rather than drifting
    from a hand-copied entry."""
    data = yaml.safe_load(_DOWNLOAD_ENA_STUDY_YAML_PATH.read_text())
    action = ActionDefinition.model_validate(data)
    return [e for e in action.steps if isinstance(e, WorkflowAction)]


def _entry_by_name(name: str):
    for entry in _download_ena_study_action_entries():
        if entry.name == name:
            return entry
    raise AssertionError(f"no action entry named {name!r} in download-ena-study YAML")


def _write_run_map(path: Path, roster: list[tuple[int, str]]) -> None:
    """Write the `(prep_sample_idx, ena_run_accession)` roster Parquet the
    runner materializes for the step. Verbatim shape from
    qiita-compute-orchestrator/tests/test_ingest_ena_reads.py's
    `_write_run_map`."""
    rows = ", ".join(f"({idx}, '{acc}')" for idx, acc in roster)
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES "
            + rows
            + ") AS t(prep_sample_idx, ena_run_accession)) "
            f"TO '{path}' (FORMAT parquet)"
        )


def _write_intermediate(
    path: Path,
    rows: list[tuple[int, str, str, list[int] | None, str | None, list[int] | None]],
) -> None:
    """Write the `_stage_run_reads` intermediate shape: `(sequence_index,
    read_id, sequence1, qual1, sequence2, qual2)` — miint's 1-based per-run
    row index, the exact 6-column projection `_stage_run_reads` itself
    copies. Verbatim from qiita-compute-orchestrator/tests/
    test_ingest_ena_reads.py's `_write_intermediate`."""
    with duckdb.connect(":memory:") as conn:
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


def _fake_stage_run_reads_factory(by_run: dict[str, tuple[list[tuple], list[str]]]):
    """Build a `_stage_run_reads`-shaped fake keyed by run_accession — no
    network, no real miint ENA extension. Each entry is `(rows, warnings)`;
    `rows` are `_write_intermediate` row tuples."""

    def _fake(
        run_accession,
        download_method,
        intermediate_path,
        duckdb_tmp,
        memory_gb,
        threads,
    ):
        rows, warnings = by_run[run_accession]
        _write_intermediate(intermediate_path, rows)
        return len(rows), warnings

    return _fake


def _data_plane_url(data_plane) -> str:
    return f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"


def _count_read_rows(data_plane, prep_sample_idxs: list[int]) -> int:
    conn = ducklake_connect(data_plane["data_path"])
    try:
        (n,) = conn.execute(
            "SELECT count(*) FROM qiita_lake.read WHERE prep_sample_idx = ANY(?)",
            [list(prep_sample_idxs)],
        ).fetchone()
        return n
    finally:
        conn.close()


async def _run_register_files(
    postgres_pool, data_plane, *, staging_dir: Path, work_ticket_idx: int
):
    """Drive the REAL runner adapter for the download-ena-study `register-files`
    entry — the exact production tail `ingest_ena_reads` hands its
    `read_staging_dir` output to."""
    from qiita_control_plane.runner import _run_action_primitive

    entry = _entry_by_name("register-files")
    await _run_action_primitive(
        postgres_pool,
        entry,
        {"read_staging_dir": str(staging_dir)},
        staging_dir,
        {},
        work_ticket_idx=work_ticket_idx,
        signing_key=data_plane["secret"],
        data_plane_url=_data_plane_url(data_plane),
    )


@pytest.fixture
async def two_ena_prep_samples(postgres_pool, human_admin_session):
    """Two sequenced prep_samples (distinct biosamples) to scope the roster
    against — one destined paired-end, one single-end. Reverse-FK cleanup on
    teardown; `qiita.sequence_range` rows cascade off `prep_sample` deletion
    (ON DELETE CASCADE), so no separate cleanup is needed for the ranges this
    test mints."""
    from qiita_control_plane.testing.db_seeds import (
        seed_biosample_with_sequenced_prep_sample,
    )

    admin_idx = human_admin_session["principal_idx"]
    biosample_paired, prep_paired = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=admin_idx
    )
    biosample_single, prep_single = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=admin_idx
    )
    yield {"paired": prep_paired, "single": prep_single}
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1)",
        [prep_paired, prep_single],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1)",
        [biosample_paired, biosample_single],
    )


# Fixture rows: 3 paired-end reads (non-NULL sequence2/qual2), 2 single-end
# reads (NULL sequence2/qual2). Small and deterministic — just enough to prove
# shape, contiguity, and the paired/single-end NULL split.
_PAIRED_ROWS = [
    (1, "ERR-PAIR-r1", "ACGTACGTAC", [30] * 10, "TGCATGCATG", [30] * 10),
    (2, "ERR-PAIR-r2", "GGGGCCCCTT", [35] * 10, "AAAATTTTGG", [35] * 10),
    (3, "ERR-PAIR-r3", "TTTTAAAACC", [40] * 10, "CCCCGGGGAA", [40] * 10),
]
_SINGLE_ROWS = [
    (1, "ERR-SINGLE-r1", "ACGTACGTAC", [30] * 10, None, None),
    (2, "ERR-SINGLE-r2", "GGGGCCCCTT", [35] * 10, None, None),
]
_PAIRED_RUN_ACCESSION = "ERR_PAIR001"
_SINGLE_RUN_ACCESSION = "ERR_SINGLE001"
_WORK_TICKET_IDX = 91001


async def test_ena_reads_land_in_ducklake_read_via_register_files(
    postgres_pool,
    data_plane,
    two_ena_prep_samples,
    human_admin_session,
    tmp_path,
    monkeypatch,
):
    """End-to-end: two runs (one paired, one single-end) fetched via the
    monkeypatched `_stage_run_reads` seam, minted through the real
    `qiita.mint_sequence_range`, written by the real `write_sorted_reads` /
    `hardlink` pipeline, and registered into the real DuckLake `read` table by
    the real `register-files` action. Then proves the register is STORED
    ONCE, not merely stored: re-running the idempotent hardlink-of-durable-
    copy path and re-registering under the SAME work_ticket_idx is refused by
    the data plane (a colliding lake destination filename), leaving the row
    count unchanged."""
    from qiita_compute_orchestrator import sequence_range_retry
    from qiita_compute_orchestrator.jobs import ingest_ena_reads
    from qiita_compute_orchestrator.sequence_range import MintedSequenceRange

    admin_idx = human_admin_session["principal_idx"]
    prep_paired = two_ena_prep_samples["paired"]
    prep_single = two_ena_prep_samples["single"]

    # In-process replacement for mint_sequence_range that calls the CP's mint
    # function directly through the existing postgres_pool — verbatim pattern
    # from test_native_step_smoke.py's `_local_mint`.
    mint_calls: list[tuple[int, int]] = []

    async def _local_mint(*, http, prep_sample_idx, count, work_ticket_idx):
        mint_calls.append((prep_sample_idx, count))
        row = await postgres_pool.fetchrow(
            "SELECT * FROM qiita.mint_sequence_range($1, $2, $3, $4)",
            prep_sample_idx,
            count,
            admin_idx,
            work_ticket_idx,
        )
        return MintedSequenceRange(
            prep_sample_idx=row["prep_sample_idx"],
            sequence_idx_start=row["sequence_idx_start"],
            sequence_idx_stop=row["sequence_idx_stop"],
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _local_mint)

    # No network, no checked-in fixture: the whole read_ena_sequences seam is
    # replaced with an inline DuckDB COPY of a few ENA-shaped rows.
    by_run = {
        _PAIRED_RUN_ACCESSION: (_PAIRED_ROWS, []),
        _SINGLE_RUN_ACCESSION: (_SINGLE_ROWS, []),
    }
    monkeypatch.setattr(
        ingest_ena_reads, "_stage_run_reads", _fake_stage_run_reads_factory(by_run)
    )

    roster = [
        (prep_paired, _PAIRED_RUN_ACCESSION),
        (prep_single, _SINGLE_RUN_ACCESSION),
    ]
    run_map_path = tmp_path / "run_map.parquet"
    _write_run_map(run_map_path, roster)

    inputs = ingest_ena_reads.Inputs(
        run_map=run_map_path,
        reads_staging_root=tmp_path / "reads-staging",
        sequenced_pool_idx=1,
        sequencing_run_idx=1,
        work_ticket_idx=_WORK_TICKET_IDX,
    )

    # --- Call execute() directly (Option A) ---
    outputs = await ingest_ena_reads.execute(inputs, tmp_path / "ws1")
    assert sorted(mint_calls) == [
        (prep_paired, len(_PAIRED_ROWS)),
        (prep_single, len(_SINGLE_ROWS)),
    ]

    # --- Drive the REAL register-files tail into the REAL data plane. ---
    await _run_register_files(
        postgres_pool,
        data_plane,
        staging_dir=outputs["read_staging_dir"],
        work_ticket_idx=_WORK_TICKET_IDX,
    )

    total_reads = len(_PAIRED_ROWS) + len(_SINGLE_ROWS)

    # --- Assert against the real DuckLake `read` table. ---
    conn = ducklake_connect(data_plane["data_path"])
    try:
        schema = conn.execute(
            "SELECT column_name, column_type FROM (DESCRIBE qiita_lake.read)"
        ).fetchall()
        assert schema == READ_TABLE_SCHEMA

        total = conn.execute(
            "SELECT count(*) FROM qiita_lake.read WHERE prep_sample_idx = ANY(?)",
            [[prep_paired, prep_single]],
        ).fetchone()[0]
        assert total == total_reads

        distinct = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT prep_sample_idx FROM qiita_lake.read"
                " WHERE prep_sample_idx = ANY(?)",
                [[prep_paired, prep_single]],
            ).fetchall()
        }
        assert distinct == {prep_paired, prep_single}

        for prep_idx, rows in (
            (prep_paired, _PAIRED_ROWS),
            (prep_single, _SINGLE_ROWS),
        ):
            mn, mx, cnt = conn.execute(
                "SELECT min(sequence_idx), max(sequence_idx), count(*)"
                " FROM qiita_lake.read WHERE prep_sample_idx = ?",
                [prep_idx],
            ).fetchone()
            assert cnt == len(rows)
            # Contiguous minted range: exactly count values, no gaps.
            assert mx - mn + 1 == cnt

        paired_seq2 = [
            r[0]
            for r in conn.execute(
                "SELECT sequence2 FROM qiita_lake.read WHERE prep_sample_idx = ?",
                [prep_paired],
            ).fetchall()
        ]
        assert len(paired_seq2) == len(_PAIRED_ROWS)
        assert all(v is not None for v in paired_seq2)

        single_seq2 = [
            r[0]
            for r in conn.execute(
                "SELECT sequence2 FROM qiita_lake.read WHERE prep_sample_idx = ?",
                [prep_single],
            ).fetchall()
        ]
        assert len(single_seq2) == len(_SINGLE_ROWS)
        assert all(v is None for v in single_seq2)
    finally:
        conn.close()

    # -------------------------------------------------------------------
    # AMENDMENT 1: prove STORED ONCE, not just stored.
    #
    # Re-execute ingest_ena_reads against the SAME reads_staging_root: the
    # durable per-sample read.parquet already exists, so execute() takes its
    # idempotent fast path (no re-fetch, no re-mint — see the module
    # docstring) and simply re-creates the register part by hardlinking to
    # the SAME durable inode, in a FRESH workspace. Re-running register-files
    # with the SAME work_ticket_idx is then refused: the data plane mints a
    # lake destination filename deterministically from
    # (work_ticket_idx, basename) (`lake_dest_filename`), and refuses to
    # overwrite an existing one (`move_file`'s AlreadyExists guard — see
    # qiita-data-plane/src/flight_service.rs). This is the actual mechanism
    # that keeps a same-ticket retry from duplicating rows: it is a REFUSAL
    # (a raised error), not a silent, graceful no-op — the read table itself
    # has no uniqueness constraint (mirrors read_mask; see
    # test_read_mask_e2e.py's docstring), so nothing else would stop a
    # different-ticket resubmit from duplicating (that is exactly what the
    # CP's submit-time disallow-without-delete gate exists to prevent, one
    # layer up from what this test exercises).
    #
    # NOTE on the exception type: `library.register_files`'s docstring says
    # "Raises pyarrow.flight.FlightError on transport / data-plane failure",
    # but an application-level DoAction error with no dedicated Arrow Flight
    # status mapping (Rust's `Status::already_exists` here) actually surfaces
    # as a plain `pyarrow.lib.ArrowException` ("Unknown: ..."), NOT a
    # `pyarrow.flight.FlightError` subclass — confirmed empirically running
    # this test. That docstring is imprecise for this case; out of scope to
    # fix here (no product code changes accompany this test).
    # -------------------------------------------------------------------
    outputs2 = await ingest_ena_reads.execute(inputs, tmp_path / "ws2")
    # No new mint calls — the durable copy already existed (idempotent skip).
    assert sorted(mint_calls) == [
        (prep_paired, len(_PAIRED_ROWS)),
        (prep_single, len(_SINGLE_ROWS)),
    ]

    with pytest.raises(pa.lib.ArrowException, match="already exists"):
        await _run_register_files(
            postgres_pool,
            data_plane,
            staging_dir=outputs2["read_staging_dir"],
            work_ticket_idx=_WORK_TICKET_IDX,  # SAME ticket -> colliding lake filename
        )

    assert _count_read_rows(data_plane, [prep_paired, prep_single]) == total_reads
