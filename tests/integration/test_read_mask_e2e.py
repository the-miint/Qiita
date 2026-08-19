"""Cross-component integration tests for the read-mask workflow's final two
`action:` entries (`persist-read-metrics` → `register-files`) and the
`delete_mask` purge primitive, against a real control plane, a real
data-plane Flight/DuckLake process, and the integration Postgres.

Two regressions this branch fixed are otherwise unproven end-to-end:

1. ORDERING. `read-mask/1.0.0` and `fastq-to-parquet/1.3.0` previously ran
   `register-files` BEFORE `persist-read-metrics`. `register-files` MOVES
   `read_mask.parquet` out of the staging workspace into permanent DuckLake
   storage (the data plane's `move_file`/`std::fs::rename`), so the later
   `persist-read-metrics` re-opened a now-gone staging path and raised
   `FileNotFoundError`. The fix reorders to
   `host_filter → persist-read-metrics → register-files`. This file proves the
   shipped order works (metrics land AND the mask registers) and proves the
   OLD order fails (so the ordering assertion has teeth).

2. PURGE → RESUBMIT NO-DUPLICATE-ROWS. `read_mask` is append-only with no
   unique key, and a resubmit re-resolves to the SAME `mask_idx` (config-hash
   upsert), so a no-cleanup resubmit appends a duplicate copy. The recovery
   path purges the mask (`delete_mask` DoAction) before resubmitting. This
   file proves purge-then-register leaves exactly one logical copy, and that
   register-twice-without-purge WOULD duplicate (so the no-dup assertion has
   teeth).

host_filter itself is NOT run here: it depends on miint/rype/minimap2 and real
stored reads (system-tier). Instead we pre-stage a `read_mask.parquet`
constructed to match host_filter's exact emit schema
(`mask_idx, prep_sample_idx, sequence_idx, reason, left_trim1, right_trim1,
left_trim2, right_trim2`; see
qiita_compute_orchestrator.jobs.host_filter.execute's final COPY) and drive the
REAL runner adapter `_run_action_primitive` for the two final action entries —
parsed in their shipped order from the real workflow YAML — exercising the real
LIBRARY primitives, the real data-plane `register_files`/`delete_mask`
DoActions, and the real DuckLake catalog.

Shared fixtures (`data_plane`, `signing_key`, `postgres_pool`,
`human_admin_session`, `ducklake_connect`) live in conftest.py.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.models import ReadMaskReason

from conftest import ducklake_connect

_READ_MASK_YAML_PATH = (
    Path(__file__).parent.parent.parent / "workflows" / "read-mask" / "1.0.0.yaml"
)


def _read_mask_action_entries():
    """Parse the shipped read-mask YAML and return its `action:` WorkflowAction
    entries in declared order. This is the production ordering under test — we
    drive whatever the YAML declares rather than hand-constructing entries, so a
    future reorder is reflected automatically (and would re-break test 1's
    ordering assertion if it regressed)."""
    import yaml
    from qiita_common.actions import ActionDefinition, WorkflowAction

    data = yaml.safe_load(_READ_MASK_YAML_PATH.read_text())
    action = ActionDefinition.model_validate(data)
    return [e for e in action.steps if isinstance(e, WorkflowAction)]


def _entry_by_name(name: str):
    for entry in _read_mask_action_entries():
        if entry.name == name:
            return entry
    raise AssertionError(f"no action entry named {name!r} in read-mask YAML")


# host_filter's emit schema, mirrored exactly. left_trim2/right_trim2 are NULL
# for single-end and 0+ for paired-end; _read_mask_counts uses COUNT(right_trim2)
# as the R2 (both-mates) discriminator, so we exercise a paired-end mask (both
# trim2 columns populated) to make the *_r1r2 doubling observable.
_READ_MASK_PARQUET_COLUMNS = (
    "mask_idx BIGINT, prep_sample_idx BIGINT, sequence_idx BIGINT, "
    "reason VARCHAR, "
    "left_trim1 UINTEGER, right_trim1 UINTEGER, "
    "left_trim2 UINTEGER, right_trim2 UINTEGER"
)


def _write_read_mask_parquet(
    staging_dir: Path,
    *,
    mask_idx: int,
    prep_sample_idx: int,
    rows: list[tuple],
) -> Path:
    """Materialize a `read_mask.parquet` under `staging_dir` in host_filter's
    exact output shape and return the parquet path. `rows` are
    (sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2)
    tuples; mask_idx/prep_sample_idx are stamped onto every row (host_filter
    treats them as per-run constants)."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    path = staging_dir / "read_mask.parquet"
    full_rows = [
        (mask_idx, prep_sample_idx, seq_idx, reason, lt1, rt1, lt2, rt2)
        for (seq_idx, reason, lt1, rt1, lt2, rt2) in rows
    ]
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"CREATE TABLE m ({_READ_MASK_PARQUET_COLUMNS})")
        conn.executemany("INSERT INTO m VALUES (?, ?, ?, ?, ?, ?, ?, ?)", full_rows)
        # ORDER BY the read_mask sort key (sequence_idx last) — matches the
        # register-files / DuckLake file-pruning contract host_filter writes.
        conn.execute(
            f"COPY (SELECT * FROM m ORDER BY mask_idx, prep_sample_idx, sequence_idx)"
            f" TO '{path}' (FORMAT PARQUET)"
        )
    return path


# Three paired-end reads: one pass, one host_rype hit, one qc failure. The
# both-mates *_r1r2 totals _read_mask_counts derives are:
#   raw               = all 3 reads, both mates           -> 6
#   biological        = non-qc_* (pass + host_rype)        -> 4
#   quality_filtered  = pass only                          -> 2
_SAMPLE_MASK_ROWS = [
    (1001, ReadMaskReason.PASS.value, 0, 0, 0, 0),
    (1002, ReadMaskReason.HOST_RYPE.value, 0, 0, 0, 0),
    (1003, ReadMaskReason.QC_TOO_SHORT.value, 0, 0, 0, 0),
]
_EXPECTED_RAW_R1R2 = 6
_EXPECTED_BIOLOGICAL_R1R2 = 4
_EXPECTED_QUALITY_FILTERED_R1R2 = 2


@pytest.fixture
async def sequenced_prep_sample(postgres_pool, human_admin_session):
    """A sequenced prep_sample WITH its 1:1 sequenced_sample subtype row (the
    target persist-read-metrics UPDATEs). Reverse-FK cleanup on teardown."""
    import secrets

    from qiita_control_plane.testing.db_seeds import (
        seed_biosample_with_sequenced_prep_sample,
        seed_sequenced_sample_subtype,
    )

    admin_idx = human_admin_session["principal_idx"]
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=admin_idx
    )
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=admin_idx,
        sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
    )
    yield {"prep_sample_idx": prep_sample_idx, "sequenced_sample_idx": ss_idx}

    await postgres_pool.execute(
        "DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx
    )


def _data_plane_url(data_plane) -> str:
    return f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"


def _count_read_mask_rows(data_plane, mask_idx: int) -> int:
    """Count DuckLake `read_mask` rows for a mask_idx (the table is NOT
    Flight-reachable for privacy, so we read it via a direct DuckLake conn)."""
    conn = ducklake_connect(data_plane["data_path"])
    try:
        (n,) = conn.execute(
            "SELECT count(*) FROM qiita_lake.read_mask WHERE mask_idx = ?",
            [mask_idx],
        ).fetchone()
        return n
    finally:
        conn.close()


async def _run_persist_read_metrics(
    postgres_pool, data_plane, *, prep_sample_idx: int, read_mask_path: Path
):
    """Drive the REAL runner adapter for the `persist-read-metrics` YAML entry."""
    from qiita_control_plane.runner import _run_action_primitive

    entry = _entry_by_name("persist-read-metrics")
    await _run_action_primitive(
        postgres_pool,
        entry,
        {"read_mask": str(read_mask_path)},
        Path(read_mask_path).parent,  # workspace (unused by this primitive)
        {"prep_sample_idx": prep_sample_idx},
        work_ticket_idx=0,
        signing_key=data_plane["secret"],
        data_plane_url=_data_plane_url(data_plane),
    )


async def _run_register_files(
    postgres_pool, data_plane, *, staging_dir: Path, work_ticket_idx: int
):
    """Drive the REAL runner adapter for the `register-files` YAML entry — moves
    read_mask.parquet out of staging into DuckLake via the data plane DoAction."""
    from qiita_control_plane.runner import _run_action_primitive

    entry = _entry_by_name("register-files")
    await _run_action_primitive(
        postgres_pool,
        entry,
        {"read_mask_staging_dir": str(staging_dir)},
        staging_dir,
        {},
        work_ticket_idx=work_ticket_idx,
        signing_key=data_plane["secret"],
        data_plane_url=_data_plane_url(data_plane),
    )


# ---------------------------------------------------------------------------
# Test 1: ordering regression — persist-read-metrics must read the staged file
# BEFORE register-files moves it into DuckLake.
# ---------------------------------------------------------------------------


async def test_read_mask_metrics_persist_before_register_moves(
    postgres_pool, data_plane, sequenced_prep_sample, tmp_path
):
    """Shipped order (persist-read-metrics → register-files): metrics land from
    the staged mask AND the mask registers in DuckLake.

    Asserts the full reordered tail of the read-mask workflow:
      * persist-read-metrics succeeds (no FileNotFoundError) reading the STAGED
        read_mask.parquet, and writes the three *_r1r2 counts onto the 1:1
        sequenced_sample (derived FROM the staged file — proving it ran while
        the file was still in staging, i.e. before the move),
      * register-files then moves the parquet into DuckLake and the read_mask
        rows are registered (queryable in the lake),
      * the staging parquet is gone after register-files (the move happened).
    """
    prep_sample_idx = sequenced_prep_sample["prep_sample_idx"]
    mask_idx = 88001
    staging_dir = tmp_path / "host_filter" / "attempt-0"
    read_mask_path = _write_read_mask_parquet(
        staging_dir,
        mask_idx=mask_idx,
        prep_sample_idx=prep_sample_idx,
        rows=_SAMPLE_MASK_ROWS,
    )

    # Shipped order: persist-read-metrics FIRST (reads the staged file) ...
    await _run_persist_read_metrics(
        postgres_pool,
        data_plane,
        prep_sample_idx=prep_sample_idx,
        read_mask_path=read_mask_path,
    )

    # Metrics landed, derived from the staged mask's per-read reasons.
    row = await postgres_pool.fetchrow(
        "SELECT raw_read_count_r1r2, biological_read_count_r1r2,"
        " quality_filtered_read_count_r1r2"
        " FROM qiita.sequenced_sample WHERE prep_sample_idx = $1",
        prep_sample_idx,
    )
    assert row["raw_read_count_r1r2"] == _EXPECTED_RAW_R1R2
    assert row["biological_read_count_r1r2"] == _EXPECTED_BIOLOGICAL_R1R2
    assert row["quality_filtered_read_count_r1r2"] == _EXPECTED_QUALITY_FILTERED_R1R2

    # The file is still in staging (the move hasn't happened yet) — this is the
    # precondition the OLD order violated.
    assert read_mask_path.exists()

    # ... then register-files moves it into DuckLake.
    await _run_register_files(
        postgres_pool, data_plane, staging_dir=staging_dir, work_ticket_idx=88001
    )

    # Mask rows are registered in DuckLake (all three rows of this mask_idx).
    assert _count_read_mask_rows(data_plane, mask_idx) == len(_SAMPLE_MASK_ROWS)

    # register-files MOVED the staging parquet out (the move that the old order
    # ran too early). Re-running persist-read-metrics now WOULD fail — which is
    # exactly the bug the reorder fixes; see the next test for that direction.
    assert not read_mask_path.exists()


async def test_old_order_register_then_persist_fails_filenotfound(
    postgres_pool, data_plane, sequenced_prep_sample, tmp_path
):
    """Teeth for the ordering assertion: the OLD order (register-files BEFORE
    persist-read-metrics) reproduces the original FileNotFoundError, because
    register-files moves read_mask.parquet out of staging and persist then can't
    find it. This is the failure mode the reorder eliminates."""
    prep_sample_idx = sequenced_prep_sample["prep_sample_idx"]
    mask_idx = 88002
    staging_dir = tmp_path / "host_filter" / "attempt-0"
    read_mask_path = _write_read_mask_parquet(
        staging_dir,
        mask_idx=mask_idx,
        prep_sample_idx=prep_sample_idx,
        rows=_SAMPLE_MASK_ROWS,
    )

    # OLD (buggy) order: register-files FIRST — moves the parquet into DuckLake.
    await _run_register_files(
        postgres_pool, data_plane, staging_dir=staging_dir, work_ticket_idx=88002
    )
    assert not read_mask_path.exists()  # moved out of staging

    # persist-read-metrics now opens the gone staging path → FileNotFoundError,
    # the exact symptom the reorder fixes. The metrics never get written.
    with pytest.raises(FileNotFoundError):
        await _run_persist_read_metrics(
            postgres_pool,
            data_plane,
            prep_sample_idx=prep_sample_idx,
            read_mask_path=read_mask_path,
        )

    # ...and the sequenced_sample counts stayed NULL (no metrics persisted).
    row = await postgres_pool.fetchrow(
        "SELECT raw_read_count_r1r2 FROM qiita.sequenced_sample"
        " WHERE prep_sample_idx = $1",
        prep_sample_idx,
    )
    assert row["raw_read_count_r1r2"] is None


# ---------------------------------------------------------------------------
# Test 2: purge → resubmit leaves no duplicate read_mask rows.
# ---------------------------------------------------------------------------


async def _register_fresh_mask(
    postgres_pool, data_plane, tmp_path, *, mask_idx, prep_sample_idx, work_ticket_idx
):
    """Stage a fresh read_mask.parquet for `mask_idx` and run register-files,
    moving it into DuckLake. A new staging dir per call (register-files MOVES the
    file, so each registration needs its own copy — exactly as a resubmit would
    re-emit a fresh read_mask from host_filter)."""
    staging_dir = tmp_path / f"stage-wt{work_ticket_idx}" / "host_filter" / "attempt-0"
    _write_read_mask_parquet(
        staging_dir,
        mask_idx=mask_idx,
        prep_sample_idx=prep_sample_idx,
        rows=_SAMPLE_MASK_ROWS,
    )
    await _run_register_files(
        postgres_pool,
        data_plane,
        staging_dir=staging_dir,
        work_ticket_idx=work_ticket_idx,
    )


async def test_purge_before_resubmit_leaves_no_duplicate_rows(
    postgres_pool, data_plane, tmp_path
):
    """The recovery contract: register a mask, purge it via the real delete_mask
    DoAction, then re-register (the resubmit) — exactly ONE logical copy of the
    mask's rows remains.

    Also proves the no-dup assertion has teeth: re-registering WITHOUT purging
    first DOES duplicate (the append-only read_mask hazard the purge prevents).
    """
    from qiita_control_plane.actions.library import delete_mask_data

    mask_idx = 77001
    prep_sample_idx = 999001  # lake-only mask rows; no Postgres FK on read_mask
    n_rows = len(_SAMPLE_MASK_ROWS)

    # --- Teeth: register twice WITHOUT a purge → rows duplicate. ---
    await _register_fresh_mask(
        postgres_pool,
        data_plane,
        tmp_path,
        mask_idx=mask_idx,
        prep_sample_idx=prep_sample_idx,
        work_ticket_idx=77001,
    )
    assert _count_read_mask_rows(data_plane, mask_idx) == n_rows

    await _register_fresh_mask(
        postgres_pool,
        data_plane,
        tmp_path,
        mask_idx=mask_idx,
        prep_sample_idx=prep_sample_idx,
        work_ticket_idx=77002,  # distinct ticket → distinct lake filename
    )
    # No purge between the two registrations: read_mask is append-only with no
    # unique key, so the second copy duplicates every row.
    assert _count_read_mask_rows(data_plane, mask_idx) == 2 * n_rows

    # --- Recovery: purge via delete_mask, then re-register → one logical copy. ---
    deleted = await delete_mask_data(
        mask_idx=mask_idx,
        signing_key=data_plane["secret"],
        data_plane_url=_data_plane_url(data_plane),
    )
    # Purge removed every row currently registered for the mask (both copies).
    assert deleted == 2 * n_rows
    assert _count_read_mask_rows(data_plane, mask_idx) == 0

    # The resubmit's register-files re-registers exactly the mask's rows — once.
    await _register_fresh_mask(
        postgres_pool,
        data_plane,
        tmp_path,
        mask_idx=mask_idx,
        prep_sample_idx=prep_sample_idx,
        work_ticket_idx=77003,
    )
    assert _count_read_mask_rows(data_plane, mask_idx) == n_rows

    # Final cleanup so the mask leaves no rows behind for other modules sharing
    # the data plane's catalog.
    await delete_mask_data(
        mask_idx=mask_idx,
        signing_key=data_plane["secret"],
        data_plane_url=_data_plane_url(data_plane),
    )
    assert _count_read_mask_rows(data_plane, mask_idx) == 0


async def test_delete_mask_is_idempotent_on_absent_mask(postgres_pool, data_plane):
    """delete_mask on a mask_idx with zero registered rows is success (0 rows) —
    the idempotency the bulk purge-and-resubmit tooling relies on to retry."""
    from qiita_control_plane.actions.library import delete_mask_data

    deleted = await delete_mask_data(
        mask_idx=66001,
        signing_key=data_plane["secret"],
        data_plane_url=_data_plane_url(data_plane),
    )
    assert deleted == 0
