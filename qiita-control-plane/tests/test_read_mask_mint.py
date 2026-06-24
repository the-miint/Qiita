"""Tests for the runner's read-mask identity (mask_idx) minting.

`_mint_read_mask` resolves a prep_sample's filtering config (filter workflow +
version + the host references the host_filter step actually applies, passed
through from action_context + the resolved QC config) and mints a deduplicated
mask_idx via mint_mask_definition. The same effective config resolves to the same
mask_idx; a different config (a different host reference, instrument, or adapter
set) mints a new one.

`_workflow_needs_mask` is the pure-unit gate that decides whether the runner
mints a mask before the step loop (keys off a step threading `mask_idx` via
`params:`).
"""

import json
import secrets

import pytest
import pytest_asyncio
from qiita_common.actions import WorkflowAction, WorkflowStep

from qiita_control_plane import runner
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)


def _step(name: str, params: dict | None = None) -> WorkflowStep:
    return WorkflowStep.model_validate(
        {
            "kind": "step",
            "name": name,
            "step_type": "singleton",
            "module": f"qiita_compute_orchestrator.jobs.{name}",
            "inputs": [],
            "params": params or {},
            "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
        }
    )


def test_workflow_needs_mask_true_when_param_threads_mask_idx():
    steps = [_step("fastq"), _step("host_filter", params={"mask_idx": "mask_idx"})]
    assert runner._workflow_needs_mask(steps) is True


def test_workflow_needs_mask_false_without_mask_param():
    steps = [
        _step("fastq"),
        _step("qc", params={"instrument_model": "instrument_model"}),
        WorkflowAction.model_validate(
            {"kind": "action", "name": "register-files", "inputs": ["x"]}
        ),
    ]
    assert runner._workflow_needs_mask(steps) is False


# --------------------------------------------------------------------------- DB


@pytest_asyncio.fixture
async def seeded(postgres_pool):
    """Seed principal + biosample + sequenced prep_sample + sequenced_sample
    subtype; yield the ids and clean up FK-reverse."""
    principal_idx = await seed_user_principal(postgres_pool, prefix="mask-mint", suffix="owner")
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
    )
    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idx": prep_sample_idx,
    }
    await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


@pytest.mark.db
async def test_mint_read_mask_binds_and_dedups(seeded):
    """The same config mints once and resolves to the same mask_idx; a different
    instrument model mints a distinct mask_idx (config drives identity)."""
    pool = seeded["pool"]
    common = dict(
        action_id="fastq-to-parquet",
        action_version="1.3.0",
        prep_sample_idx=seeded["prep_sample_idx"],
        originator_principal_idx=seeded["principal_idx"],
        adapter_parquet=None,
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
    )
    a = await runner._mint_read_mask(pool, instrument_model="NextSeq 550", **common)
    b = await runner._mint_read_mask(pool, instrument_model="NextSeq 550", **common)
    c = await runner._mint_read_mask(pool, instrument_model="Illumina MiSeq", **common)

    assert runner.MASK_IDX_BINDING in a
    assert a["mask_idx"] == b["mask_idx"]  # same config -> same mask
    assert c["mask_idx"] != a["mask_idx"]  # different instrument -> different mask
    # cleanup the minted rows so the shared DB stays clean
    await pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
        [a["mask_idx"], c["mask_idx"]],
    )


@pytest.mark.db
async def test_mint_read_mask_host_ref_drives_identity(seeded):
    """The host refs that go into the mask identity come from the APPLIED filter
    config (passed straight through from action_context). Two mints differing only
    in `host_rype_reference_idx` produce DIFFERENT mask_idx; identical config (incl.
    host refs) collapses to the SAME mask_idx."""
    pool = seeded["pool"]
    common = dict(
        action_id="fastq-to-parquet",
        action_version="1.3.0",
        prep_sample_idx=seeded["prep_sample_idx"],
        originator_principal_idx=seeded["principal_idx"],
        instrument_model="NextSeq 550",
        adapter_parquet=None,
        host_minimap2_reference_idx=None,
    )
    a = await runner._mint_read_mask(pool, host_rype_reference_idx=7, **common)
    a_again = await runner._mint_read_mask(pool, host_rype_reference_idx=7, **common)
    b = await runner._mint_read_mask(pool, host_rype_reference_idx=9, **common)

    assert a["mask_idx"] == a_again["mask_idx"]  # identical config -> same mask
    assert b["mask_idx"] != a["mask_idx"]  # different applied host ref -> diff mask
    await pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
        [a["mask_idx"], b["mask_idx"]],
    )


@pytest.mark.db
async def test_mint_read_mask_adapter_bytes_drive_identity(seeded, tmp_path):
    """Different adapter-set bytes -> different mask_idx (the adapter_set_hash is
    folded into the config). Exercises `_adapter_set_hash` with real differing
    bytes (callers in other tests pass adapter_parquet=None)."""
    pool = seeded["pool"]
    adapters_a = tmp_path / "adapters_a.parquet"
    adapters_b = tmp_path / "adapters_b.parquet"
    adapters_a.write_bytes(b"adapter-set-A-bytes")
    adapters_b.write_bytes(b"adapter-set-B-bytes")

    common = dict(
        action_id="fastq-to-parquet",
        action_version="1.3.0",
        prep_sample_idx=seeded["prep_sample_idx"],
        originator_principal_idx=seeded["principal_idx"],
        instrument_model="NextSeq 550",
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
    )
    a = await runner._mint_read_mask(pool, adapter_parquet=adapters_a, **common)
    a_again = await runner._mint_read_mask(pool, adapter_parquet=adapters_a, **common)
    b = await runner._mint_read_mask(pool, adapter_parquet=adapters_b, **common)

    assert a["mask_idx"] == a_again["mask_idx"]  # same adapter bytes -> same mask
    assert b["mask_idx"] != a["mask_idx"]  # different adapter bytes -> different mask
    await pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
        [a["mask_idx"], b["mask_idx"]],
    )


@pytest.mark.db
async def test_persist_mask_idx_writes_minted_mask_onto_ticket(seeded):
    """The runner persists the minted mask_idx onto the ticket row. Mint a real
    mask via `_mint_read_mask`, then persist it via the same `_persist_mask_idx`
    the pre-loop block calls; the prep_sample-scoped work_ticket's `mask_idx`
    column equals the minted value. Re-running is idempotent (a resume re-mints
    to the same mask_idx and re-writes the same value)."""
    pool = seeded["pool"]
    principal_idx = seeded["principal_idx"]
    prep_sample_idx = seeded["prep_sample_idx"]

    action_id = "read-mask"
    version = f"mask-persist-test-{secrets.token_hex(4)}"
    await pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, "
        "  context_schema, steps, "
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, "
        "  success_status, failure_status"
        ") VALUES ($1, $2, 'prep_sample', $3::text[], $4::jsonb,"
        "  $5::jsonb, $6::jsonb, 1, 1, '1 minute', $7, $8)",
        action_id,
        version,
        ["feature:mint"],
        json.dumps({"service": False, "human_roles": ["wet_lab_admin"]}),
        json.dumps({}),
        json.dumps([]),
        "active",
        "failed",
    )
    work_ticket_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, prep_sample_idx, action_context"
        ") VALUES ($1, $2, $3, 'prep_sample', $4, '{}'::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        principal_idx,
        prep_sample_idx,
    )
    try:
        minted = await runner._mint_read_mask(
            pool,
            action_id=action_id,
            action_version=version,
            prep_sample_idx=prep_sample_idx,
            originator_principal_idx=principal_idx,
            instrument_model="NextSeq 550",
            adapter_parquet=None,
            host_rype_reference_idx=None,
            host_minimap2_reference_idx=None,
        )
        mask_idx = minted[runner.MASK_IDX_BINDING]

        # Before persist: column is NULL.
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            is None
        )

        await runner._persist_mask_idx(pool, work_ticket_idx, mask_idx)
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            == mask_idx
        )

        # Idempotent: a re-mint (same config -> same mask_idx) re-writes the same
        # value, no error.
        await runner._persist_mask_idx(pool, work_ticket_idx, mask_idx)
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            == mask_idx
        )
    finally:
        # work_ticket.mask_idx FK is ON DELETE SET NULL, but the work_ticket row
        # still references the mask_definition; drop the ticket first.
        await pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
        )
        await pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
            action_id,
            version,
        )
        await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)


@pytest.mark.db
async def test_mint_read_mask_requires_sequenced_sample(seeded):
    """A prep_sample with no sequenced_sample row is a SUBMISSION BAD_INPUT
    (the sample must be pooled before a mask can be minted)."""
    from qiita_common.backend_failure import BackendFailure

    with pytest.raises(BackendFailure, match="no sequenced_sample row"):
        await runner._mint_read_mask(
            seeded["pool"],
            action_id="fastq-to-parquet",
            action_version="1.3.0",
            prep_sample_idx=2_000_000_001,  # nonexistent
            originator_principal_idx=seeded["principal_idx"],
            instrument_model=None,
            adapter_parquet=None,
            host_rype_reference_idx=None,
            host_minimap2_reference_idx=None,
        )
