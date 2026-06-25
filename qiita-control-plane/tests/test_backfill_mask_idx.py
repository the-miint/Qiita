"""Tests for the one-time `work_ticket.mask_idx` backfill.

`backfill_work_ticket_mask_idx` populates `mask_idx` for existing read-mask /
fastq-to-parquet tickets created before the column existed. For each ticket it
reconstructs the mint-time filtering config via the SAME `_build_mask_params`
shape, computes the canonical-JSON SHA-256, and LOOKS IT UP in mask_definition
(`lookup_mask_idx_by_params`) — never minting. A hit populates the column; a miss
(failed-before-mint / drifted config) is skipped and reported.

These tests pass `default_adapter_reference_idx=None` so the backfill derives
`adapter_set_hash=None` without touching the data plane — matching a mint with
`adapter_parquet=None`. They are db-marked: they seed a prep_sample +
sequenced_sample + action + work_ticket and a real mask_definition row.
"""

import json
import secrets

import pytest
import pytest_asyncio

from qiita_control_plane import runner
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)

# No live data plane in these tests; the adapter set is configured-absent so the
# backfill derives adapter_set_hash=None (matching a mint with adapter_parquet=None).
_NO_ADAPTER_BACKFILL = dict(
    default_adapter_reference_idx=None,
    data_plane_url="grpc://unused:0",
    hmac_secret=b"unused-secret-key-16b",
)

# A >=16-byte key so the signed Flight ticket _resolve_qc_adapters builds is the
# shape the (stubbed-away) data plane would accept; the stub never verifies it,
# but keeping it well-formed mirrors a real run.
_ADAPTER_HMAC_SECRET = b"adapter-backfill-secret-key-32bytes!"

# Fixed (feature_idx, chunk_index, chunk_data) rows the stubbed DoGet returns for
# BOTH the mint and the backfill, so `_write_adapter_parquet` reassembles
# byte-identical adapter Parquets on both sides and `_adapter_set_hash` agrees.
_STUB_ADAPTER_CHUNKS = [
    (101, 0, "ACGT"),
    (101, 1, "TTAA"),
    (102, 0, "GGCCGGCC"),
]


async def _seed_action(pool, action_id: str, version: str) -> None:
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


async def _seed_work_ticket(
    pool,
    *,
    action_id: str,
    version: str,
    principal_idx: int,
    prep_sample_idx: int,
    action_context: dict,
    state: str = "failed",
) -> int:
    # state='failed' requires the failure_* columns (work_ticket_failure_consistent
    # CHECK) — set the submission-stage shape the ~300+ recovery targets carry so
    # the seeded row mirrors a real legacy failed ticket. Non-failed states leave
    # them NULL (the same CHECK forbids them set).
    if state == "failed":
        failure_type, failure_stage, failure_reason = (
            "permanent",
            "step_run",
            "read_mask parquet not found",
        )
        failure_step_name = "persist-read-metrics"
    else:
        failure_type = failure_stage = failure_reason = failure_step_name = None
    return await pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, prep_sample_idx, action_context, state,"
        "  failure_type, failure_stage, failure_step_name, failure_reason"
        ") VALUES ($1, $2, $3, 'prep_sample', $4, $5::jsonb, $6, $7, $8, $9, $10)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        principal_idx,
        prep_sample_idx,
        json.dumps(action_context),
        state,
        failure_type,
        failure_stage,
        failure_step_name,
        failure_reason,
    )


@pytest_asyncio.fixture
async def seeded(postgres_pool):
    """principal + biosample + sequenced prep_sample + sequenced_sample; yields
    ids and FK-reverse cleans up."""
    principal_idx = await seed_user_principal(postgres_pool, prefix="backfill", suffix="owner")
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
async def test_backfill_populates_matching_ticket(seeded, tmp_path):
    """A ticket whose config matches an already-minted mask gets its NULL mask_idx
    populated to that mask. A second run is a no-op (idempotent)."""
    pool = seeded["pool"]
    principal_idx = seeded["principal_idx"]
    prep_sample_idx = seeded["prep_sample_idx"]
    action_id = "read-mask"
    version = f"backfill-{secrets.token_hex(4)}"
    action_context = {"instrument_model": "NextSeq 550"}

    await _seed_action(pool, action_id, version)
    work_ticket_idx = await _seed_work_ticket(
        pool,
        action_id=action_id,
        version=version,
        principal_idx=principal_idx,
        prep_sample_idx=prep_sample_idx,
        action_context=action_context,
    )
    # Mint the mask the way the runner would for this config (adapter_parquet=None
    # -> adapter_set_hash=None, matching the backfill's no-adapter mode).
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
    try:
        # Precondition: ticket's mask_idx is NULL (it was created before the column).
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            is None
        )

        report = await runner.backfill_work_ticket_mask_idx(
            pool, workspace=tmp_path, apply=True, **_NO_ADAPTER_BACKFILL
        )
        assert report["applied"] is True
        assert {"work_ticket_idx": work_ticket_idx, "mask_idx": mask_idx} in report[
            "populated_detail"
        ]
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            == mask_idx
        )

        # Idempotent: a second apply finds nothing left to populate (mask_idx
        # already set, scoped to IS NULL), and the column is unchanged.
        report2 = await runner.backfill_work_ticket_mask_idx(
            pool, workspace=tmp_path, apply=True, **_NO_ADAPTER_BACKFILL
        )
        assert work_ticket_idx not in [d["work_ticket_idx"] for d in report2["populated_detail"]]
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            == mask_idx
        )
    finally:
        await pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
        )
        await pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
        )
        await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)


@pytest.mark.db
async def test_backfill_skips_ticket_with_no_matching_mask(seeded, tmp_path):
    """A ticket whose reconstructed config matches NO mask_definition row (failed
    before minting, or drifted config) is skipped and reported — never crashed,
    never minted. The column stays NULL."""
    pool = seeded["pool"]
    principal_idx = seeded["principal_idx"]
    prep_sample_idx = seeded["prep_sample_idx"]
    action_id = "read-mask"
    version = f"backfill-nomask-{secrets.token_hex(4)}"
    # An instrument model for which no mask was ever minted -> no matching hash.
    action_context = {"instrument_model": f"Unminted-{secrets.token_hex(4)}"}

    await _seed_action(pool, action_id, version)
    work_ticket_idx = await _seed_work_ticket(
        pool,
        action_id=action_id,
        version=version,
        principal_idx=principal_idx,
        prep_sample_idx=prep_sample_idx,
        action_context=action_context,
    )
    try:
        report = await runner.backfill_work_ticket_mask_idx(
            pool, workspace=tmp_path, apply=True, **_NO_ADAPTER_BACKFILL
        )
        assert work_ticket_idx in report["skipped_no_mask"]
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            is None
        )
    finally:
        await pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
        )
        await pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
        )


@pytest_asyncio.fixture
async def adapter_reference_idx(seeded):
    """An ACTIVE artifact_sequence_set reference — the canonical adapter set
    `_resolve_qc_adapters` resolves and DoGets. Its sequence chunks are supplied
    by the stubbed DoGet, not real DuckLake rows."""
    pool = seeded["pool"]
    principal_idx = seeded["principal_idx"]
    idx = await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'artifact_sequence_set', 'active', $2)"
        " RETURNING reference_idx",
        f"backfill-adapters-{secrets.token_hex(4)}",
        principal_idx,
    )
    yield idx
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


@pytest.mark.db
async def test_backfill_matches_adapter_bearing_mint(
    seeded, adapter_reference_idx, tmp_path, monkeypatch
):
    """Hash-fidelity regression on the ADAPTER-BEARING path: a mask minted with a
    NON-None adapter set must be found by a backfill that re-materializes the SAME
    adapter set. This is the one fragile round-trip (adapter DoGet ->
    `_write_adapter_parquet` -> `_adapter_set_hash`) that the other tests skip by
    using `adapter_set_hash=None`.

    Stubbing approach: `runner._do_get_reference_sequence_chunks` is the module
    seam both paths funnel through — the mint side here calls the real
    `_resolve_qc_adapters` to stage the adapter Parquet, and the backfill calls it
    again via `_materialize_backfill_adapter_set_hash`. Patching that single
    function (the lowest stable seam, isolated in the source precisely so tests can
    stub the live DoGet) makes BOTH sides reassemble byte-identical adapter Parquet
    from the same fixed chunks, so the two `_adapter_set_hash` values agree without
    a live data plane. The test then PROVES mint and backfill compute the same
    mask identity — the key regression risk if the two code paths ever drift.
    """
    pool = seeded["pool"]
    principal_idx = seeded["principal_idx"]
    prep_sample_idx = seeded["prep_sample_idx"]
    action_id = "read-mask"
    version = f"backfill-adapter-{secrets.token_hex(4)}"
    action_context = {"instrument_model": "NextSeq 550"}

    # Both the mint-side _resolve_qc_adapters call and the backfill's
    # _materialize_backfill_adapter_set_hash go through this one stub, so they hash
    # identical adapter bytes.
    def _stub_chunks(data_plane_url, ticket_bytes):
        return list(_STUB_ADAPTER_CHUNKS)

    monkeypatch.setattr(runner, "_do_get_reference_sequence_chunks", _stub_chunks)

    await _seed_action(pool, action_id, version)
    work_ticket_idx = await _seed_work_ticket(
        pool,
        action_id=action_id,
        version=version,
        principal_idx=principal_idx,
        prep_sample_idx=prep_sample_idx,
        action_context=action_context,
    )

    # Mint side: materialize the adapter Parquet exactly as the runner would
    # (through the stubbed DoGet), then mint with that path so adapter_set_hash is
    # NON-None — the case the other tests never reach.
    mint_workspace = tmp_path / "mint"
    bound = await runner._resolve_qc_adapters(
        pool,
        default_adapter_reference_idx=adapter_reference_idx,
        data_plane_url="grpc://unused:0",
        hmac_secret=_ADAPTER_HMAC_SECRET,
        workspace=mint_workspace,
    )
    adapter_parquet = bound[runner.QC_ADAPTER_BINDING]
    # Guard: this path genuinely reaches `_adapter_set_hash` with a real adapter
    # set, so the minted mask's params carry a NON-None adapter_set_hash (the case
    # the other tests, which pass adapter_parquet=None, never exercise).
    mint_adapter_hash = runner._adapter_set_hash(adapter_parquet)
    assert isinstance(mint_adapter_hash, str) and mint_adapter_hash

    minted = await runner._mint_read_mask(
        pool,
        action_id=action_id,
        action_version=version,
        prep_sample_idx=prep_sample_idx,
        originator_principal_idx=principal_idx,
        instrument_model="NextSeq 550",
        adapter_parquet=adapter_parquet,
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
    )
    mask_idx = minted[runner.MASK_IDX_BINDING]
    try:
        # Backfill side: same adapter reference -> re-materialize through the same
        # stub -> same adapter_set_hash -> same params hash -> finds the same mask.
        backfill_workspace = tmp_path / "backfill"
        report = await runner.backfill_work_ticket_mask_idx(
            pool,
            workspace=backfill_workspace,
            default_adapter_reference_idx=adapter_reference_idx,
            data_plane_url="grpc://unused:0",
            hmac_secret=_ADAPTER_HMAC_SECRET,
            apply=True,
        )
        assert {"work_ticket_idx": work_ticket_idx, "mask_idx": mask_idx} in report[
            "populated_detail"
        ], "backfill failed to reproduce the adapter-bearing mint's mask identity"
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            == mask_idx
        )
    finally:
        await pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
        )
        await pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
        )
        await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)


@pytest.mark.db
async def test_backfill_dry_run_does_not_write(seeded, tmp_path):
    """apply=False classifies the same hit but writes nothing — the column stays
    NULL and the report flags applied=False."""
    pool = seeded["pool"]
    principal_idx = seeded["principal_idx"]
    prep_sample_idx = seeded["prep_sample_idx"]
    action_id = "fastq-to-parquet"
    version = f"backfill-dry-{secrets.token_hex(4)}"
    action_context = {"instrument_model": "Illumina MiSeq"}

    await _seed_action(pool, action_id, version)
    work_ticket_idx = await _seed_work_ticket(
        pool,
        action_id=action_id,
        version=version,
        principal_idx=principal_idx,
        prep_sample_idx=prep_sample_idx,
        action_context=action_context,
        state="completed",  # a COMPLETED ticket is still processed (shared-mask guard)
    )
    minted = await runner._mint_read_mask(
        pool,
        action_id=action_id,
        action_version=version,
        prep_sample_idx=prep_sample_idx,
        originator_principal_idx=principal_idx,
        instrument_model="Illumina MiSeq",
        adapter_parquet=None,
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
    )
    mask_idx = minted[runner.MASK_IDX_BINDING]
    try:
        report = await runner.backfill_work_ticket_mask_idx(
            pool, workspace=tmp_path, apply=False, **_NO_ADAPTER_BACKFILL
        )
        assert report["applied"] is False
        # The completed ticket IS classified as a hit (processed in any state)...
        assert {"work_ticket_idx": work_ticket_idx, "mask_idx": mask_idx} in report[
            "populated_detail"
        ]
        # ...but dry-run wrote nothing.
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            is None
        )
    finally:
        await pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
        )
        await pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
        )
        await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
