"""End-to-end integration test for the orchestrator's
`mint_sequence_range` helper against a real control-plane HTTP route.

The orchestrator's own unit tests (qiita-compute-orchestrator/tests/
test_sequence_range.py) mock the transport, and the smoke test in
test_native_step_smoke.py monkeypatches `mint_sequence_range` itself
to bypass HTTP. Neither path proves the orchestrator's
`Settings(co_to_cp_token=...)` flows all the way through httpx into
the CP's `POST /api/v1/sequence-range` route and lands a row in
`qiita.sequence_range`. This file fills that gap.

The full path exercised here:
    Settings.co_to_cp_token
        → make_cp_client(transport=ASGITransport(app=cp_app))
        → mint_sequence_range
        → POST /api/v1/sequence-range with Bearer header
        → CP route guard (compute SA + sequence_range:mint scope)
        → qiita.mint_sequence_range() plpgsql
        → qiita.sequence_range row
        → MintedSequenceRange returned to the caller
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport
from qiita_compute_orchestrator.config import Settings
from qiita_compute_orchestrator.cp_client import make_cp_client
from qiita_compute_orchestrator.sequence_range import (
    MintedSequenceRange,
    SequenceRangeAlreadyExists,
    mint_sequence_range,
)
from qiita_control_plane.config import Settings as CPSettings
from qiita_control_plane.main import app as cp_app


@pytest.fixture
async def cp_app_with_pool(postgres_pool, hmac_secret):
    """Wire the CP FastAPI app to the integration postgres pool +
    settings so its routes work in-process under ASGITransport. Same
    setup pattern as test_e2e_reference.test_ticket_endpoint_rejects_*.
    """
    cp_app.state.pool = postgres_pool
    cp_app.state.settings = CPSettings(
        database_url="unused-in-test",
        flight_signing_key=hmac_secret,
        data_plane_url="grpc://unused:0",
    )
    yield cp_app


@pytest.fixture
async def e2e_prep_sample(postgres_pool, human_admin_session):
    """A sequenced prep_sample to mint a range against. Mirrors the
    smoke test's fixture so this file stays in sync with the
    seed-helper surface; reverse-FK cleanup runs on yield exit."""
    from qiita_control_plane.testing.db_seeds import (
        seed_biosample_with_sequenced_prep_sample,
    )

    admin_idx = human_admin_session["principal_idx"]
    biosample_idx, idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=admin_idx
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.sequence_range WHERE prep_sample_idx = $1", idx
    )
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx
    )


def _make_settings(sa_token: str) -> Settings:
    """Build an orchestrator Settings instance for the test. Settings
    is a frozen dataclass with several required fields beyond what
    `mint_sequence_range` actually reads; pass dummies for the
    backend / shared_fs / cp_to_co fields so the construction
    succeeds. `cp_url="http://test"` because httpx still uses
    base_url for relative-path resolution even when an ASGITransport
    is wired up downstream — ASGITransport ignores the host:port
    part."""
    return Settings(
        backend_type="local",
        path_scratch="/tmp/qiita-e2e-unused",
        path_derived="/tmp/qiita-e2e-unused-derived",
        cp_to_co_token="unused-in-e2e",
        cp_url="http://test",
        co_to_cp_token=sa_token,
    )


async def test_mint_sequence_range_full_path(
    postgres_pool,
    cp_app_with_pool,
    e2e_prep_sample,
    compute_worker_service_account,
):
    """Settings → make_cp_client → mint_sequence_range → CP route →
    DB. Asserts the helper returns the right MintedSequenceRange AND
    that qiita.sequence_range carries a row matching the returned
    bounds — proving the full request/response/DB-write cycle, not
    just that the helper handles a 201."""
    settings = _make_settings(compute_worker_service_account["token"])
    http = make_cp_client(
        settings=settings,
        transport=ASGITransport(app=cp_app_with_pool),
    )

    async with http:
        minted = await mint_sequence_range(
            http=http,
            prep_sample_idx=e2e_prep_sample,
            count=4,
        )

    # Helper returned a MintedSequenceRange with the expected geometry.
    assert isinstance(minted, MintedSequenceRange)
    assert minted.prep_sample_idx == e2e_prep_sample
    assert minted.sequence_idx_stop == minted.sequence_idx_start + 3  # count=4

    # DB row landed and matches what the helper handed back.
    row = await postgres_pool.fetchrow(
        "SELECT sequence_idx_start, sequence_idx_stop"
        " FROM qiita.sequence_range WHERE prep_sample_idx = $1",
        e2e_prep_sample,
    )
    assert row is not None
    assert row["sequence_idx_start"] == minted.sequence_idx_start
    assert row["sequence_idx_stop"] == minted.sequence_idx_stop


async def test_mint_sequence_range_second_attempt_raises_already_exists(
    postgres_pool,
    cp_app_with_pool,
    e2e_prep_sample,
    compute_worker_service_account,
):
    """A second mint for the same prep_sample 409s and the helper
    raises SequenceRangeAlreadyExists. Proves the 409 path also flows
    end-to-end through the real CP route (not just the helper's
    branch on the mocked status code)."""
    settings = _make_settings(compute_worker_service_account["token"])
    http = make_cp_client(
        settings=settings,
        transport=ASGITransport(app=cp_app_with_pool),
    )

    async with http:
        # First call seeds the range.
        await mint_sequence_range(http=http, prep_sample_idx=e2e_prep_sample, count=4)
        # Second call hits qiita.sequence_range's UNIQUE(prep_sample_idx) →
        # CP route returns 409 → helper raises the typed exception.
        with pytest.raises(SequenceRangeAlreadyExists) as ei:
            await mint_sequence_range(
                http=http, prep_sample_idx=e2e_prep_sample, count=4
            )
    assert ei.value.prep_sample_idx == e2e_prep_sample
    # The new recovery hint surfaces (replaces the previous "delete
    # the prep_sample" framing that contradicted the recovery runbook).
    assert "pre_minted_range" in str(ei.value)
    assert "fastq-to-parquet-retry-recovery.md" in str(ei.value)
