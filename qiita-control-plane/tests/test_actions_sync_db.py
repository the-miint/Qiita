"""DB-bound tests for qiita_control_plane.actions.sync — re-enable +
auto-deprecate reconciliation.

The pure-unit tests in test_actions_sync.py cover the prefix and schema
validators (which fire before any SQL). Here we drive sync against a
real Postgres so the WHERE-clause invariants in the reconciliation
UPDATEs are exercised:

- New action_id+version row → enabled=true.
- Bump path (1.0.0 → 1.1.0) → 1.0.0 auto-deprecated, 1.1.0 enabled.
- Revert path (1.1.0 gone, only 1.0.0 on disk) → 1.0.0 re-enabled,
  1.1.0 auto-deprecated.
- Manual disable (any disabled_reason ≠ AUTO_DEPRECATE_REASON) is not
  clobbered by re-sync — attribution stays intact.
"""

from __future__ import annotations

import json
import uuid
from datetime import timedelta

import pytest
from qiita_common.actions import (
    ActionCeiling,
    ActionDefinition,
    Audience,
)
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX
from qiita_common.models import ScopeTargetKind, StepType

from qiita_control_plane.actions.sync import AUTO_DEPRECATE_REASON, sync_actions

pytestmark = pytest.mark.db


def _build_action(*, action_id: str, version: str) -> ActionDefinition:
    """Minimal valid ActionDefinition parameterized by action_id + version."""
    return ActionDefinition(
        action_id=action_id,
        version=version,
        target_kind=ScopeTargetKind.REFERENCE,
        scopes=["reference:write"],
        audience=Audience(service=False, human_roles=["system_admin"]),
        context_schema={},
        steps=[
            {
                "kind": "step",
                "name": "noop",
                "step_type": StepType.SINGLETON,
                "container": "qiita/noop:1.0.0",
                "entrypoint": "/opt/qiita/noop.sh",
                "baseline_resources": {
                    "cpu": 1,
                    "mem_gb": 1,
                    "walltime": timedelta(minutes=1),
                },
            }
        ],
        action_ceiling=ActionCeiling(cpu=1, mem_gb=1, walltime=timedelta(minutes=1), gpu=0),
    )


async def _action_row(postgres_pool, action_id: str, version: str) -> dict:
    """Read the enabled / disabled_* flags for assertions."""
    row = await postgres_pool.fetchrow(
        "SELECT enabled, disabled_at, disabled_reason, disabled_by_idx"
        " FROM qiita.action WHERE action_id = $1 AND version = $2",
        action_id,
        version,
    )
    assert row is not None, f"action {action_id!r}/{version!r} missing from DB"
    return dict(row)


@pytest.fixture
async def fresh_action_id(postgres_pool):
    """Unique action_id per test + teardown to clear all of its versions.
    Keeps tests independent without manual cleanup at every call site."""
    aid = f"sync-test-{uuid.uuid4()}"
    yield aid
    await postgres_pool.execute("DELETE FROM qiita.action WHERE action_id = $1", aid)


async def test_sync_inserts_new_action_enabled(postgres_pool, fresh_action_id):
    """A fresh action_id+version is inserted with enabled=true; the
    reconciliation passes don't disturb the happy path."""
    async with postgres_pool.acquire() as conn:
        result = await sync_actions(
            conn, [_build_action(action_id=fresh_action_id, version="1.0.0")]
        )

    assert result == {"inserted": 1, "updated": 0}
    row = await _action_row(postgres_pool, fresh_action_id, "1.0.0")
    assert row["enabled"] is True
    assert row["disabled_at"] is None
    assert row["disabled_reason"] is None
    assert row["disabled_by_idx"] is None


async def test_sync_auto_deprecates_other_versions(postgres_pool, fresh_action_id):
    """Bump path: 1.0.0 is the only version on disk; later, 1.1.0
    replaces it. After the second sync, 1.0.0 is auto-deprecated with
    SYSTEM_PRINCIPAL_IDX attribution and 1.1.0 is enabled."""
    # Seed: 1.0.0 only.
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, [_build_action(action_id=fresh_action_id, version="1.0.0")])
    assert (await _action_row(postgres_pool, fresh_action_id, "1.0.0"))["enabled"] is True

    # Bump: directory now has 1.1.0 only.
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, [_build_action(action_id=fresh_action_id, version="1.1.0")])

    old = await _action_row(postgres_pool, fresh_action_id, "1.0.0")
    assert old["enabled"] is False
    assert old["disabled_at"] is not None
    assert old["disabled_reason"] == AUTO_DEPRECATE_REASON
    assert old["disabled_by_idx"] == SYSTEM_PRINCIPAL_IDX

    new = await _action_row(postgres_pool, fresh_action_id, "1.1.0")
    assert new["enabled"] is True
    assert new["disabled_at"] is None
    assert new["disabled_reason"] is None
    assert new["disabled_by_idx"] is None


async def test_sync_re_enables_previously_auto_deprecated(postgres_pool, fresh_action_id):
    """Revert path: 1.0.0 was bumped to 1.1.0 (auto-deprecating 1.0.0);
    reverting the YAML brings 1.0.0 back on disk only. Re-sync
    re-enables 1.0.0 and auto-deprecates 1.1.0."""
    # Seed → bump.
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, [_build_action(action_id=fresh_action_id, version="1.0.0")])
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, [_build_action(action_id=fresh_action_id, version="1.1.0")])
    assert (await _action_row(postgres_pool, fresh_action_id, "1.0.0"))["enabled"] is False

    # Revert: directory has 1.0.0 only again.
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, [_build_action(action_id=fresh_action_id, version="1.0.0")])

    revived = await _action_row(postgres_pool, fresh_action_id, "1.0.0")
    assert revived["enabled"] is True
    assert revived["disabled_at"] is None
    assert revived["disabled_reason"] is None
    assert revived["disabled_by_idx"] is None

    now_disabled = await _action_row(postgres_pool, fresh_action_id, "1.1.0")
    assert now_disabled["enabled"] is False
    assert now_disabled["disabled_reason"] == AUTO_DEPRECATE_REASON
    assert now_disabled["disabled_by_idx"] == SYSTEM_PRINCIPAL_IDX


async def test_sync_does_not_clobber_manual_disable(postgres_pool, fresh_action_id):
    """A row manually disabled out-of-band (disabled_reason != the
    sync sentinel) is left alone by re-sync. The re-enable UPDATE's
    WHERE filter excludes it; the auto-deprecate-others UPDATE doesn't
    touch it either because it's already enabled=false."""
    # Seed a manually-disabled row directly. SYSTEM_PRINCIPAL_IDX is
    # a convenient existing principal for the FK (the test's point is
    # the reason-string filter, not which principal).
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience,"
        "  context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling,"
        "  enabled, disabled_at, disabled_reason, disabled_by_idx"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb,"
        "  $5::jsonb, $6::jsonb, 1, 1, '1 minute',"
        "  false, NOW(), 'broken in production', $7)",
        fresh_action_id,
        "1.0.0",
        ["reference:write"],
        json.dumps({"service": False, "human_roles": ["system_admin"]}),
        json.dumps({}),
        json.dumps([]),
        SYSTEM_PRINCIPAL_IDX,
    )

    # Re-sync with the matching YAML.
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, [_build_action(action_id=fresh_action_id, version="1.0.0")])

    row = await _action_row(postgres_pool, fresh_action_id, "1.0.0")
    # Reason mismatch → re-enable filter skips → row stays disabled.
    assert row["enabled"] is False
    assert row["disabled_reason"] == "broken in production"
    assert row["disabled_by_idx"] == SYSTEM_PRINCIPAL_IDX
