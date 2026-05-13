"""Tests for qiita_control_plane.actions.sync.

Two concerns:
- The sync-time `check_schema` gate: a malformed `context_schema` is
  rejected before any row is upserted. Pure unit test (no DB needed
  because the gate runs inside the loop body, before the SQL).
- DB-level happy path is exercised by the loader/library tests; not
  duplicated here.
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from qiita_common.actions import (
    ActionCeiling,
    ActionDefinition,
    Audience,
)
from qiita_common.models import ScopeTargetKind, StepType

from qiita_control_plane.actions.context_validator import SchemaError
from qiita_control_plane.actions.sync import sync_actions


def _build_action(*, context_schema: dict) -> ActionDefinition:
    """Minimal ActionDefinition with the given context_schema."""
    return ActionDefinition(
        action_id="test-action",
        version="1.0",
        target_kind=ScopeTargetKind.REFERENCE,
        scopes=["reference:write"],
        audience=Audience(service=False, human_roles=["system_admin"]),
        context_schema=context_schema,
        steps=[
            {
                "kind": "step",
                "name": "noop",
                "step_type": StepType.SINGLETON,
                "container": "qiita/noop:1.0.0",
                "baseline_resources": {
                    "cpu": 1,
                    "mem_gb": 1,
                    "walltime": timedelta(minutes=1),
                },
            }
        ],
        action_ceiling=ActionCeiling(cpu=1, mem_gb=1, walltime=timedelta(minutes=1), gpu=0),
    )


class _FakeConn:
    """asyncpg.Connection-shaped stub. `transaction()` returns an async
    context manager; `fetchrow` and `execute` record calls so tests can
    assert what SQL did or did not run.

    Note: real behavior of the re-enable and auto-deprecate UPDATE
    statements is exercised by the DB-marked integration tests under
    tests/test_actions_sync_db.py."""

    def __init__(self):
        self.fetchrow = AsyncMock(return_value={"inserted": True})
        self.execute = AsyncMock(return_value="UPDATE 0")
        self._transaction = MagicMock()
        self._transaction.__aenter__ = AsyncMock(return_value=None)
        self._transaction.__aexit__ = AsyncMock(return_value=None)

    def transaction(self):
        return self._transaction


async def test_sync_actions_rejects_malformed_schema_before_any_write():
    """A bad context_schema raises SchemaError before fetchrow runs;
    the transaction body unwinds without committing anything."""
    bad_action = _build_action(context_schema={"type": "this-is-not-a-real-type"})
    conn = _FakeConn()

    with pytest.raises(SchemaError):
        await sync_actions(conn, [bad_action])

    # No upsert or reconciliation SQL was issued.
    assert conn.fetchrow.await_count == 0
    assert conn.execute.await_count == 0


async def test_sync_actions_accepts_valid_schema():
    """Sanity check: a well-formed schema reaches the upsert layer."""
    good_action = _build_action(
        context_schema={
            "type": "object",
            "properties": {"sample_count": {"type": "integer"}},
        }
    )
    conn = _FakeConn()

    result = await sync_actions(conn, [good_action])
    assert result == {"inserted": 1, "updated": 0}
    # One upsert (fetchrow) + two reconciliation UPDATEs (execute):
    # one re-enable, one auto-deprecate-others.
    assert conn.fetchrow.await_count == 1
    assert conn.execute.await_count == 2


async def test_sync_actions_rejects_bad_module_prefix_before_any_write():
    """A step with a module path outside NATIVE_MODULE_PREFIX is rejected
    before the transaction opens — fail-fast before any DB write."""
    bad_action = ActionDefinition(
        action_id="test-action",
        version="1.0",
        target_kind=ScopeTargetKind.REFERENCE,
        scopes=["reference:write"],
        audience=Audience(service=False, human_roles=["system_admin"]),
        context_schema={},
        steps=[
            {
                "kind": "step",
                "name": "native",
                "step_type": StepType.SINGLETON,
                # Wrong prefix — should be qiita_compute_orchestrator.jobs.*.
                "module": "os.system",
                "baseline_resources": {
                    "cpu": 1,
                    "mem_gb": 1,
                    "walltime": timedelta(minutes=1),
                },
            }
        ],
        action_ceiling=ActionCeiling(cpu=1, mem_gb=1, walltime=timedelta(minutes=1), gpu=0),
    )
    conn = _FakeConn()

    with pytest.raises(ValueError, match="qiita_compute_orchestrator.jobs"):
        await sync_actions(conn, [bad_action])

    assert conn.fetchrow.await_count == 0
    assert conn.execute.await_count == 0
