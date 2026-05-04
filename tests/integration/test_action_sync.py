"""Integration tests for the action-registry sync — end-to-end YAML→DB
upsert that verifies the YAML-authoritative vs DB-authoritative split."""

from datetime import timedelta

import pytest


_REFERENCE_ADD_YAML = """\
action_id: reference-add
version: 1.0.0
target_kind: reference
description: Hash, mint features, write membership, load reference data.
scopes: [feature:mint, reference:write]
audience:
  service: false
  human_roles: [wet_lab_admin, system_admin]
steps:
  - step: hash
    step_type: singleton
    container: qiita/reference-hash:1.0.0
    baseline_resources: {cpu: 4, mem_gb: 8, walltime: PT1H}
  - action: mint-features
  - action: write-membership
action_ceiling: {cpu: 16, mem_gb: 64, walltime: PT4H, gpu: 0}
"""


@pytest.fixture
async def workflows_dir(tmp_path):
    """Materialize a single reference-add YAML inside a temp workflows dir."""
    d = tmp_path / "workflows"
    (d / "reference-add").mkdir(parents=True)
    (d / "reference-add" / "1.0.0.yaml").write_text(_REFERENCE_ADD_YAML)
    return d


@pytest.fixture
async def clean_action_table(postgres_pool):
    """Truncate qiita.action before and after each test in this module so
    runs don't bleed into each other. Cascades through work_ticket FK by
    truncating it first."""
    async with postgres_pool.acquire() as conn:
        await conn.execute("TRUNCATE qiita.work_ticket, qiita.action")
    yield
    async with postgres_pool.acquire() as conn:
        await conn.execute("TRUNCATE qiita.work_ticket, qiita.action")


async def test_sync_inserts_new_action(
    postgres_pool, workflows_dir, clean_action_table
):
    """First sync of a YAML inserts the row with enabled=true and stamps
    first_seen_at."""
    from qiita_control_plane.actions import load_actions, sync_actions

    actions = load_actions(workflows_dir)
    assert len(actions) == 1

    async with postgres_pool.acquire() as conn:
        result = await sync_actions(conn, actions)
        assert result == {"inserted": 1, "updated": 0}

        row = await conn.fetchrow(
            "SELECT * FROM qiita.action WHERE action_id=$1 AND version=$2",
            "reference-add",
            "1.0.0",
        )
    assert row["target_kind"] == "reference"
    assert sorted(row["scopes"]) == ["feature:mint", "reference:write"]
    assert row["enabled"] is True
    assert row["first_seen_at"] is not None
    assert row["disabled_at"] is None
    # Resource ceilings unpacked from the action_ceiling sub-object.
    assert row["cpu_ceiling"] == 16
    assert row["mem_ceiling_gb"] == 64
    assert row["walltime_ceiling"] == timedelta(hours=4)
    assert row["gpu_ceiling"] == 0


async def test_sync_updates_yaml_authoritative_columns_only(
    postgres_pool, workflows_dir, clean_action_table
):
    """Re-sync after a YAML edit overwrites YAML-authoritative columns and
    leaves DB-authoritative state alone — the load-bearing invariant of B7."""
    from qiita_control_plane.actions import load_actions, sync_actions

    # First sync — establish the row.
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, load_actions(workflows_dir))
        first_seen = await conn.fetchval(
            "SELECT first_seen_at FROM qiita.action WHERE action_id=$1 AND version=$2",
            "reference-add",
            "1.0.0",
        )
        # Operator manually disables the action between syncs.
        await conn.execute(
            """
            UPDATE qiita.action
               SET enabled = false,
                   disabled_at = now(),
                   disabled_by_idx = 1,
                   disabled_reason = 'incident-1234'
             WHERE action_id=$1 AND version=$2
            """,
            "reference-add",
            "1.0.0",
        )

    # Second sync after a YAML edit (description + bumped ceiling).
    edited = (workflows_dir / "reference-add" / "1.0.0.yaml").read_text()
    edited = edited.replace(
        "description: Hash, mint features, write membership, load reference data.",
        "description: Edited description.",
    )
    edited = edited.replace("cpu: 16", "cpu: 24")
    (workflows_dir / "reference-add" / "1.0.0.yaml").write_text(edited)

    async with postgres_pool.acquire() as conn:
        result = await sync_actions(conn, load_actions(workflows_dir))
        assert result == {"inserted": 0, "updated": 1}

        row = await conn.fetchrow(
            "SELECT * FROM qiita.action WHERE action_id=$1 AND version=$2",
            "reference-add",
            "1.0.0",
        )

    # YAML-authoritative columns reflect the edit.
    assert row["description"] == "Edited description."
    assert row["cpu_ceiling"] == 24

    # DB-authoritative state is preserved across re-sync.
    assert row["enabled"] is False
    assert row["disabled_at"] is not None
    assert row["disabled_by_idx"] == 1
    assert row["disabled_reason"] == "incident-1234"
    assert row["first_seen_at"] == first_seen


async def test_sync_is_idempotent(postgres_pool, workflows_dir, clean_action_table):
    """Re-running sync without a YAML change reports zero inserts and one
    update; calling N times converges deterministically."""
    from qiita_control_plane.actions import load_actions, sync_actions

    actions = load_actions(workflows_dir)
    async with postgres_pool.acquire() as conn:
        first = await sync_actions(conn, actions)
        second = await sync_actions(conn, actions)
        third = await sync_actions(conn, actions)

    assert first == {"inserted": 1, "updated": 0}
    assert second == {"inserted": 0, "updated": 1}
    assert third == {"inserted": 0, "updated": 1}


async def test_sync_round_trips_through_action_definition(
    postgres_pool, workflows_dir, clean_action_table
):
    """The DB row reconstructs back into ActionDefinition cleanly — verifies
    the JSONB columns (audience, context_schema, steps) preserve the
    discriminated-union shape and the timedelta walltime."""
    from qiita_common.actions import ActionDefinition
    from qiita_control_plane.actions import load_actions, sync_actions

    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, load_actions(workflows_dir))
        row = await conn.fetchrow(
            "SELECT * FROM qiita.action WHERE action_id=$1 AND version=$2",
            "reference-add",
            "1.0.0",
        )

    # Reconstruct: the DB row's columns map to the YAML/Pydantic shape.
    import json

    reconstructed = ActionDefinition.model_validate(
        {
            "action_id": row["action_id"],
            "version": row["version"],
            "target_kind": row["target_kind"],
            "description": row["description"],
            "scopes": list(row["scopes"]),
            "audience": json.loads(row["audience"]),
            "context_schema": json.loads(row["context_schema"]),
            # `steps` round-trips through the discriminator form (kind/name);
            # the model_validator(mode="before") shorthand-rewrite handles
            # both forms, so passing kind+name works without rewriting back
            # to step:/action: shorthand.
            "steps": json.loads(row["steps"]),
            "action_ceiling": {
                "cpu": row["cpu_ceiling"],
                "mem_gb": row["mem_ceiling_gb"],
                "walltime": row["walltime_ceiling"],
                "gpu": row["gpu_ceiling"],
            },
        }
    )

    assert reconstructed.action_id == "reference-add"
    assert reconstructed.audience.human_roles[0].value == "wet_lab_admin"
    assert reconstructed.steps[0].name == "hash"
    assert reconstructed.steps[0].baseline_resources.walltime == timedelta(hours=1)
    assert reconstructed.steps[1].name == "mint-features"
    assert reconstructed.action_ceiling.walltime == timedelta(hours=4)


async def test_sync_transaction_rolls_back_on_failure(
    postgres_pool, workflows_dir, clean_action_table
):
    """A FK-violating row inside the batch must roll back the whole batch —
    no partial state. We force this by closing the connection mid-sync via
    a manual abort."""
    from qiita_common.actions import ActionDefinition
    from qiita_control_plane.actions import load_actions, sync_actions

    actions = load_actions(workflows_dir)
    # Synthesize a second action that will fail the CHECK on cpu_ceiling
    # by reaching into the model post-construction (Pydantic prevents this
    # at construction). Easier: create one valid + one invalid via raw asyncpg
    # call inside a transaction and confirm the first one's row didn't land.
    bad = ActionDefinition.model_validate(
        {
            "action_id": "needs-rollback",
            "version": "0.0.1",
            "target_kind": "reference",
            "scopes": [],
            "audience": {"service": True, "human_roles": []},
            "steps": [
                {
                    "step": "x",
                    "step_type": "singleton",
                    "container": "img:1",
                    "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
                }
            ],
            "action_ceiling": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
        }
    )

    # Now monkey-patch its mem_ceiling reference to something that violates
    # the CHECK at the DB level (mem_ceiling_gb > 0). Pydantic forbids
    # gt=0 at construction, so we bypass by post-set:
    object.__setattr__(bad.action_ceiling, "mem_gb", 0)

    async with postgres_pool.acquire() as conn:
        with pytest.raises(Exception):  # noqa: BLE001 — asyncpg.CheckViolationError or similar
            await sync_actions(conn, [actions[0], bad])

        rowcount = await conn.fetchval("SELECT count(*) FROM qiita.action")
    assert rowcount == 0, "transaction should have rolled back the entire batch"


async def test_sync_handles_empty_action_list(postgres_pool, clean_action_table):
    """Calling sync with an empty list is a no-op (no transaction, no error)."""
    from qiita_control_plane.actions import sync_actions

    async with postgres_pool.acquire() as conn:
        result = await sync_actions(conn, [])
        assert result == {"inserted": 0, "updated": 0}
