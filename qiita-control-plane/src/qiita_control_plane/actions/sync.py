"""Upsert ActionDefinition rows into qiita.action.

Only YAML-authoritative columns are written. DB-authoritative state
(`enabled`, `first_seen_at`, `disabled_*`) is preserved across syncs;
re-adding a YAML for an action an operator manually disabled does NOT
re-enable it.

The whole batch runs in one transaction so a partial failure leaves the
catalog at its previous state — better than half-applied YAML.
"""

import json

import asyncpg
from qiita_common.actions import ActionDefinition

# `xmax = 0 AS inserted` is the canonical PostgreSQL upsert-discrimination
# trick: a freshly-inserted row has xmax=0 (no deletion txn), an
# UPDATE-on-conflict row gets xmax set to the current txn id. Lets us
# return inserted/updated counts without a second query.
_UPSERT_SQL = """
INSERT INTO qiita.action (
    action_id, version, target_kind, description,
    scopes, audience, context_schema, steps,
    cpu_ceiling, mem_ceiling_gb, walltime_ceiling, gpu_ceiling
)
VALUES (
    $1, $2, $3, $4,
    $5, $6::jsonb, $7::jsonb, $8::jsonb,
    $9, $10, $11, $12
)
ON CONFLICT (action_id, version) DO UPDATE SET
    target_kind      = EXCLUDED.target_kind,
    description      = EXCLUDED.description,
    scopes           = EXCLUDED.scopes,
    audience         = EXCLUDED.audience,
    context_schema   = EXCLUDED.context_schema,
    steps            = EXCLUDED.steps,
    cpu_ceiling      = EXCLUDED.cpu_ceiling,
    mem_ceiling_gb   = EXCLUDED.mem_ceiling_gb,
    walltime_ceiling = EXCLUDED.walltime_ceiling,
    gpu_ceiling      = EXCLUDED.gpu_ceiling
RETURNING xmax = 0 AS inserted
"""


async def sync_actions(
    conn: asyncpg.Connection,
    actions: list[ActionDefinition],
) -> dict[str, int]:
    """Upsert each ActionDefinition. Returns {"inserted": N, "updated": M}.

    asyncpg auto-converts datetime.timedelta to INTERVAL, so walltime fields
    pass through without manual encoding. JSONB columns get pre-encoded as
    JSON strings and cast `::jsonb` in the SQL — avoids needing a per-conn
    type codec registration just for sync.
    """
    inserted = 0
    updated = 0
    async with conn.transaction():
        for a in actions:
            audience_json = json.dumps(a.audience.model_dump(mode="json"))
            context_schema_json = json.dumps(a.context_schema)
            steps_json = json.dumps([s.model_dump(mode="json") for s in a.steps])
            row = await conn.fetchrow(
                _UPSERT_SQL,
                a.action_id,
                a.version,
                a.target_kind,
                a.description,
                a.scopes,
                audience_json,
                context_schema_json,
                steps_json,
                a.action_ceiling.cpu,
                a.action_ceiling.mem_gb,
                a.action_ceiling.walltime,
                a.action_ceiling.gpu,
            )
            if row["inserted"]:
                inserted += 1
            else:
                updated += 1
    return {"inserted": inserted, "updated": updated}
