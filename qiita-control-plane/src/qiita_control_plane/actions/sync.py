"""Upsert ActionDefinition rows into qiita.action.

Only YAML-authoritative columns are written via the upsert. DB-authoritative
state (`enabled`, `first_seen_at`, `disabled_*`) is preserved across syncs
except for the two reconciliation passes below:

- **Re-enable** any version of a synced action_id whose row is currently
  disabled with `disabled_reason='auto-deprecate-sync'`. This is the
  `git revert` → re-sync path: a previously auto-deprecated row reappears
  on disk and becomes enabled again. Operator manual disables (any other
  `disabled_reason`) are NOT touched.
- **Auto-deprecate** any *other* version of the same action_id that is
  currently enabled. This is the bump path: adding `1.1.0` to a directory
  that already has `1.0.0` in the DB leaves only `1.1.0` enabled. The
  `enabled = true` WHERE filter keeps re-syncs idempotent and out-of-band
  manual disables untouched.

The whole batch (including reconciliation) runs in one transaction so a
partial failure leaves the catalog at its previous state — better than
half-applied YAML.
"""

import json

import asyncpg
from qiita_common.actions import NATIVE_MODULE_PREFIX, ActionDefinition
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX

from .context_validator import check_schema

# Sentinel that distinguishes sync-driven disables from out-of-band
# manual disables. Re-enable and auto-deprecate UPDATEs filter on this
# exact value: a row whose `disabled_reason` is anything else (a free
# text reason from a future admin-disable CLI, or NULL) is treated as
# manually disabled and left alone. Tests import this symbol so a
# rename here doesn't silently break the manual-disable guarantee.
AUTO_DEPRECATE_REASON: str = "auto-deprecate-sync"

# `xmax = 0 AS inserted` is the canonical PostgreSQL upsert-discrimination
# trick: a freshly-inserted row has xmax=0 (no deletion txn), an
# UPDATE-on-conflict row gets xmax set to the current txn id. Lets us
# return inserted/updated counts without a second query.
_UPSERT_SQL = """
INSERT INTO qiita.action (
    action_id, version, target_kind, description,
    scopes, audience, context_schema, steps,
    cpu_ceiling, mem_ceiling_gb, walltime_ceiling, gpu_ceiling,
    success_status, failure_status
)
VALUES (
    $1, $2, $3, $4,
    $5, $6::jsonb, $7::jsonb, $8::jsonb,
    $9, $10, $11, $12,
    $13, $14
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
    gpu_ceiling      = EXCLUDED.gpu_ceiling,
    success_status   = EXCLUDED.success_status,
    failure_status   = EXCLUDED.failure_status
RETURNING xmax = 0 AS inserted
"""


# Re-enable a row that was previously disabled by a prior sync. The
# `disabled_reason = $3` filter (bound to AUTO_DEPRECATE_REASON)
# guarantees we only clear rows we set ourselves — manual disables
# (different reason or NULL) are left alone. Idempotent: a no-op on a
# row that's already enabled (the `enabled = false` predicate excludes
# it).
_RE_ENABLE_SQL = """
UPDATE qiita.action
   SET enabled         = true,
       disabled_at     = NULL,
       disabled_reason = NULL,
       disabled_by_idx = NULL
 WHERE action_id = $1
   AND version = $2
   AND enabled = false
   AND disabled_reason = $3
"""

# Auto-deprecate every other version of this action_id. The
# `enabled = true` filter keeps re-syncs idempotent and out-of-band
# manual disables untouched (they're already disabled; this UPDATE
# skips them so their `disabled_by_idx` attribution stays on the human
# who turned them off).
_AUTO_DEPRECATE_OTHERS_SQL = """
UPDATE qiita.action
   SET enabled         = false,
       disabled_at     = NOW(),
       disabled_reason = $4,
       disabled_by_idx = $3
 WHERE action_id = $1
   AND version != $2
   AND enabled = true
"""


def _validate_native_module_prefixes(actions: list[ActionDefinition]) -> None:
    """Refuse to sync if any step declares a `module:` path outside
    `NATIVE_MODULE_PREFIX`. Pure string check — the control plane does
    not have the orchestrator's deps and cannot import the module to
    verify it exists; that's the orchestrator's boot scan's job.

    Runs before the transaction opens so a typo'd YAML can't half-apply.
    """
    errors = []
    for a in actions:
        for entry in a.steps:
            module = getattr(entry, "module", None)
            if module is not None and not module.startswith(NATIVE_MODULE_PREFIX):
                errors.append(
                    f"  action {a.action_id!r}/{a.version!r} step {entry.name!r}:"
                    f" module={module!r} must start with {NATIVE_MODULE_PREFIX!r}"
                )
    if errors:
        raise ValueError(
            f"native step modules must live under {NATIVE_MODULE_PREFIX!r}:\n" + "\n".join(errors)
        )


async def sync_actions(
    conn: asyncpg.Connection,
    actions: list[ActionDefinition],
) -> dict[str, int]:
    """Upsert each ActionDefinition; reconcile `enabled` per the module
    docstring. Returns {"inserted": N, "updated": M} for the upsert
    counts only — the reconciliation passes don't contribute.

    asyncpg auto-converts datetime.timedelta to INTERVAL, so walltime fields
    pass through without manual encoding. JSONB columns get pre-encoded as
    JSON strings and cast `::jsonb` in the SQL — avoids needing a per-conn
    type codec registration just for sync.
    """
    # Fail-fast outside the transaction: a typo'd module path should not
    # open and roll back a Postgres transaction.
    _validate_native_module_prefixes(actions)

    inserted = 0
    updated = 0
    async with conn.transaction():
        for a in actions:
            # Refuse to persist a malformed schema. Letting it through
            # would surface as a runtime 500 on the first submission;
            # catching it here makes it a deploy-time error the operator
            # sees while syncing YAML.
            check_schema(a.context_schema)

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
                a.success_status,
                a.failure_status,
            )
            if row["inserted"]:
                inserted += 1
            else:
                updated += 1

            # Re-enable this row if a prior sync auto-deprecated it (the
            # git-revert → re-sync path). No-op for freshly-inserted rows
            # and for rows currently in any other disabled state.
            await conn.execute(_RE_ENABLE_SQL, a.action_id, a.version, AUTO_DEPRECATE_REASON)

            # Auto-deprecate every other version of this action_id.
            await conn.execute(
                _AUTO_DEPRECATE_OTHERS_SQL,
                a.action_id,
                a.version,
                SYSTEM_PRINCIPAL_IDX,
                AUTO_DEPRECATE_REASON,
            )
    return {"inserted": inserted, "updated": updated}
