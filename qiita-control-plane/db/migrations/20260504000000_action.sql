-- migrate:up

-- =============================================================================
-- ENUM TYPES shared by qiita.action and qiita.work_ticket
-- =============================================================================
-- scope_target_kind is shared between action.target_kind (what arm of the
-- tagged union an action expects) and work_ticket.scope_target_kind (what
-- arm a given ticket carries). Living in one CREATE TYPE means the value
-- set is defined exactly once across the schema; matches the
-- qiita.system_role / qiita.tier convention used elsewhere.
--
-- Mirrored by qiita_common.models.ScopeTargetKind. The two value sets are kept
-- in lockstep by tests — change both in the same PR.
CREATE TYPE qiita.scope_target_kind AS ENUM (
    'study_prep',
    'reference',
    'prep_sample'
);


-- =============================================================================
-- ACTION (compute-orchestrator action registry)
-- =============================================================================
-- Each row is an action definition synced from a YAML file under workflows/.
-- (action_id, version) is the YAML's address and this table's primary key —
-- multiple versions of the same action_id can coexist, each its own row.
--
-- Two column tiers:
--   * YAML-authoritative — overwritten on every sync. Never hand-edited;
--     a divergence between YAML and DB is treated as the YAML being correct.
--   * DB-authoritative — never touched by sync. Operational state (enabled
--     flag, audit columns, future aggregate stats).
--
-- Sync upserts the YAML-authoritative columns by (action_id, version);
-- removing a YAML does not auto-disable a row (audit trail, in-flight
-- tickets keep working). To disable an action, an operator flips the
-- DB-authoritative `enabled` flag via qiita-admin; YAML re-add does not
-- re-enable a manually disabled row.

CREATE TABLE qiita.action (
    -- ===== PRIMARY KEY (YAML's address) =====
    action_id          TEXT NOT NULL CHECK (length(action_id) BETWEEN 1 AND 255),
    version            TEXT NOT NULL CHECK (length(version) BETWEEN 1 AND 100),

    -- ===== YAML-AUTHORITATIVE COLUMNS =====
    -- The kind of resource a work_ticket invoking this action targets. Must
    -- match the ticket's scope_target.kind at submission (route handler
    -- 422s on mismatch).
    target_kind        qiita.scope_target_kind NOT NULL,

    -- When target_kind = 'prep_sample', this list of processing_kind values
    -- declares which prep_sample subtypes the action accepts. The submit
    -- route reads the prep_sample's actual processing_kind and 422s if it's
    -- not in this list. Empty array = "any kind" (cross-kind admin
    -- actions). For non-prep_sample target_kinds, the column must be empty
    -- (enforced by action_processing_kinds_only_for_prep_sample CHECK
    -- below). qiita.processing_kind is the same ENUM qiita.prep_sample
    -- uses, defined in 20260501000011_prep_sample.sql.
    target_processing_kinds  qiita.processing_kind[] NOT NULL DEFAULT '{}',

    description        TEXT,

    -- Required scopes, AND-composed. Validated at sync time against
    -- qiita_common.auth_constants.Scope; an unknown scope string makes the
    -- deploy fail before the upsert lands.
    scopes             TEXT[] NOT NULL,

    -- { service: bool, human_roles: ["user"|"wet_lab_admin"|"system_admin"] }.
    -- Answers "may invoke," not "may execute."
    audience           JSONB NOT NULL,

    -- Per-action JSON Schema fragment validated against work_ticket.action_context
    -- at submission. Default is `{}` (accept anything). Schema is opaque to the
    -- DB; the route handler holds the validator.
    context_schema     JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Step list as JSONB. Each entry is one of:
    --   { kind: "step",   name, step_type, container, baseline_resources, target_status }
    --   { kind: "action", name, target_status, ... }  -- control-plane primitive
    -- step_type ∈ {map, reduce, singleton}. The orchestrator interprets this
    -- column; the DB does not introspect step shape beyond storing JSON.
    -- target_status (per-entry, optional) tells the runner what status to
    -- PATCH the scope_target to before the entry runs.
    steps              JSONB NOT NULL,

    -- Workflow-level status terminals. The runner PATCHes the scope_target
    -- to success_status when every entry has succeeded, and best-effort to
    -- failure_status if any entry raises. Both optional: a workflow that
    -- targets a resource without a status lifecycle leaves them NULL.
    success_status     TEXT,
    failure_status     TEXT,

    -- Action-wide resource ceilings. Resolution at submit-time clamps
    -- yaml.<dim> × profile.<dim>_mult against profile.<dim>_max AND these
    -- ceilings; the lower wins. Ceilings are mandatory so every action has
    -- a hard upper bound regardless of profile.
    cpu_ceiling        INTEGER NOT NULL CHECK (cpu_ceiling > 0),
    mem_ceiling_gb     INTEGER NOT NULL CHECK (mem_ceiling_gb > 0),
    walltime_ceiling   INTERVAL NOT NULL CHECK (walltime_ceiling > '0'::interval),
    -- gpu_ceiling=0 is the explicit "this action does not get GPU" case;
    -- raising it requires both YAML opt-in here and a profile with gpu_max>0.
    gpu_ceiling        INTEGER NOT NULL DEFAULT 0 CHECK (gpu_ceiling >= 0),

    -- ===== DB-AUTHORITATIVE COLUMNS (sync never touches) =====
    -- Disabled actions reject new submissions (route-handler check); in-flight
    -- tickets continue to completion. New rows default enabled=true.
    enabled            BOOLEAN NOT NULL DEFAULT true,

    -- First time this (action_id, version) appeared in a sync. Set on INSERT
    -- and never updated.
    first_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Audit columns mirror qiita.principal's disabled_* pattern (reason
    -- optional; at, by mandatory when disabled=false).
    disabled_at        TIMESTAMPTZ,
    disabled_reason    TEXT,
    disabled_by_idx    BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,

    -- Bumped on every UPDATE (re-sync, enable/disable) by qiita.set_updated_at().
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (action_id, version),

    CONSTRAINT action_disabled_consistent CHECK (
        (enabled = true
            AND disabled_at IS NULL
            AND disabled_reason IS NULL
            AND disabled_by_idx IS NULL)
        OR
        (enabled = false
            AND disabled_at IS NOT NULL
            AND disabled_by_idx IS NOT NULL)
    ),

    -- target_processing_kinds is only meaningful when target_kind =
    -- 'prep_sample'; for other scope kinds (reference, study_prep) the
    -- column must be empty. Catches a YAML that declares
    -- target_processing_kinds against a non-prep_sample target_kind —
    -- such a declaration would silently no-op at the route layer
    -- (which only consults the list for prep_sample-scoped submissions),
    -- so reject it at sync time.
    CONSTRAINT action_processing_kinds_only_for_prep_sample CHECK (
        target_kind = 'prep_sample'
        OR cardinality(target_processing_kinds) = 0
    )
);

COMMENT ON TABLE qiita.action IS
    'Action registry — synced from workflows/<action_id>/<version>.yaml at deploy '
    'time. YAML-authoritative columns overwrite on every sync; DB-authoritative '
    'columns (enabled, audit, stats) are never touched by sync.';

-- Submission path filters by enabled+target_kind; partial index on enabled=true
-- because disabled rows are the rare case (admin override) and we never want
-- to scan them when looking for live actions.
CREATE INDEX action_enabled_target_kind_idx
    ON qiita.action (target_kind)
    WHERE enabled = true;

CREATE TRIGGER action_set_updated_at
    BEFORE UPDATE ON qiita.action
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- migrate:down

DROP TRIGGER IF EXISTS action_set_updated_at ON qiita.action;
DROP TABLE IF EXISTS qiita.action;
DROP TYPE IF EXISTS qiita.scope_target_kind;
