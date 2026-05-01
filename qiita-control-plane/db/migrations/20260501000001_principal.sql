-- migrate:up

-- =============================================================================
-- SYSTEM ROLE ENUM
-- =============================================================================
-- System-wide role hierarchy. Values are ordered such that higher roles
-- subsume lower ones: a system_admin can do everything a wet_lab_admin can do,
-- and a wet_lab_admin can do everything a user can do. Authorization
-- predicates check `system_role >= 'wet_lab_admin'` or
-- `system_role >= 'system_admin'` to gate operations by minimum required role.
CREATE TYPE qiita.system_role AS ENUM (
    'user',
    'wet_lab_admin',
    'system_admin'
);


-- =============================================================================
-- PRINCIPAL
-- =============================================================================

CREATE TABLE qiita.principal (
    idx             BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    display_name    VARCHAR(255) NOT NULL,
    system_role     qiita.system_role NOT NULL DEFAULT 'user',
    created_by_idx  BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Disabled state — temporary block primitive. The auth layer rejects login
    -- / token-use when either disabled or retired is true; only retired is
    -- terminal. Disabling does NOT auto-revoke tokens; admin can bulk-revoke
    -- separately if desired. principal_not_both_disabled_and_retired forbids
    -- the simultaneous-true case at the schema level.
    disabled         BOOLEAN NOT NULL DEFAULT false,
    disabled_at      TIMESTAMPTZ,
    disabled_by_idx  BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    disable_reason   TEXT,

    -- Retirement columns; see retired column comment.
    retired         BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx  BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at      TIMESTAMPTZ,
    retire_reason   TEXT,

    CONSTRAINT principal_retirement_consistent CHECK (
        (retired = false
            AND retired_at IS NULL
            AND retired_by_idx IS NULL
            AND retire_reason IS NULL)
        OR
        (retired = true
            AND retired_at IS NOT NULL
            AND retired_by_idx IS NOT NULL)
    ),

    CONSTRAINT principal_disabled_consistent CHECK (
        (disabled = false
            AND disabled_at IS NULL
            AND disabled_by_idx IS NULL
            AND disable_reason IS NULL)
        OR
        (disabled = true
            AND disabled_at IS NOT NULL
            AND disabled_by_idx IS NOT NULL)
    ),

    CONSTRAINT principal_not_both_disabled_and_retired
        CHECK (NOT (disabled AND retired)),

    -- The system principal (idx=1) must remain in the active state.
    -- Disabling or retiring it would block automatic system-generated audit
    -- events that record it as the actor.
    CONSTRAINT principal_system_principal_always_active
        CHECK (idx <> 1 OR (NOT disabled AND NOT retired))
);

COMMENT ON TABLE qiita.principal IS
    'Registered principals of the system.';

COMMENT ON COLUMN qiita.principal.retired IS
    'Boolean flag; when true, the principal is retired from the system. The auth layer is responsible for blocking login by retired principals. `retired_at`, `retired_by_idx`, and `retire_reason` carry the audit trail.';

COMMENT ON COLUMN qiita.principal.retired_at IS
    'Timestamp of retirement; NULL when active. Mandatory when `retired = true` per principal_retirement_consistent.';

COMMENT ON COLUMN qiita.principal.retired_by_idx IS
    'The system_admin who performed the retirement. Mandatory when `retired = true` per principal_retirement_consistent. The FK uses ON DELETE RESTRICT: a principal who has ever retired another principal cannot be hard-deleted until those retirements are reassigned or cleared at the DDL level. Principal deletion is not offered through the API, so this clause is operationally a safeguard for database-level cleanups.';

COMMENT ON COLUMN qiita.principal.retire_reason IS
    'Free-text reason supplied by the retiring admin.';

CREATE INDEX principal_display_name_idx ON qiita.principal (display_name);

-- Partial index because the active set is expected to dominate the table;
-- queries against retired principals (admin listings, audit lookups) are the
-- minority case.
CREATE INDEX principal_retired_idx
    ON qiita.principal (retired_at)
    WHERE retired = true;


-- =============================================================================
-- SYSTEM PRINCIPAL (seeded at idx=1)
-- =============================================================================
-- The system principal acts as:
--   * Backfill target for FKs that need a non-null actor when no human is
--     responsible (e.g. `qiita.references.created_by_idx` for system imports).
--   * Audit-event "actor" for system-generated events (e.g., automatic
--     token revocation on retirement, fired by a DB trigger).
-- It is neither a user nor a service_account (no subtype row exists for it,
-- enforced by CHECK (principal_idx <> 1) on each subtype). It cannot
-- authenticate. The deferred self-FK on principal.created_by_idx lets
-- created_by_idx=1 reference itself within this transaction (dbmate wraps
-- each migration in a transaction).
-- system_role='system_admin' is for audit-log self-documentation; it grants
-- no capability since the principal has no credentials.

INSERT INTO qiita.principal (idx, display_name, system_role, created_by_idx)
    OVERRIDING SYSTEM VALUE
    VALUES (1, 'system', 'system_admin', 1);
SELECT setval(pg_get_serial_sequence('qiita.principal', 'idx'), 1);


-- migrate:down

DELETE FROM qiita.principal WHERE idx = 1;
DROP TABLE IF EXISTS qiita.principal;
DROP TYPE IF EXISTS qiita.system_role;
