-- migrate:up

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


CREATE TABLE qiita.principal (
    idx             BIGSERIAL PRIMARY KEY,
    display_name    VARCHAR(255) NOT NULL,
    system_role     qiita.system_role NOT NULL DEFAULT 'user',
    created_by_idx  BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

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
    )
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


-- migrate:down

DROP TABLE IF EXISTS qiita.principal;
DROP TYPE IF EXISTS qiita.system_role;
