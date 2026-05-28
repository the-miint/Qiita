-- migrate:up transaction:false
-- ALTER TYPE ... ADD VALUE cannot run inside a transaction on any Postgres
-- version. dbmate auto-wraps migrations in BEGIN/COMMIT by default; we opt
-- out via the transaction:false directive. This is the first non-atomic
-- migration in this repo. The non-atomicity is the cost; idempotency
-- helpers (IF NOT EXISTS, IF EXISTS) make re-runs safe even if a later
-- statement fails.

ALTER TYPE qiita.scope_target_kind ADD VALUE IF NOT EXISTS 'sequenced_pool';

ALTER TABLE qiita.work_ticket
  ADD COLUMN IF NOT EXISTS sequenced_pool_idx BIGINT
    REFERENCES qiita.sequenced_pool(idx) ON DELETE RESTRICT;

ALTER TABLE qiita.work_ticket DROP CONSTRAINT IF EXISTS work_ticket_scope_target_consistent;
ALTER TABLE qiita.work_ticket ADD CONSTRAINT work_ticket_scope_target_consistent CHECK (
    (scope_target_kind = 'study_prep'
        AND study_idx IS NOT NULL
        AND prep_idx IS NOT NULL
        AND reference_idx IS NULL
        AND prep_sample_idx IS NULL
        AND sequenced_pool_idx IS NULL)
    OR
    (scope_target_kind = 'reference'
        AND reference_idx IS NOT NULL
        AND study_idx IS NULL
        AND prep_idx IS NULL
        AND prep_sample_idx IS NULL
        AND sequenced_pool_idx IS NULL)
    OR
    (scope_target_kind = 'prep_sample'
        AND prep_sample_idx IS NOT NULL
        AND study_idx IS NULL
        AND prep_idx IS NULL
        AND reference_idx IS NULL
        AND sequenced_pool_idx IS NULL)
    OR
    (scope_target_kind = 'sequenced_pool'
        AND sequenced_pool_idx IS NOT NULL
        AND study_idx IS NULL
        AND prep_idx IS NULL
        AND reference_idx IS NULL
        AND prep_sample_idx IS NULL)
);

CREATE INDEX IF NOT EXISTS work_ticket_sequenced_pool_idx
    ON qiita.work_ticket (sequenced_pool_idx)
    WHERE sequenced_pool_idx IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS work_ticket_one_in_flight_per_sequenced_pool
    ON qiita.work_ticket (action_id, action_version, sequenced_pool_idx)
    WHERE sequenced_pool_idx IS NOT NULL
      AND state IN ('pending', 'queued', 'processing');

-- migrate:down transaction:false

DROP INDEX IF EXISTS qiita.work_ticket_one_in_flight_per_sequenced_pool;
DROP INDEX IF EXISTS qiita.work_ticket_sequenced_pool_idx;
ALTER TABLE qiita.work_ticket DROP CONSTRAINT IF EXISTS work_ticket_scope_target_consistent;
ALTER TABLE qiita.work_ticket ADD CONSTRAINT work_ticket_scope_target_consistent CHECK (
    (scope_target_kind = 'study_prep'
        AND study_idx IS NOT NULL
        AND prep_idx IS NOT NULL
        AND reference_idx IS NULL
        AND prep_sample_idx IS NULL)
    OR
    (scope_target_kind = 'reference'
        AND reference_idx IS NOT NULL
        AND study_idx IS NULL
        AND prep_idx IS NULL
        AND prep_sample_idx IS NULL)
    OR
    (scope_target_kind = 'prep_sample'
        AND prep_sample_idx IS NOT NULL
        AND study_idx IS NULL
        AND prep_idx IS NULL
        AND reference_idx IS NULL)
);
ALTER TABLE qiita.work_ticket DROP COLUMN IF EXISTS sequenced_pool_idx;
-- A Postgres ENUM value cannot be removed without recreating the type.
-- 'sequenced_pool' stays in the ENUM after down; safe because no rows
-- with that scope_target_kind can exist (FK column has been dropped).
