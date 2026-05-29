-- migrate:up
-- The 'sequenced_pool' ENUM value was added in the prior migration
-- (20260528000000_scope_target_kind_add_sequenced_pool.sql); splitting the
-- ADD VALUE off lets this migration reference the new value in a CHECK
-- constraint without tripping Postgres's "unsafe use of new value of enum
-- type" guard (SQLSTATE 55P04). With the ENUM value already committed,
-- this file runs atomically under dbmate's default transactional wrapping.

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

-- migrate:down

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
