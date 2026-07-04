-- migrate:up
-- The 'block' scope_target_kind ENUM value was added in the prior migration
-- (20260701000004_scope_target_kind_add_block.sql); splitting the ADD VALUE off
-- lets this migration reference the new value in the scope-target CHECK without
-- tripping Postgres's "unsafe use of new value of enum type" guard (55P04).

-- The block scope-target arm. NO ON DELETE action (plain NO ACTION): the
-- reverse edge qiita.block.work_ticket_idx → work_ticket is ON DELETE CASCADE,
-- and NO ACTION's end-of-statement check (unlike RESTRICT's immediate check)
-- lets a cascading delete of a ticket-then-its-block resolve without a
-- self-deadlock on the mutual reference.
ALTER TABLE qiita.work_ticket
  ADD COLUMN IF NOT EXISTS block_idx BIGINT
    REFERENCES qiita.block(block_idx);

COMMENT ON COLUMN qiita.work_ticket.block_idx IS
    'The block this ticket runs (bulk-block read masking). NULL for every '
    'non-block scope. NO ACTION FK (deferred-style end-of-statement check) so '
    'the mutual reference with qiita.block.work_ticket_idx (ON DELETE CASCADE) '
    'does not deadlock a cascading delete.';

-- Extend the tagged-union consistency CHECK: add block_idx IS NULL to every
-- existing arm and a new 'block' arm (block_idx set, every other scope NULL).
ALTER TABLE qiita.work_ticket DROP CONSTRAINT IF EXISTS work_ticket_scope_target_consistent;
ALTER TABLE qiita.work_ticket ADD CONSTRAINT work_ticket_scope_target_consistent CHECK (
    (scope_target_kind = 'study_prep'
        AND study_idx IS NOT NULL
        AND prep_idx IS NOT NULL
        AND reference_idx IS NULL
        AND prep_sample_idx IS NULL
        AND sequenced_pool_idx IS NULL
        AND block_idx IS NULL)
    OR
    (scope_target_kind = 'reference'
        AND reference_idx IS NOT NULL
        AND study_idx IS NULL
        AND prep_idx IS NULL
        AND prep_sample_idx IS NULL
        AND sequenced_pool_idx IS NULL
        AND block_idx IS NULL)
    OR
    (scope_target_kind = 'prep_sample'
        AND prep_sample_idx IS NOT NULL
        AND study_idx IS NULL
        AND prep_idx IS NULL
        AND reference_idx IS NULL
        AND sequenced_pool_idx IS NULL
        AND block_idx IS NULL)
    OR
    (scope_target_kind = 'sequenced_pool'
        AND sequenced_pool_idx IS NOT NULL
        AND study_idx IS NULL
        AND prep_idx IS NULL
        AND reference_idx IS NULL
        AND prep_sample_idx IS NULL
        AND block_idx IS NULL)
    OR
    (scope_target_kind = 'block'
        AND block_idx IS NOT NULL
        AND study_idx IS NULL
        AND prep_idx IS NULL
        AND reference_idx IS NULL
        AND prep_sample_idx IS NULL
        AND sequenced_pool_idx IS NULL)
);

-- Partial index mirroring the other scope arms: only block-scoped tickets
-- carry a block_idx, so a partial index avoids the NULL rows and supports
-- "find the ticket for this block" and the disallow-without-delete lookup.
CREATE INDEX IF NOT EXISTS work_ticket_block_idx
    ON qiita.work_ticket (block_idx)
    WHERE block_idx IS NOT NULL;

-- Atomic one-in-flight-per-block gate (the disallow-without-delete backstop):
-- at most one ticket per (action, block) may be non-terminal at a time.
CREATE UNIQUE INDEX IF NOT EXISTS work_ticket_one_in_flight_per_block
    ON qiita.work_ticket (action_id, action_version, block_idx)
    WHERE block_idx IS NOT NULL
      AND state IN ('pending', 'queued', 'processing');


-- migrate:down

DROP INDEX IF EXISTS qiita.work_ticket_one_in_flight_per_block;
DROP INDEX IF EXISTS qiita.work_ticket_block_idx;
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
ALTER TABLE qiita.work_ticket DROP COLUMN IF EXISTS block_idx;
