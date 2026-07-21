-- migrate:up transaction:false
-- CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction block, and
-- dbmate sends every statement of a migration file to libpq in a single Exec that
-- behaves as one implicit transaction even under transaction:false — so each of
-- the up/down blocks must be EXACTLY ONE statement.
--
-- Step 1 of expanding the notify sweeper's owed-set partial index to the new
-- terminal state 'cancelled': drop the old index here, recreate it with the
-- widened predicate in the very next migration (20260721000002). Two files
-- because a CONCURRENTLY drop and a CONCURRENTLY create can't share one Exec.
DROP INDEX CONCURRENTLY IF EXISTS qiita.qiita_work_ticket_email_owed_idx;

-- migrate:down transaction:false
-- Recreate the PRE-cancelled predicate (the shape before this expand pair), so a
-- down restores the index the earlier migration created.
CREATE INDEX CONCURRENTLY qiita_work_ticket_email_owed_idx
    ON qiita.work_ticket (originator_principal_idx, updated_at)
    WHERE notified_at IS NULL
      AND state IN ('completed', 'failed', 'no_data')
      AND failure_type IS DISTINCT FROM 'retriable';
