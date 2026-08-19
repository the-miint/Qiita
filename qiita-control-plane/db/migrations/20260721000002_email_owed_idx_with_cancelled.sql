-- migrate:up transaction:false
-- CREATE INDEX CONCURRENTLY cannot run inside a transaction block; dbmate runs
-- this file's up block as one Exec, so it must be EXACTLY ONE statement.
--
-- Step 2 of the owed-set index expand: recreate the notify sweeper's partial
-- index with 'cancelled' added to the terminal-state list. An operator-cancelled
-- ticket is a terminal outcome its ORIGINATOR is owed a digest for (they may not
-- be the admin who cancelled it), so it joins completed / failed / no_data here.
-- The predicate MUST byte-match the sweeper's owed-set WHERE — the terminal-state
-- literals in SORTED order (matching qiita_control_plane.notify.sweeper's
-- `tuple(sorted(TERMINAL_WORK_TICKET_STATES))`) plus the retriable carve-out — or
-- the planner won't use it. 'cancelled' sorts first. Uses the value added by
-- 20260721000000; safe in a separate migration (separate transaction from the
-- ALTER TYPE ADD VALUE).
CREATE INDEX CONCURRENTLY qiita_work_ticket_email_owed_idx
    ON qiita.work_ticket (originator_principal_idx, updated_at)
    WHERE notified_at IS NULL
      AND state IN ('cancelled', 'completed', 'failed', 'no_data')
      AND failure_type IS DISTINCT FROM 'retriable';

-- migrate:down transaction:false
DROP INDEX CONCURRENTLY IF EXISTS qiita.qiita_work_ticket_email_owed_idx;
