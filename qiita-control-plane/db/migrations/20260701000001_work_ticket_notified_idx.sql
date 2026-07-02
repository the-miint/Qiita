-- migrate:up transaction:false
-- CREATE INDEX CONCURRENTLY cannot run inside a transaction block, and dbmate
-- sends every statement of a migration file to libpq in a single Exec that
-- behaves as one implicit transaction even under transaction:false — so this
-- up block must be EXACTLY ONE statement (the down block likewise). This is a
-- DIFFERENT reason than the 'no_data' migration's transaction:false (that one
-- is for ALTER TYPE ... ADD VALUE).
--
-- Partial index backing the notify sweeper's owed-set SELECT. The predicate
-- MUST byte-match that WHERE clause — including the terminal-state literals
-- (in sorted order, matching qiita_control_plane.notify.sweeper) and the
-- failure_type IS DISTINCT FROM 'retriable' carve-out — or the planner won't
-- use it. Partial keeps it small: it carries only the handful of owed rows,
-- never the full terminal-ticket history.
CREATE INDEX CONCURRENTLY qiita_work_ticket_email_owed_idx
    ON qiita.work_ticket (originator_principal_idx, updated_at)
    WHERE notified_at IS NULL
      AND state IN ('completed', 'failed', 'no_data')
      AND failure_type IS DISTINCT FROM 'retriable';

-- migrate:down transaction:false
DROP INDEX CONCURRENTLY IF EXISTS qiita.qiita_work_ticket_email_owed_idx;
