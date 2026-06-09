-- migrate:up

-- Visibility for the runner's in-place infra-unreachable retry. When a
-- step's submit/poll/result call keeps failing because the orchestrator or
-- slurmrestd is unreachable, the runner retries in place forever (the
-- never-fail-on-outage rule) — previously with nothing surfaced, so a ticket
-- looked silently wedged in `processing`. These two columns let the status
-- routes show *why* it is stuck and *since when*.
--
-- Both nullable, no CHECK: a transient marker is orthogonal to the failure_*
-- surface (the ticket is `processing`, not `failed`) and to every lifecycle
-- state. The runner sets them while retrying and clears them once it makes
-- progress or the ticket terminalizes. Plain columns, expand-only — safe to
-- add ahead of the code that writes them.
ALTER TABLE qiita.work_ticket
    ADD COLUMN transient_reason TEXT,
    ADD COLUMN transient_since  TIMESTAMPTZ;

COMMENT ON COLUMN qiita.work_ticket.transient_reason IS
    'Human-readable reason the runner is currently retrying in place (e.g. '
    '"submit: slurmrestd_unreachable"); NULL when not stuck. Advisory only — '
    'distinct from the failure_* surface, which is set only on a FAILED ticket.';
COMMENT ON COLUMN qiita.work_ticket.transient_since IS
    'When the current in-place-retry episode began (first infra-unreachable '
    'failure of the stuck step); NULL when not stuck. Pairs with '
    'transient_reason for the "stuck since T" status view.';

-- migrate:down

ALTER TABLE qiita.work_ticket
    DROP COLUMN transient_reason,
    DROP COLUMN transient_since;
