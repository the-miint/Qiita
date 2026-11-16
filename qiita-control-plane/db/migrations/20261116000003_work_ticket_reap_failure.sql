-- migrate:up

-- A cancel whose reap of the ticket's SLURM jobs failed. Set by
-- `work_ticket_cancel.cancel_work_ticket` when the scancel raises, cleared when a later
-- cancel of the same ticket reaps without error. While set, a redrive is refused
-- (`ticket_redrive.reset_ticket_to_pending`): a job the failed reap left running could
-- still be writing under the ticket. NULL on every ticket that never had a failed reap.
ALTER TABLE qiita.work_ticket
    ADD COLUMN reap_failed_at TIMESTAMPTZ,
    ADD COLUMN reap_failure TEXT,
    ADD CONSTRAINT work_ticket_reap_failure_consistent CHECK (
        (reap_failed_at IS NULL AND reap_failure IS NULL)
        OR (reap_failed_at IS NOT NULL AND reap_failure IS NOT NULL
            AND btrim(reap_failure) <> '')
    );

COMMENT ON COLUMN qiita.work_ticket.reap_failed_at IS
    'When the last cancel''s reap of this ticket''s SLURM jobs failed; NULL when none '
    'failed or a later cancel reaped. A redrive is refused while it is set.';
COMMENT ON COLUMN qiita.work_ticket.reap_failure IS
    'The error of that failed reap; set exactly when reap_failed_at is.';


-- migrate:down

ALTER TABLE qiita.work_ticket
    DROP CONSTRAINT IF EXISTS work_ticket_reap_failure_consistent,
    DROP COLUMN IF EXISTS reap_failure,
    DROP COLUMN IF EXISTS reap_failed_at;
