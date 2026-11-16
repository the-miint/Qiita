-- migrate:up

-- A cancel's reap of the ticket's SLURM jobs that has not succeeded.
-- `work_ticket_cancel.cancel_work_ticket` sets both columns in its flip transaction,
-- before the scancel, with `reap_note` the in-flight marker. A reap without error
-- clears them; a failed reap keeps `reap_pending_since` and writes its error to
-- `reap_note`; a cancel interrupted mid-reap leaves the in-flight marker until the next
-- cancel's reap clears it. While set, a redrive is refused
-- (`ticket_redrive.reset_ticket_to_pending`): a job the reap has not reached could
-- still be writing under the ticket.
ALTER TABLE qiita.work_ticket
    ADD COLUMN reap_pending_since TIMESTAMPTZ,
    ADD COLUMN reap_note TEXT,
    ADD CONSTRAINT work_ticket_reap_pending_consistent CHECK (
        (reap_pending_since IS NULL AND reap_note IS NULL)
        OR (reap_pending_since IS NOT NULL AND reap_note IS NOT NULL
            AND btrim(reap_note) <> '')
    );

COMMENT ON COLUMN qiita.work_ticket.reap_pending_since IS
    'When the cancel whose reap of this ticket''s SLURM jobs has not succeeded '
    'started it: set before the scancel, kept when the reap fails or is interrupted, '
    'NULL once a reap succeeds. Each cancel writes its own value, and its outcome '
    'applies only while the value is still its own. A redrive is refused while set.';
COMMENT ON COLUMN qiita.work_ticket.reap_note IS
    'Set exactly when reap_pending_since is: the in-flight marker while the reap runs '
    'or after it was interrupted, else the error of the failed reap.';


-- migrate:down

ALTER TABLE qiita.work_ticket
    DROP CONSTRAINT IF EXISTS work_ticket_reap_pending_consistent,
    DROP COLUMN IF EXISTS reap_note,
    DROP COLUMN IF EXISTS reap_pending_since;
