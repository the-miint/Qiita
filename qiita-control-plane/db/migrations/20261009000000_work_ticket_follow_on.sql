-- migrate:up

-- A follow-on: a ticket the control plane submits as this ticket's originator,
-- on the same scope target, once this ticket COMPLETES. It lets one gesture run
-- a chain whose later stage needs the earlier stage's output to exist.
--
-- `on_success` is the follow-on's {action_id, action_version, action_context}.
-- Its outcome is recorded here, exactly one of:
--   follow_on_work_ticket_idx  the ticket it created, written in the same
--                              transaction as that ticket's INSERT, so a created
--                              follow-on is never unrecorded and never created twice
--   follow_on_error            why its submission was refused
-- Both NULL on a completed ticket means the submission has not happened yet (or
-- failed transiently); the startup reconcile submits it then.
--
-- follow_on_work_ticket_idx carries no foreign key: it records
-- that the follow-on WAS submitted. Deleting that ticket must not clear the
-- record, or the next reconcile would submit the follow-on again.
ALTER TABLE qiita.work_ticket
    ADD COLUMN on_success JSONB,
    ADD COLUMN follow_on_work_ticket_idx BIGINT,
    ADD COLUMN follow_on_error TEXT;

ALTER TABLE qiita.work_ticket
    ADD CONSTRAINT work_ticket_follow_on_needs_on_success CHECK (
        on_success IS NOT NULL
        OR (follow_on_work_ticket_idx IS NULL AND follow_on_error IS NULL)
    ),
    ADD CONSTRAINT work_ticket_follow_on_one_outcome CHECK (
        num_nonnulls(follow_on_work_ticket_idx, follow_on_error) <= 1
    );

COMMENT ON COLUMN qiita.work_ticket.on_success IS
    'Follow-on {action_id, action_version, action_context} the control plane '
    'submits as the originator, on this ticket''s scope target, once it COMPLETES. '
    'NULL = no follow-on.';
COMMENT ON COLUMN qiita.work_ticket.follow_on_work_ticket_idx IS
    'The ticket the follow-on submission created, recorded in the transaction '
    'that created it. No FK: deleting that ticket must not re-arm the follow-on.';
COMMENT ON COLUMN qiita.work_ticket.follow_on_error IS
    'Why the follow-on submission was refused; NULL otherwise.';

-- The startup reconcile looks for completed tickets whose follow-on has no
-- outcome yet; keep that scan off the full table.
CREATE INDEX work_ticket_follow_on_pending_idx
    ON qiita.work_ticket (work_ticket_idx)
    WHERE state = 'completed'
      AND on_success IS NOT NULL
      AND follow_on_work_ticket_idx IS NULL
      AND follow_on_error IS NULL;

-- migrate:down

DROP INDEX IF EXISTS qiita.work_ticket_follow_on_pending_idx;
ALTER TABLE qiita.work_ticket
    DROP CONSTRAINT IF EXISTS work_ticket_follow_on_one_outcome,
    DROP CONSTRAINT IF EXISTS work_ticket_follow_on_needs_on_success,
    DROP COLUMN IF EXISTS follow_on_error,
    DROP COLUMN IF EXISTS follow_on_work_ticket_idx,
    DROP COLUMN IF EXISTS on_success;
