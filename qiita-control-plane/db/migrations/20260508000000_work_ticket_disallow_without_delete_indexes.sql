-- migrate:up

-- =============================================================================
-- WORK TICKET disallow-without-delete unique partial indexes
-- =============================================================================
-- Enforces, atomically in the database, the rule that at most one ticket for
-- a given (action_id, action_version, scope_target) can be in a non-terminal
-- state at a time. Previously enforced only by a SELECT-then-INSERT in the
-- route handler, which is racy under concurrent submissions.
--
-- Three indexes — one per scope-target arm — because the partial-uniqueness
-- key differs by arm. The route handler still SELECT-LIMIT-1s for the
-- happy-path 409 with a useful blocking-ticket idx in the error body; the
-- index is the actual gate when two submissions race past that check.

CREATE UNIQUE INDEX work_ticket_one_in_flight_per_reference
    ON qiita.work_ticket (action_id, action_version, reference_idx)
    WHERE reference_idx IS NOT NULL
      AND state IN ('pending', 'queued', 'processing');

CREATE UNIQUE INDEX work_ticket_one_in_flight_per_study_prep
    ON qiita.work_ticket (action_id, action_version, study_idx, prep_idx)
    WHERE study_idx IS NOT NULL
      AND state IN ('pending', 'queued', 'processing');

CREATE UNIQUE INDEX work_ticket_one_in_flight_per_sequenced_sample
    ON qiita.work_ticket (action_id, action_version, sequenced_sample_idx)
    WHERE sequenced_sample_idx IS NOT NULL
      AND state IN ('pending', 'queued', 'processing');


-- =============================================================================
-- COMMENT ON COLUMN qiita.work_ticket.action_context
-- =============================================================================
-- Document at the column level that action_context is application-validated
-- against the action's declared context_schema, so future readers don't have
-- to grep to discover the structural contract is not enforced by Postgres.

COMMENT ON COLUMN qiita.work_ticket.action_context IS
    'Action-defined free-form parameters. Validated at submission against '
    'qiita.action.context_schema (a JSON Schema fragment) by the control '
    'plane route handler — no DB-level constraint. An action with empty '
    'context_schema accepts any object; default ''{}'' means no parameters.';


-- migrate:down

DROP INDEX IF EXISTS qiita.work_ticket_one_in_flight_per_reference;
DROP INDEX IF EXISTS qiita.work_ticket_one_in_flight_per_study_prep;
DROP INDEX IF EXISTS qiita.work_ticket_one_in_flight_per_sequenced_sample;
COMMENT ON COLUMN qiita.work_ticket.action_context IS NULL;
