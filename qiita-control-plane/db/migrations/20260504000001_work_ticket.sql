-- migrate:up

-- =============================================================================
-- ENUM TYPES used only by qiita.work_ticket
-- =============================================================================
-- work_ticket_state is the closed lifecycle set mirrored by
-- qiita_common.models.WorkTicketState. PENDING / QUEUED / PROCESSING are
-- the non-terminal states the orchestrator actively manages. COMPLETED /
-- FAILED are terminal.
CREATE TYPE qiita.work_ticket_state AS ENUM (
    'pending',
    'queued',
    'processing',
    'completed',
    'failed'
);

-- failure_type discriminates retriable infra failures (NODE_FAIL, OOM,
-- transient FS errors, slurmrestd reachability) from permanent ones (bad
-- input, contract violations, exit codes from terminal-failure workflows,
-- retries-exhausted). The runner consults the type to decide PROCESSING →
-- QUEUED retry vs PROCESSING → FAILED. Mirrored by
-- qiita_common.models.FailureType.
CREATE TYPE qiita.failure_type AS ENUM (
    'retriable',
    'permanent'
);

-- work_ticket_failure_stage is the coarse "where in the lifecycle did it
-- fail" discriminator. step_run is paired with a non-NULL
-- failure_step_name pointing at the specific entry in action.steps that
-- raised; submission and finalize cover everything outside the step loop.
-- Mirrored by qiita_common.models.WorkTicketFailureStage.
CREATE TYPE qiita.work_ticket_failure_stage AS ENUM (
    'submission',  -- before the runner loop: action lookup, scope_target,
                   -- ticket transition PENDING → PROCESSING
    'step_run',    -- inside the runner loop, executing one entry of
                   -- action.steps (workflow step OR action-library primitive)
    'finalize'     -- after the loop: success_status PATCH, ticket transition
                   -- PROCESSING → COMPLETED
);


-- =============================================================================
-- WORK TICKET (compute-orchestrator action invocations)
-- =============================================================================
-- A work_ticket is the control-plane's record of an action invocation: who
-- requested it, which resource it targets, what action-defined context it
-- carries, and what lifecycle state it's in. The orchestrator pulls tickets
-- off the queue, dispatches the action's step pipeline, and reports back via
-- state transitions.
--
-- Submission gate: the application layer disallows new submissions when an
-- existing ticket for the same (scope_target, action_id, action_version) is
-- in PENDING / QUEUED / PROCESSING; COMPLETED requires explicit DELETE
-- before re-submission. That check is enforced in the route handler, not
-- here — the DB carries no partial-unique index for it because the exact
-- triple to key on (and whether to include action_context fields) depends
-- on per-action semantics that live in the registry.

CREATE TABLE qiita.work_ticket (
    work_ticket_idx          BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Action invoked by this ticket. (action_id, action_version) FK into
    -- qiita.action; RESTRICT on delete so an action with outstanding
    -- tickets can't be hard-deleted out from under them.
    action_id                TEXT NOT NULL CHECK (length(action_id) BETWEEN 1 AND 255),
    action_version           TEXT NOT NULL CHECK (length(action_version) BETWEEN 1 AND 100),
    FOREIGN KEY (action_id, action_version)
        REFERENCES qiita.action (action_id, version)
        ON DELETE RESTRICT,

    -- Submitter. Priority and resource profile resolve from the originator,
    -- not the executor. RESTRICT so a principal with outstanding tickets
    -- can't be hard-deleted.
    originator_principal_idx BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,

    -- Tagged-union scope target. Exactly one of (study_idx, prep_idx) or
    -- reference_idx is non-null, governed by scope_target_kind via the
    -- work_ticket_scope_target_consistent CHECK below. This is the column
    -- the resource-ACL gate keys off; matches the discriminated-union shape
    -- of qiita_common.models.ScopeTarget.
    --
    -- prep_idx carries no FK because there is no prep table yet; it's a
    -- plain BIGINT. study_idx and reference_idx both RESTRICT so a
    -- referenced row can't disappear from under an in-flight ticket.
    scope_target_kind        qiita.scope_target_kind NOT NULL,
    study_idx                BIGINT REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    prep_idx                 BIGINT,
    reference_idx            BIGINT REFERENCES qiita.reference(reference_idx) ON DELETE RESTRICT,

    CONSTRAINT work_ticket_scope_target_consistent CHECK (
        (scope_target_kind = 'study_prep'
            AND study_idx IS NOT NULL
            AND prep_idx IS NOT NULL
            AND reference_idx IS NULL)
        OR
        (scope_target_kind = 'reference'
            AND reference_idx IS NOT NULL
            AND study_idx IS NULL
            AND prep_idx IS NULL)
    ),

    -- Action-defined free-form context. Per-action JSON-Schema validation
    -- (against the action's declared `context_schema`) happens at submission
    -- in the route handler; this column accepts any object.
    action_context           JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Lifecycle. Mirrors qiita_common.models.WorkTicketState.
    state                    qiita.work_ticket_state NOT NULL DEFAULT 'pending',

    -- Retry accounting. retry_count starts at 0 and is incremented by the
    -- runner on each PROCESSING → QUEUED retry transition. When a step
    -- raises a retriable BackendFailure and retry_count < max_retries, the
    -- ticket bounces back to QUEUED for another attempt; otherwise it
    -- transitions to FAILED with the captured failure_*. Tickets inherit
    -- the DB default (3) on submission; admins can override per-row to
    -- nudge a specific stuck ticket without a redeploy.
    --
    -- max_retries upper bound 100: a ticket retrying 100 times has either
    -- hit a genuinely transient pattern (unlikely past ~5 attempts) or
    -- the failure is being mis-classified as transient. The cap forces
    -- ops to investigate rather than letting a misclassification eat
    -- compute indefinitely.
    retry_count              INT NOT NULL DEFAULT 0
        CHECK (retry_count >= 0),
    max_retries              INT NOT NULL DEFAULT 3
        CHECK (max_retries >= 0 AND max_retries <= 100),

    -- Failure surface. Set together when state = 'failed'; all NULL
    -- otherwise — enforced by work_ticket_failure_consistent below. The
    -- coarse stage is enum-bounded; failure_step_name carries the
    -- workflow step's `.name` when the failure occurred inside the step
    -- loop (free TEXT because step names are open-ended per-action).
    -- failure_reason is the human-readable explanation that appears in
    -- ops dashboards and post-mortems.
    failure_type             qiita.failure_type,
    failure_stage            qiita.work_ticket_failure_stage,
    failure_step_name        TEXT
        CHECK (failure_step_name IS NULL OR length(failure_step_name) BETWEEN 1 AND 255),
    failure_reason           TEXT,

    -- Failure columns travel together: all set on FAILED, all NULL otherwise.
    -- Loud constraint instead of relying on code-level discipline; a stale
    -- failure_reason on a COMPLETED ticket would mislead ops dashboards.
    CONSTRAINT work_ticket_failure_consistent CHECK (
        (state = 'failed'
            AND failure_type IS NOT NULL
            AND failure_stage IS NOT NULL
            AND failure_reason IS NOT NULL)
        OR
        (state <> 'failed'
            AND failure_type IS NULL
            AND failure_stage IS NULL
            AND failure_step_name IS NULL
            AND failure_reason IS NULL)
    ),

    -- failure_step_name is meaningful only when the step loop was running.
    -- Couples the open-text column to the closed-enum stage so the two
    -- can't drift (e.g. a "stage = submission" row carrying a step name
    -- copied from a previous attempt).
    CONSTRAINT work_ticket_failure_step_name_consistent CHECK (
        (failure_stage = 'step_run' AND failure_step_name IS NOT NULL)
        OR
        (failure_stage IN ('submission', 'finalize') AND failure_step_name IS NULL)
        OR
        (failure_stage IS NULL AND failure_step_name IS NULL)
    ),

    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by qiita.set_updated_at().
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE qiita.work_ticket IS
    'Action invocations: who requested, which resource, what action-defined '
    'context, and lifecycle state. (action_id, action_version) FK into '
    'qiita.action. Granularity is one row per action invocation, scoped at '
    'reference / study_prep level — not per sample. Sample-level fan-out '
    'happens inside the workflow''s `step:` entries (e.g. map steps that '
    'submit one SLURM job per prep_sample_idx); all such jobs share the '
    'same work_ticket_idx.';

COMMENT ON COLUMN qiita.work_ticket.failure_step_name IS
    'Free-text step name copied from action.steps[i].name when a STEP_RUN '
    'failure occurs. Free-text rather than FK because the set of valid '
    'names is per-action — but the value is always a snapshot of an entry '
    'from the action this ticket was submitted against. Ops dashboards '
    'join back to action metadata via (action_id, action_version, '
    'failure_step_name).';

COMMENT ON COLUMN qiita.work_ticket.max_retries IS
    'Upper bound on retry attempts for retriable BackendFailure. Default 3. '
    'Admins can raise per-row to nudge a stuck ticket; the 100 ceiling '
    'forces ops investigation rather than letting a mis-classified-transient '
    'failure eat compute indefinitely.';

-- The orchestrator polls for PENDING / QUEUED / PROCESSING tickets to
-- dispatch and watch; COMPLETED / FAILED are terminal and seldom queried
-- in bulk. A plain b-tree on `state` is sufficient at expected volumes.
CREATE INDEX work_ticket_state_idx ON qiita.work_ticket (state);

-- Supports "list my tickets" and originator-keyed audit queries.
CREATE INDEX work_ticket_originator_idx ON qiita.work_ticket (originator_principal_idx);

-- Partial indexes on the scope-target columns: each ticket sets exactly one
-- of the two arms, so a partial index avoids carrying NULL rows the arm
-- never queries. Supports "find tickets targeting this reference" and the
-- disallow-without-delete check for sample-processing actions.
CREATE INDEX work_ticket_reference_idx
    ON qiita.work_ticket (reference_idx)
    WHERE reference_idx IS NOT NULL;

CREATE INDEX work_ticket_study_prep_idx
    ON qiita.work_ticket (study_idx, prep_idx)
    WHERE study_idx IS NOT NULL;

CREATE TRIGGER work_ticket_set_updated_at
    BEFORE UPDATE ON qiita.work_ticket
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- migrate:down

DROP TRIGGER IF EXISTS work_ticket_set_updated_at ON qiita.work_ticket;
DROP TABLE IF EXISTS qiita.work_ticket;
DROP TYPE IF EXISTS qiita.work_ticket_failure_stage;
DROP TYPE IF EXISTS qiita.failure_type;
DROP TYPE IF EXISTS qiita.work_ticket_state;
