-- migrate:up

-- =============================================================================
-- ENUM TYPE used only by qiita.work_ticket
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
DROP TYPE IF EXISTS qiita.work_ticket_state;
