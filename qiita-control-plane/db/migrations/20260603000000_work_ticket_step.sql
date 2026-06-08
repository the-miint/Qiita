-- migrate:up
-- =============================================================================
-- WORK TICKET STEP (per-entry execution progress — the restart-recovery spine)
-- =============================================================================
-- One row per (work_ticket_idx, step_index, attempt): every entry of an
-- action's `steps:` list — compute `step:` and in-process `action:` alike —
-- written by the control-plane runner as it executes the entry. It is the
-- durable record that lets a CP restart (a routine, undrained event on every
-- deploy) re-attach to in-flight work instead of failing it.
--
-- Why every entry, not just SLURM jobs: correctly resuming a *multi-step*
-- workflow requires knowing which entries already completed — to skip them
-- and rebuild the in-memory `bound` outputs from disk. So in-process
-- `action:` entries (compute_target = 'control_plane') are recorded here
-- too. This is deliberately broader than "SLURM job history".
--
-- Write-ahead: the runner inserts state = 'submitting' with the deterministic
-- job_name *before* calling the backend submit. If the process dies between
-- submit and persisting the returned job id, recovery still finds the row and
-- can adopt the orphaned SLURM job by name (CO's find-by-name lookup). The
-- (work_ticket_idx, step_index, attempt) primary key makes the write-ahead
-- insert idempotent on re-entry (ON CONFLICT DO NOTHING in step_progress.py).

CREATE TABLE qiita.work_ticket_step (
    -- Parent ticket. CASCADE on delete: the disallow-without-delete flow
    -- DELETEs a COMPLETED work_ticket before resubmission; its progress
    -- rows are history of that ticket and go with it.
    work_ticket_idx  BIGINT NOT NULL
        REFERENCES qiita.work_ticket(work_ticket_idx) ON DELETE CASCADE,

    -- 0-based position in action.steps. attempt is this entry's retry
    -- counter: a retriable BackendFailure resubmits as attempt + 1 (a new
    -- row); the prior attempt's row stays as the historical record.
    step_index       INT NOT NULL CHECK (step_index >= 0),
    attempt          INT NOT NULL DEFAULT 0 CHECK (attempt >= 0),

    -- Snapshot of action.steps[step_index].name. Free TEXT because step
    -- names are per-action / open-ended — same rationale as
    -- qiita.work_ticket.failure_step_name.
    step_name        TEXT NOT NULL CHECK (length(step_name) BETWEEN 1 AND 255),

    -- Where this entry executes. Mirrored by qiita_common.models.ComputeTarget
    -- (StrEnum). TEXT/CHECK, not a Postgres ENUM — same deliberate carve-out
    -- as upload.status / reference.status; see CLAUDE.md "Enum parity". Keep
    -- both sides in sync by hand. Only 'slurm' carries a slurm_job_id /
    -- job_name; 'local' and 'control_plane' run in-process (enforced below).
    compute_target   TEXT NOT NULL
        CHECK (compute_target IN ('slurm', 'local', 'control_plane')),

    -- CP-side write-ahead lifecycle. Mirrored by
    -- qiita_common.models.StepProgressState (StrEnum). TEXT/CHECK, not a
    -- Postgres ENUM — same carve-out as compute_target above.
    -- submitting → submitted → running → completed | failed.
    state            TEXT NOT NULL DEFAULT 'submitting'
        CHECK (state IN ('submitting', 'submitted', 'running', 'completed', 'failed')),

    -- The real SLURM job id, recorded by record_submitted once the backend
    -- returns it. NULL for in-process targets, and NULL for a 'slurm' entry
    -- still in 'submitting' (submit not yet returned).
    slurm_job_id     BIGINT,

    -- Deterministic job name qiita-wt{idx}-{step}-a{attempt}, written at
    -- write-ahead time so recovery can find a job whose id was never
    -- persisted. NULL for in-process targets.
    job_name         TEXT,

    -- Job id / name belong only to a real SLURM job; in-process entries
    -- must carry neither.
    CONSTRAINT work_ticket_step_slurm_fields_consistent CHECK (
        compute_target = 'slurm'
        OR (slurm_job_id IS NULL AND job_name IS NULL)
    ),

    -- The inverse: a SLURM entry that has advanced past write-ahead and
    -- hasn't failed MUST carry its job id. slurm_job_id starts NULL at
    -- 'submitting' (record_submitted sets it on submit return); a 'failed'
    -- entry may still be NULL because a write-ahead row can fail before the
    -- submit ever returns. So only 'submitted' / 'running' / 'completed'
    -- demand the id — defence-in-depth against a job-less SLURM row reaching
    -- a state the recovery path expects to re-attach by id.
    CONSTRAINT work_ticket_step_slurm_job_id_present CHECK (
        compute_target <> 'slurm'
        OR state IN ('submitting', 'failed')
        OR slurm_job_id IS NOT NULL
    ),

    -- Failure surface, set together on state = 'failed', NULL otherwise.
    -- failure_kind is the fine-grained qiita_common FailureKind value (free
    -- TEXT — FailureKind has no Postgres twin; retriable-vs-permanent is
    -- derived from it in Python). failure_reason is the human explanation.
    failure_kind     TEXT,
    failure_reason   TEXT,
    CONSTRAINT work_ticket_step_failure_consistent CHECK (
        (state = 'failed'
            AND failure_kind IS NOT NULL
            AND failure_reason IS NOT NULL)
        OR
        (state <> 'failed'
            AND failure_kind IS NULL
            AND failure_reason IS NULL)
    ),

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by qiita.set_updated_at().
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (work_ticket_idx, step_index, attempt)
);

COMMENT ON TABLE qiita.work_ticket_step IS
    'Per-entry execution progress for a work_ticket: one row per '
    '(work_ticket_idx, step_index, attempt) covering every action.steps '
    'entry (compute step: and in-process action: alike). Write-ahead spine '
    'for compute decoupling + restart recovery — the runner records '
    'state=submitting BEFORE submit so a CP restart can re-attach in-flight '
    'work rather than fail it. compute_target / state mirror the '
    'qiita_common StrEnums ComputeTarget / StepProgressState.';

COMMENT ON COLUMN qiita.work_ticket_step.job_name IS
    'Deterministic SLURM job name qiita-wt{idx}-{step}-a{attempt}, written '
    'at write-ahead time so recovery can adopt a job whose id was never '
    'persisted (CO find-by-name). NULL for in-process targets.';

CREATE TRIGGER work_ticket_step_set_updated_at
    BEFORE UPDATE ON qiita.work_ticket_step
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- migrate:down

DROP TRIGGER IF EXISTS work_ticket_step_set_updated_at ON qiita.work_ticket_step;
DROP TABLE IF EXISTS qiita.work_ticket_step;
