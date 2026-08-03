-- Per-step escalated resource floor, learned by the runner's retry ladder.
--
-- Nullable JSONB mapping a `step:` entry's name to the floor that step's
-- OOM/TIMEOUT escalation has climbed to:
--
--     {"assemble": {"mem_gb": 384, "walltime_seconds": 115200}}
--
-- Written by the runner as `_run_entry_with_retry` escalates and read back at
-- the start of every run, so a control-plane restart or a `/run` redrive
-- continues the ladder rather than restarting it at the YAML baseline. See the
-- COMMENT ON COLUMN below for how it composes with `resource_override`.
--
-- Keyed by step NAME, which ActionDefinition._step_entry_names_unique
-- guarantees is unique among `step:` entries within an action; the ticket pins
-- (action_id, action_version), so the key space is stable for its whole life.
-- Only `step:` entries carry baseline_resources and escalate.
--
-- The CHECK exists because the read side cannot tell a jsonb `null` from a SQL
-- NULL — both decode to Python None, which the runner reads as "nothing
-- escalated yet". Clearing this column by hand is a documented operator step,
-- so `'null'::jsonb` instead of NULL would silently reset a ticket's ladder.
-- The write side needs no such guard: `jsonb_set` errors on a non-object
-- target (verified on PG 17.10 — "cannot set path in scalar"), so a bad shape
-- fails the UPDATE loudly rather than no-opping.
--
-- Additive and backfill-free: every existing row reads as NULL, i.e. today's
-- behaviour.

-- migrate:up
ALTER TABLE qiita.work_ticket
  ADD COLUMN escalated_resource_floor JSONB,
  ADD CONSTRAINT work_ticket_escalated_resource_floor_is_object CHECK (
      escalated_resource_floor IS NULL
      OR jsonb_typeof(escalated_resource_floor) = 'object'
  );

COMMENT ON COLUMN qiita.work_ticket.escalated_resource_floor IS
    'Per-step resource floor the runner learned by escalating: '
    '{"<step name>": {"mem_gb": N, "walltime_seconds": N}}. Written on each '
    'OOM/TIMEOUT escalation so a control-plane restart or /run redrive '
    'continues the ladder instead of restarting it at the baseline. NULL '
    'means nothing escalated yet. Runner-owned and PER STEP, unlike '
    'resource_override (caller-supplied at submission, ticket-wide); the two '
    'compose at dispatch as raise-only floors, still ceiling-clamped. A step '
    'renamed in the workflow YAML leaves a stale key here — inert, since an '
    'unmatched key is never read. Surviving a redrive is the point, but a '
    'ticket redriven with a corrected, smaller input keeps the floor its '
    'earlier input taught it; NULL the column (never ''null''::jsonb, which '
    'the CHECK rejects) to put it back on the baseline.';


-- migrate:down

ALTER TABLE qiita.work_ticket
  DROP CONSTRAINT work_ticket_escalated_resource_floor_is_object,
  DROP COLUMN escalated_resource_floor;
