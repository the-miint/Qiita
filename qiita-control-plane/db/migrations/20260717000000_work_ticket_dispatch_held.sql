-- migrate:up

-- Fan-out dispatch throttle (sharded reference-index build, bulk read-mask
-- block, bulk sharded-alignment block).
--
-- `dispatch_held` marks a fan-out work_ticket that has been INSERTed (durable +
-- reconcile-visible) but NOT yet released for dispatch. The control-plane
-- "pump" (qiita_control_plane.fanout_dispatch.top_up_dispatch) releases held
-- tickets only FANOUT_MAX_INFLIGHT at a time per cohort, so a 1000-shard build
-- can no longer open ~1000 concurrent data-plane streams against a single
-- data-plane instance at once (the WOL3 / reference-16 incident: fd exhaustion,
-- submit-time ticket expiry from the backlog, cascading failures). A held
-- ticket sits `state = 'pending'` with `dispatch_held = true`; the pump flips it
-- to false and fires schedule_dispatch.
--
-- Constant DEFAULT false => this is a metadata-only ADD COLUMN (no table
-- rewrite, no long ACCESS EXCLUSIVE lock on the busy work_ticket table) and
-- every pre-existing row reads false, i.e. behaves EXACTLY as before: dispatched
-- immediately, reconciled at startup like any other in-flight ticket. Only the
-- three fan-out INSERT paths opt a row into `true`; a non-fan-out ticket is
-- never held.
ALTER TABLE qiita.work_ticket
    ADD COLUMN dispatch_held BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN qiita.work_ticket.dispatch_held IS
    'Fan-out throttle flag. true = INSERTed but not yet released for dispatch; '
    'the control-plane pump (fanout_dispatch.top_up_dispatch) releases held '
    'tickets FANOUT_MAX_INFLIGHT at a time per cohort, and startup reconcile does '
    'NOT auto-dispatch them (the pump owns them). Always false for non-fan-out '
    'tickets.';

-- Partial index over only the (transient, small) held set so the pump''s
-- "next held ticket in this cohort" pick and the reconcile "which cohorts still
-- have held tickets" scan stay cheap regardless of total work_ticket volume.
-- The cohort filter (reference_idx / mask_idx / alignment_idx) rides the
-- existing scope-target partial indexes; this one narrows to held rows and
-- carries the idx for the ORDER BY the pump releases in.
CREATE INDEX work_ticket_dispatch_held_idx
    ON qiita.work_ticket (work_ticket_idx)
    WHERE dispatch_held;

-- migrate:down

DROP INDEX IF EXISTS qiita.work_ticket_dispatch_held_idx;
ALTER TABLE qiita.work_ticket DROP COLUMN IF EXISTS dispatch_held;
