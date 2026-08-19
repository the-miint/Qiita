-- migrate:up transaction:false
-- Postgres 12+ forbids using a newly-added ENUM value in any other statement
-- of the same transaction (SQLSTATE 55P04, "unsafe use of new value of enum
-- type"). dbmate sends every statement of a migration file to libpq in a
-- single Exec, which behaves as one implicit transaction even with the
-- transaction:false directive — so the ALTER TYPE ADD VALUE lives in its own
-- migration file, with no other statement referencing the value.
--
-- 'cancelled' is the terminal outcome for an OPERATOR-stopped work_ticket (the
-- `qiita-admin ticket cancel` path): the control plane flips it terminal so the
-- poll loop aborts and no new attempt is submitted, then reaps its SLURM job(s).
-- It carries NULL failure_* columns — distinct from 'failed' so a deliberate stop
-- is legible (ticket list, pool rollups, notify digest) rather than masquerading
-- as a genuine failure. Like 'failed' it is redrivable in place via /run.
--
-- No `work_ticket_one_in_flight_per_*` index change is needed: those partial
-- indexes enumerate the NON-terminal set (pending/queued/processing), and
-- 'cancelled' is terminal, so the derived non-terminal set is unchanged and the
-- parity test (test_work_ticket_state_parity) still passes. Mirrored by
-- qiita_common.models.WorkTicketState; the two value sets are kept in lockstep by
-- tests (test_enum_parity + test_work_ticket_state_parity) — change both in the
-- same PR.

ALTER TYPE qiita.work_ticket_state ADD VALUE IF NOT EXISTS 'cancelled';

-- migrate:down transaction:false
-- A Postgres ENUM value cannot be removed without recreating the type.
-- 'cancelled' stays in the ENUM after down. Safe: any rows carrying it must be
-- re-dispositioned before a down that would tighten the type, which this
-- migration does not attempt.
