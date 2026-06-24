-- migrate:up transaction:false
-- Postgres 12+ forbids using a newly-added ENUM value in any other statement
-- of the same transaction (SQLSTATE 55P04, "unsafe use of new value of enum
-- type"). dbmate sends every statement of a migration file to libpq in a
-- single Exec, which behaves as one implicit transaction even with the
-- transaction:false directive — so the ALTER TYPE ADD VALUE lives in its own
-- migration file, with no other statement referencing the value.
--
-- 'no_data' is the terminal outcome for a work_ticket whose step legitimately
-- produced no data (an empty FASTQ well — a blank, no-template control, or
-- failed-yield well). It is distinct from 'failed': a no_data ticket carries
-- NULL failure_* columns and is counted in its own pool-completion bucket so a
-- plate full of empty wells can still reach a "done" signal. Mirrored by
-- qiita_common.models.WorkTicketState; the two value sets are kept in lockstep
-- by tests — change both in the same PR.

ALTER TYPE qiita.work_ticket_state ADD VALUE IF NOT EXISTS 'no_data';

-- migrate:down transaction:false
-- A Postgres ENUM value cannot be removed without recreating the type.
-- 'no_data' stays in the ENUM after down. Safe: any rows carrying it must be
-- re-dispositioned before a down that would tighten the type, which this
-- migration does not attempt.
