-- migrate:up

-- =============================================================================
-- WORK TICKET shard_id — the sharded-index fan-out discriminant
-- =============================================================================
-- An analysis reference can be indexed as N shards; each shard is built by its
-- own work_ticket. Those N tickets share one (action_id, action_version,
-- reference_idx), so the existing one-in-flight-per-reference partial UNIQUE
-- (20260508000000_work_ticket_disallow_without_delete_indexes.sql) would reject
-- every ticket past the first. shard_id discriminates them.
--
-- shard_id is a plain nullable INTEGER + CHECK, not a Postgres ENUM and not an
-- FK: it is an ordinal within one reference's shard set (the planner assigns
-- 0..N-1), orthogonal to the scope_target tagged union. A non-NULL value is
-- only meaningful on a reference-scoped ticket — the CHECK ties the two so a
-- shard_id can never appear on a study_prep / prep_sample / block / pool ticket.
-- Every existing ticket (all non-sharded actions, and the reference ingest
-- ticket itself) leaves shard_id NULL and is unaffected.
ALTER TABLE qiita.work_ticket
    ADD COLUMN shard_id INTEGER;

ALTER TABLE qiita.work_ticket
    ADD CONSTRAINT work_ticket_shard_id_reference_only
    CHECK (shard_id IS NULL OR (shard_id >= 0 AND scope_target_kind = 'reference'));

COMMENT ON COLUMN qiita.work_ticket.shard_id IS
    'Analysis-index shard ordinal (0..N-1) for a sharded reference build '
    'ticket; NULL for every non-sharded ticket. Only legal on reference scope '
    '(see work_ticket_shard_id_reference_only). Discriminates the N concurrent '
    'same-action build tickets that fan out over one reference so they do not '
    'collide on work_ticket_one_in_flight_per_reference.';

-- Re-partition the per-reference one-in-flight index to EXCLUDE sharded
-- tickets (shard_id IS NULL): the exact one-per-(action, reference) guarantee
-- is preserved for reference-add / host-reference-add / the ingest ticket, all
-- of which leave shard_id NULL. Sharded build tickets are gated by the new
-- per-shard index below instead.
DROP INDEX IF EXISTS qiita.work_ticket_one_in_flight_per_reference;
CREATE UNIQUE INDEX work_ticket_one_in_flight_per_reference
    ON qiita.work_ticket (action_id, action_version, reference_idx)
    WHERE reference_idx IS NOT NULL
      AND shard_id IS NULL
      AND state IN ('pending', 'queued', 'processing');

-- At most one non-terminal ticket per (action, reference, shard): the
-- disallow-without-delete backstop for the fan-out (idempotent redrive relies
-- on this via INSERT ... ON CONFLICT DO NOTHING).
CREATE UNIQUE INDEX work_ticket_one_in_flight_per_shard
    ON qiita.work_ticket (action_id, action_version, reference_idx, shard_id)
    WHERE shard_id IS NOT NULL
      AND state IN ('pending', 'queued', 'processing');


-- migrate:down

DROP INDEX IF EXISTS qiita.work_ticket_one_in_flight_per_shard;
DROP INDEX IF EXISTS qiita.work_ticket_one_in_flight_per_reference;
CREATE UNIQUE INDEX work_ticket_one_in_flight_per_reference
    ON qiita.work_ticket (action_id, action_version, reference_idx)
    WHERE reference_idx IS NOT NULL
      AND state IN ('pending', 'queued', 'processing');
ALTER TABLE qiita.work_ticket DROP CONSTRAINT IF EXISTS work_ticket_shard_id_reference_only;
ALTER TABLE qiita.work_ticket DROP COLUMN IF EXISTS shard_id;
