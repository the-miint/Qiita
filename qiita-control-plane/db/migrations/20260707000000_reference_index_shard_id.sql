-- migrate:up

-- Teach qiita.reference_index to represent one row per shard for a sharded
-- *analysis* reference index. A nullable `shard_id` discriminates the row:
--   * NULL      -> today's unsharded whole-reference index (host rype/minimap2);
--   * 0..N-1    -> one row per shard of a sharded analysis index.
--
-- Additive and backward-compatible: nullable with no default, so existing rows
-- and the pre-existing 4-column INSERT path stay valid (shard_id NULL). There
-- is no shard_count column on purpose — it is just COUNT(*) per
-- (reference_idx, index_type), and a reference only reaches `active` after every
-- shard registers, so an active reference always carries all its shards.
--
-- shard_id is a plain INTEGER + CHECK, not a Postgres ENUM and not an FK: shards
-- are deterministic/re-derivable, so a shard needs no identity table. No new
-- UNIQUE — the existing register_index idempotency key
-- (reference_idx, index_type, fs_path) already dedups shard rows on replay
-- (fs_path is shard-unique via `.../shards/{shard_id}/`), and the no-UNIQUE
-- append-generation design is deliberate.
ALTER TABLE qiita.reference_index
    ADD COLUMN shard_id INTEGER;
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_shard_id_nonneg
    CHECK (shard_id IS NULL OR shard_id >= 0);

-- migrate:down
ALTER TABLE qiita.reference_index
    DROP CONSTRAINT IF EXISTS reference_index_shard_id_nonneg;
ALTER TABLE qiita.reference_index
    DROP COLUMN IF EXISTS shard_id;
