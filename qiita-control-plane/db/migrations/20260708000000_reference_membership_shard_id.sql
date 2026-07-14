-- migrate:up

-- Record the shard planner's feature→shard assignment for a sharded *analysis*
-- reference. A nullable `shard_id` on the (reference_idx, feature_idx) junction
-- discriminates the row:
--   * NULL    -> the feature is not assigned to a shard: an unsharded reference
--                (e.g. a host reference), or a deferred/no-genome feature that
--                the current sharding pass does not cover;
--   * 0..N-1  -> the lineage-sorted shard index the feature belongs to.
--
-- Additive and backward-compatible: nullable with no default, so existing
-- membership rows and the pre-existing 2-column INSERT path stay valid
-- (shard_id NULL). A feature maps to at most one shard within a reference, so
-- the assignment rides the existing (reference_idx, feature_idx) PK rather than
-- a new table. No `shard_count` column (it is COUNT(DISTINCT shard_id) per
-- reference); the assignment is re-derivable from the deterministic planner, so
-- no identity table and no new UNIQUE are warranted. Mirrors the nullable
-- `reference_index.shard_id` discriminant added for the index registration side.
--
-- shard_id is a plain INTEGER + CHECK, not a Postgres ENUM and not an FK.
ALTER TABLE qiita.reference_membership
    ADD COLUMN shard_id INTEGER;
ALTER TABLE qiita.reference_membership
    ADD CONSTRAINT reference_membership_shard_id_nonneg
    CHECK (shard_id IS NULL OR shard_id >= 0);

-- migrate:down
ALTER TABLE qiita.reference_membership
    DROP CONSTRAINT IF EXISTS reference_membership_shard_id_nonneg;
ALTER TABLE qiita.reference_membership
    DROP COLUMN IF EXISTS shard_id;
