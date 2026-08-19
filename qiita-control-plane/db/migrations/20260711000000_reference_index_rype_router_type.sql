-- migrate:up

-- Admit the whole-reference rype ROUTER (`rype_router`) in the
-- reference_index.index_type allow-list alongside rype / minimap2 / bowtie2.
-- index_type is plain TEXT + CHECK (not a Postgres ENUM, same rationale as
-- reference.kind/status), so a new index type is a CHECK migration only —
-- register_index and the ReferenceIndex Pydantic model are already generic over
-- the string, and there is no enum-parity twin to keep in sync.
--
-- The router is a single multi-bucket rype index over the ENTIRE reference (one
-- bucket per shard) that one `rype_classify` pass turns into the read_to_shard
-- table the sharded aligners consume. It is whole-reference, so its
-- reference_index row carries shard_id NULL (like the host rype/minimap2 rows),
-- distinct from the per-shard analysis-index rows. Its Python twin is
-- INDEX_TYPE_RYPE_ROUTER in qiita_common.models.
--
-- The CHECK constraint `reference_index_index_type_check` was last redefined by
-- 20260709000000 (rype+minimap2+bowtie2). Drop it (IF EXISTS guards a divergent
-- name) and re-add it NAMED with rype_router added so the down side is symmetric.
ALTER TABLE qiita.reference_index
    DROP CONSTRAINT IF EXISTS reference_index_index_type_check;
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_index_type_check
    CHECK (index_type IN ('rype', 'minimap2', 'bowtie2', 'rype_router'));

-- migrate:down

-- Safe only while no 'rype_router' rows exist (they don't until the router
-- build+register wiring lands with this PR): re-adding the three-value CHECK
-- would be violated by any existing 'rype_router' row.
ALTER TABLE qiita.reference_index
    DROP CONSTRAINT IF EXISTS reference_index_index_type_check;
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_index_type_check
    CHECK (index_type IN ('rype', 'minimap2', 'bowtie2'));
