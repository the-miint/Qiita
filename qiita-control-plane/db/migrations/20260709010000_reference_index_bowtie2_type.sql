-- migrate:up

-- Extend the reference_index.index_type allow-list to admit the bowtie2
-- analysis-alignment index alongside rype and minimap2. index_type is plain
-- TEXT + CHECK (not a Postgres ENUM, same rationale as reference.kind/status),
-- so a new aligner index type is a CHECK migration only — register_index and
-- the ReferenceIndex Pydantic model are already generic over the string.
--
-- Unlike rype/minimap2 (dual-purpose: host-filter AND analysis), bowtie2 is an
-- analysis-only subject index (the per-shard `.bt2` set the sharded aligner
-- consumes); it is deliberately absent from HOST_FILTER_REQUIRED_INDEX_TYPES.
--
-- The CHECK constraint `reference_index_index_type_check` was last redefined by
-- 20260612000000 (rype+minimap2). Drop it (IF EXISTS guards a divergent name)
-- and re-add it NAMED with bowtie2 added so the down side is symmetric.
ALTER TABLE qiita.reference_index
    DROP CONSTRAINT IF EXISTS reference_index_index_type_check;
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_index_type_check
    CHECK (index_type IN ('rype', 'minimap2', 'bowtie2'));

-- migrate:down

-- Safe only while no 'bowtie2' rows exist (they don't until the shard-builder
-- wiring lands in a later PR): re-adding the rype+minimap2 CHECK would be
-- violated by any existing 'bowtie2' row.
ALTER TABLE qiita.reference_index
    DROP CONSTRAINT IF EXISTS reference_index_index_type_check;
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_index_type_check
    CHECK (index_type IN ('rype', 'minimap2'));
