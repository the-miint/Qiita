-- migrate:up

-- Extend the reference_index.index_type allow-list to admit the minimap2
-- sidecar host-filter index alongside rype. index_type is plain TEXT + CHECK
-- (not a Postgres ENUM, same rationale as reference.kind/status), so a new
-- aligner index type is a CHECK migration only — register_index and the
-- ReferenceIndex Pydantic model are already generic over the string.
--
-- The original inline column CHECK (migration 20260601000002) is auto-named
-- `reference_index_index_type_check` by Postgres. Drop it (IF EXISTS guards
-- against a divergent name) and re-add it NAMED so the down side is symmetric.
ALTER TABLE qiita.reference_index
    DROP CONSTRAINT IF EXISTS reference_index_index_type_check;
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_index_type_check
    CHECK (index_type IN ('rype', 'minimap2'));

-- migrate:down

-- Safe only while no 'minimap2' rows exist (they don't until the index-build
-- work lands in a later PR): re-adding the 'rype'-only CHECK would be violated
-- by any existing 'minimap2' row.
ALTER TABLE qiita.reference_index
    DROP CONSTRAINT IF EXISTS reference_index_index_type_check;
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_index_type_check
    CHECK (index_type IN ('rype'));
