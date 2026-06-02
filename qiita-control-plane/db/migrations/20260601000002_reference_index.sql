-- migrate:up

-- A built search index for a reference (today: a rype `.ryxdi` directory used
-- for host-read filtering). The control plane tracks WHERE the index lives and
-- the build parameters; the authoritative manifest (buckets, minimizer params)
-- lives inside the index artifact itself. Mirrors
-- qiita_common.models.ReferenceIndex.
--
-- Deliberately NOT UNIQUE(reference_idx, index_type): once references can grow
-- (rype supports adding buckets; minimap2/bowtie2 can add data without a full
-- reindex), a new generation appends a fresh row and the "current" index is the
-- latest created_at for (reference_idx, index_type). v1 writes exactly one row
-- per reference.
CREATE TABLE qiita.reference_index (
    reference_index_idx BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    -- ON DELETE RESTRICT per the schema-wide convention (no cascading
    -- deletes anywhere in qiita; see tests/routes/test_reference_fk_migration.py
    -- test_no_cascade_on_delete). Dropping a reference requires deleting its
    -- index rows first — deliberate, so index metadata is never silently lost.
    reference_idx       BIGINT      NOT NULL REFERENCES qiita.reference (reference_idx) ON DELETE RESTRICT,
    -- Plain TEXT + CHECK (not a Postgres ENUM), same rationale as
    -- reference.kind/status. Extend the list as new index types ship.
    index_type          TEXT        NOT NULL CHECK (index_type IN ('rype')),
    fs_path             TEXT        NOT NULL,
    params              JSONB       NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON qiita.reference_index (reference_idx);


-- migrate:down

DROP TABLE IF EXISTS qiita.reference_index;
