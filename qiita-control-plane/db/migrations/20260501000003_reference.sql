-- migrate:up

-- =============================================================================
-- REFERENCE DATABASES (sequence references and taxonomy authorities)
-- =============================================================================
-- A reference is a (name, version) pair. `kind` distinguishes sequence
-- references from taxonomy authorities. Tip-to-feature mapping for
-- phylogenies lives in DuckLake (the reference_phylogeny table stores
-- feature_idx directly on tip nodes), not in Postgres.

CREATE TABLE qiita.references (
    reference_idx   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name            TEXT        NOT NULL,
    version         TEXT        NOT NULL,
    kind            TEXT        NOT NULL CHECK (kind IN ('sequence_reference', 'taxonomy_authority')),
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'hashing', 'minting', 'loading', 'active', 'failed')),
    created_by_idx  BIGINT      NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE TABLE qiita.genomes (
    genome_idx BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    source     TEXT        NOT NULL,
    source_id  TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_id)
);

CREATE TABLE qiita.features (
    feature_idx   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    sequence_hash UUID        NOT NULL UNIQUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE qiita.reference_membership (
    reference_idx BIGINT NOT NULL REFERENCES qiita.references (reference_idx),
    feature_idx   BIGINT NOT NULL REFERENCES qiita.features (feature_idx),
    PRIMARY KEY (reference_idx, feature_idx)
);

CREATE INDEX ON qiita.reference_membership (feature_idx);

CREATE TABLE qiita.feature_genome (
    feature_idx BIGINT NOT NULL REFERENCES qiita.features (feature_idx) UNIQUE,
    genome_idx  BIGINT NOT NULL REFERENCES qiita.genomes (genome_idx),
    PRIMARY KEY (feature_idx, genome_idx)
);

CREATE INDEX ON qiita.feature_genome (genome_idx);


-- migrate:down

DROP TABLE IF EXISTS qiita.feature_genome;
DROP TABLE IF EXISTS qiita.reference_membership;
DROP TABLE IF EXISTS qiita.features;
DROP TABLE IF EXISTS qiita.genomes;
DROP TABLE IF EXISTS qiita.references;
