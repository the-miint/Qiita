-- migrate:up
-- Tip-to-feature mapping moved to DuckLake: the reference_phylogeny table
-- now stores feature_idx directly on tip nodes, eliminating the need for
-- a separate junction table in Postgres.
DROP TABLE IF EXISTS qiita.phylogeny_tip_feature;

-- migrate:down
CREATE TABLE qiita.phylogeny_tip_feature (
    reference_idx BIGINT NOT NULL REFERENCES qiita.references (reference_idx),
    node_index    BIGINT NOT NULL,
    feature_idx   BIGINT NOT NULL REFERENCES qiita.features (feature_idx),
    PRIMARY KEY (reference_idx, node_index)
);
