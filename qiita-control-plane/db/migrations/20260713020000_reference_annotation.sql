-- Postgres twin of the DuckLake `reference_annotation` table: which features of a
-- reference are ANNOTATED INTERVALS (a SynDNA insert on its plasmid, a gene on a
-- chromosome) rather than whole sequences.
--
-- Why this table has to exist, rather than the lake one being enough:
--
-- An annotated interval is minted a feature_idx of its own (from the canonical hash
-- of the extracted sub-sequence), but it is deliberately NOT written to
-- qiita.reference_membership — membership is what gets INDEXED and aligned against,
-- and reads align to the parent plasmid, never to the bare insert. That leaves
-- Postgres with a feature it minted and has no record of: `delete_reference_cascade`
-- computes orphan features purely from reference_membership, so every annotated
-- feature_idx would survive `DELETE /reference/{idx}` forever, referenced by nothing
-- — while the data plane WOULD delete its lake rows. The two stores would disagree
-- about which features exist, which is exactly the desync the cascade's own comment
-- warns against.
--
-- So this is the membership analogue for annotations: the same (reference_idx,
-- feature_idx) claim, kept in a SEPARATE table precisely so that nothing which reads
-- reference_membership — the aligner index builders, the shard planner — can ever
-- pick annotations up by accident. A `kind` column on reference_membership would put
-- inserts one forgotten `WHERE` away from being indexed and competing with their own
-- parent for alignments.
--
-- `parent_feature_idx` is carried (not strictly needed for the GC that motivates the
-- table) because it is the join the control plane needs to answer "which intervals
-- live on this plasmid" without a Flight round-trip. The full per-interval detail
-- (window, strand, attributes) stays in the lake — Postgres holds the CLAIM, the lake
-- holds the DATA, which is the same split reference_membership already uses.

-- migrate:up
CREATE TABLE qiita.reference_annotation (
    reference_idx      BIGINT NOT NULL REFERENCES qiita.reference (reference_idx),
    -- The annotation's IDENTITY within the reference (the GFF3 `ID` attribute).
    --
    -- NOT feature_idx, which is only the identity of the annotation's BYTES. The
    -- distinction is not academic: a bacterial genome carries the 16S rRNA gene in
    -- 5-7 BYTE-IDENTICAL copies, which canonically hash to ONE feature_idx. Keying
    -- on feature_idx would make those copies collide, so a PK of
    -- (reference_idx, feature_idx) cannot represent any real bacterial genome
    -- annotation — while working perfectly on the SynDNA plasmids that motivated
    -- the table. That is exactly the SynDNA-shaped design this work exists to avoid.
    --
    -- So: a feature is a SEQUENCE, an annotation is an OCCURRENCE of that sequence
    -- at a place. One feature_idx may legitimately occur many times in one
    -- reference, and a consumer aggregating coverage over the feature sums across
    -- its occurrences.
    annotation_id      TEXT   NOT NULL,
    feature_idx        BIGINT NOT NULL REFERENCES qiita.feature (feature_idx),
    parent_feature_idx BIGINT NOT NULL REFERENCES qiita.feature (feature_idx),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (reference_idx, annotation_id),
    -- An interval that is its own parent spans the whole sequence, so it is not a
    -- sub-interval at all: it hashes to the PARENT's feature_idx, and that feature
    -- IS in reference_membership and IS indexed. hash_sequences rejects this at
    -- ingest (GFF3 `region` / `source` lines); the constraint makes it
    -- unrepresentable.
    CONSTRAINT reference_annotation_not_self CHECK (feature_idx <> parent_feature_idx)
);

CREATE INDEX ON qiita.reference_annotation (feature_idx);
CREATE INDEX ON qiita.reference_annotation (parent_feature_idx);

COMMENT ON TABLE qiita.reference_annotation IS
    'Features of a reference that are ANNOTATED INTERVALS of another feature. Holds the '
    'CLAIM (so delete_reference_cascade can GC them); the per-interval detail lives in the '
    'DuckLake twin. Keyed by annotation_id, not feature_idx — identical bases share one '
    'feature_idx. See this migration''s header for why it is not reference_membership.';

-- migrate:down
DROP TABLE qiita.reference_annotation;
