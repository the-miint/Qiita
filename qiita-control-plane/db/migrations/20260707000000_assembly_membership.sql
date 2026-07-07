-- migrate:up

-- The association between a prep_sample's assembly and the deduped contig
-- features it produced — the assembly analogue of qiita.reference_membership.
--
-- A contig is a qiita.feature (content-hash deduped, minted by the SHARED
-- mint-features path — assembled contigs join the same global feature space as
-- reference sequences, so identical bytes collapse to one feature_idx and
-- feature_idx bridges assembly results to reference/read data). This junction
-- records which features a sample's assembly contains and in which bin — a
-- circular LCG genome or a refined MAG. Anchored on prep_sample (the assembly's
-- owner) rather than a named reference.
--
-- `kind` is intentionally plain TEXT (no Postgres ENUM, no CHECK): the value set
-- ('LCG'/'MAG' today) is still in flux — plasmids / fragments / sub-512kb
-- circulars may become their own kinds — and is owned by the producer. A Python
-- twin can exist without triggering the enum-parity rule (TEXT-backed, per
-- CLAUDE.md).
CREATE TABLE qiita.assembly_membership (
    prep_sample_idx BIGINT NOT NULL REFERENCES qiita.prep_sample (idx) ON DELETE CASCADE,
    kind            TEXT   NOT NULL,
    bin_id          TEXT   NOT NULL,
    feature_idx     BIGINT NOT NULL REFERENCES qiita.feature (feature_idx),
    PRIMARY KEY (prep_sample_idx, kind, bin_id, feature_idx)
);

-- Feature-first lookup (which samples/bins contain a given contig), mirroring the
-- reference_membership (feature_idx) index.
CREATE INDEX ON qiita.assembly_membership (feature_idx);

-- migrate:down

DROP TABLE qiita.assembly_membership;
