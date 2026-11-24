-- Add the `observed_feature` kind to qiita.exported_feature: a feature Qiita
-- observed itself (an amplicon sequence variant), with no reference to pair with.
-- It is `feature_idx` alone and never carries an accession, so its handle is always
-- the GENERATED 'QF' || idx, and the published-namespace index needs no change.

-- migrate:up

ALTER TABLE qiita.exported_feature
    DROP CONSTRAINT exported_feature_entity_kind_known;
ALTER TABLE qiita.exported_feature
    ADD CONSTRAINT exported_feature_entity_kind_known
    CHECK (entity_kind IN ('genome', 'feature', 'observed_feature'));

-- A reference_idx is present exactly when the row is a reference feature: a `genome`
-- has neither, a `feature` has both, an `observed_feature` has feature_idx alone. A
-- retired row is exempt, as every other shape CHECK here is, because a detach NULLs
-- the columns these test.
ALTER TABLE qiita.exported_feature
    DROP CONSTRAINT exported_feature_reference_pairs_with_feature;
ALTER TABLE qiita.exported_feature
    ADD CONSTRAINT exported_feature_reference_iff_feature_kind
    CHECK (retired OR (reference_idx IS NOT NULL) = (entity_kind = 'feature'));

-- One live identifier per observed feature, so the mint is an idempotent upsert. Its
-- own index because the reference-feature one keys on (reference_idx, feature_idx),
-- and a NULL reference_idx is distinct to a unique index.
CREATE UNIQUE INDEX exported_feature_live_observed_feature
    ON qiita.exported_feature (feature_idx)
    WHERE NOT retired AND entity_kind = 'observed_feature';

-- migrate:down

-- The restored checks admit no observed_feature row, retired or not.
DELETE FROM qiita.exported_feature WHERE entity_kind = 'observed_feature';

DROP INDEX qiita.exported_feature_live_observed_feature;

ALTER TABLE qiita.exported_feature
    DROP CONSTRAINT exported_feature_reference_iff_feature_kind;
ALTER TABLE qiita.exported_feature
    ADD CONSTRAINT exported_feature_reference_pairs_with_feature
    CHECK (retired OR (feature_idx IS NULL) = (reference_idx IS NULL));

ALTER TABLE qiita.exported_feature
    DROP CONSTRAINT exported_feature_entity_kind_known;
ALTER TABLE qiita.exported_feature
    ADD CONSTRAINT exported_feature_entity_kind_known
    CHECK (entity_kind IN ('genome', 'feature'));
