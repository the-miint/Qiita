-- Add the `observed_feature` kind to qiita.exported_feature: a feature Qiita
-- observed itself and that names nothing outside the load that produced it — an
-- amplicon sequence variant, today. The creating migration
-- (20260813000000_exported_feature.sql) already anticipated this ("a feature Qiita
-- observed itself … falls back to a minted 'QF<idx>'"), but its shape did not fit:
-- a `feature`-kind row must pair with a reference, and a de-novo ASV has none.
--
-- This kind is `feature_idx` alone (no reference, no genome). It never carries an
-- accession — an ASV has no name outside its load — so its handle is always
-- 'QF' || idx, which the existing GENERATED `export_feature_id` already produces for
-- any row where `accession_published` is false. So no accession machinery changes;
-- only the shape constraints and the per-kind live-uniqueness index.

-- migrate:up

-- Widen the entity-kind whitelist.
ALTER TABLE qiita.exported_feature
    DROP CONSTRAINT exported_feature_entity_kind_known;
ALTER TABLE qiita.exported_feature
    ADD CONSTRAINT exported_feature_entity_kind_known
    CHECK (entity_kind IN ('genome', 'feature', 'observed_feature'));

-- Replace the reference/feature biconditional. The old rule — a feature_idx is set
-- iff a reference_idx is — forbade a reference-less feature. The new rule ties the
-- reference to the `feature` kind precisely: reference_idx is present exactly when
-- the row is a reference feature, so a `genome` has neither, a `feature` has both
-- (one_kind already pins feature_idx), and an `observed_feature` has feature_idx
-- without a reference. A retired row is exempt, as every other shape CHECK here is,
-- because a detach NULLs the columns these test.
ALTER TABLE qiita.exported_feature
    DROP CONSTRAINT exported_feature_reference_pairs_with_feature;
ALTER TABLE qiita.exported_feature
    ADD CONSTRAINT exported_feature_reference_iff_feature_kind
    CHECK (retired OR (reference_idx IS NOT NULL) = (entity_kind = 'feature'));

-- One live identifier per observed feature, so the mint is an idempotent upsert.
-- A dedicated index because `exported_feature_live_reference_feature` keys on
-- (reference_idx, feature_idx) and an observed row's reference_idx is NULL, which a
-- unique index treats as distinct — two live rows for one feature_idx would slip
-- through it. PARTIAL on `NOT retired` for the same reason its siblings are: a
-- retired tuple can be re-minted rather than colliding with its own history.
CREATE UNIQUE INDEX exported_feature_live_observed_feature
    ON qiita.exported_feature (feature_idx)
    WHERE NOT retired AND entity_kind = 'observed_feature';

-- The published-namespace index (exported_feature_export_feature_id_unique) needs no
-- change: an observed feature's handle is 'QF' || idx, unique by idx under any
-- predicate, so it can neither collide with nor reserve an accession.

-- migrate:down

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
