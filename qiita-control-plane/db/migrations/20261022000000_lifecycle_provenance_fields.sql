-- Lifecycle provenance CHECKs: a live row holds none of its fields, a withdrawn row all three.

-- migrate:up
ALTER TABLE qiita.mask_definition
  DROP CONSTRAINT mask_definition_deprecation_fields,
  ADD CONSTRAINT mask_definition_deprecation_fields CHECK (
    (status = 'active' AND num_nonnulls(deprecated_at, deprecated_by_idx, deprecation_reason) = 0)
    OR (status = 'deprecated' AND num_nonnulls(deprecated_at, deprecated_by_idx, deprecation_reason) = 3)
  );

ALTER TABLE qiita.mask_sample
  DROP CONSTRAINT mask_sample_invalidation_fields,
  ADD CONSTRAINT mask_sample_invalidation_fields CHECK (
    (state <> 'invalidated' AND num_nonnulls(invalidated_at, invalidated_by_idx, invalidation_reason) = 0)
    OR (state = 'invalidated' AND num_nonnulls(invalidated_at, invalidated_by_idx, invalidation_reason) = 3)
  );

ALTER TABLE qiita.processing
  DROP CONSTRAINT processing_deprecation_fields,
  ADD CONSTRAINT processing_deprecation_fields CHECK (
    (status = 'active' AND num_nonnulls(deprecated_at, deprecated_by_idx, deprecation_reason) = 0)
    OR (status = 'deprecated' AND num_nonnulls(deprecated_at, deprecated_by_idx, deprecation_reason) = 3)
  );

ALTER TABLE qiita.assembly_sample
  DROP CONSTRAINT assembly_sample_invalidation_fields,
  ADD CONSTRAINT assembly_sample_invalidation_fields CHECK (
    (state <> 'invalidated' AND num_nonnulls(invalidated_at, invalidated_by_idx, invalidation_reason) = 0)
    OR (state = 'invalidated' AND num_nonnulls(invalidated_at, invalidated_by_idx, invalidation_reason) = 3)
  );

-- migrate:down
ALTER TABLE qiita.mask_definition
  DROP CONSTRAINT mask_definition_deprecation_fields,
  ADD CONSTRAINT mask_definition_deprecation_fields CHECK (
    (status = 'deprecated')
      = (deprecated_at IS NOT NULL
         AND deprecated_by_idx IS NOT NULL
         AND deprecation_reason IS NOT NULL)
  );

ALTER TABLE qiita.mask_sample
  DROP CONSTRAINT mask_sample_invalidation_fields,
  ADD CONSTRAINT mask_sample_invalidation_fields CHECK (
    (state = 'invalidated')
      = (invalidated_at IS NOT NULL
         AND invalidated_by_idx IS NOT NULL
         AND invalidation_reason IS NOT NULL)
  );

ALTER TABLE qiita.processing
  DROP CONSTRAINT processing_deprecation_fields,
  ADD CONSTRAINT processing_deprecation_fields CHECK (
    (status = 'deprecated')
      = (deprecated_at IS NOT NULL
         AND deprecated_by_idx IS NOT NULL
         AND deprecation_reason IS NOT NULL)
  );

ALTER TABLE qiita.assembly_sample
  DROP CONSTRAINT assembly_sample_invalidation_fields,
  ADD CONSTRAINT assembly_sample_invalidation_fields CHECK (
    (state = 'invalidated')
      = (invalidated_at IS NOT NULL
         AND invalidated_by_idx IS NOT NULL
         AND invalidation_reason IS NOT NULL)
  );
