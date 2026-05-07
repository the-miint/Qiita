-- migrate:up

-- =============================================================================
-- BIOSAMPLE GLOBAL FIELD SEED
-- =============================================================================
--
-- Bootstraps the cross-study biosample concept registry with the minimum set
-- of fields every biosample needs in order to be submittable to BioSample.
-- created_by_idx = 1 is the seeded system principal. ON CONFLICT DO NOTHING
-- keeps the seed re-runnable: if dbmate's tracking row is ever lost (manual
-- rollback, restored snapshot) and this migration is reapplied while the
-- seed rows still exist, both unique constraints (internal_name and
-- display_name) absorb the conflict instead of erroring.
INSERT INTO qiita.biosample_global_field
    (internal_name, display_name, data_type, required, created_by_idx)
VALUES
    ('collection_date', 'collection date', 'date', true, 1),
    ('geographic_location_country_or_sea', 'geographic location (country and/or sea)', 'text', true, 1),
    ('geographic_location_latitude', 'geographic location (latitude)', 'numeric', true, 1),
    ('geographic_location_longitude', 'geographic location (longitude)', 'numeric', true, 1),
    ('broad_scale_environmental_context', 'broad-scale environmental context', 'text', true, 1),
    ('local_environmental_context', 'local environmental context', 'text', true, 1),
    ('environmental_medium', 'environmental medium', 'text', true, 1),
    ('taxon_id', 'taxon id', 'text', true, 1)
ON CONFLICT DO NOTHING;


-- migrate:down

DELETE FROM qiita.biosample_global_field
WHERE internal_name IN (
    'collection_date',
    'geographic_location_country_or_sea',
    'geographic_location_latitude',
    'geographic_location_longitude',
    'broad_scale_environmental_context',
    'local_environmental_context',
    'environmental_medium',
    'taxon_id'
);
