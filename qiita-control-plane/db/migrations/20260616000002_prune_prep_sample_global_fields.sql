-- migrate:up

-- =============================================================================
-- PRUNE SEEDED PREP-SAMPLE GLOBAL FIELDS
-- =============================================================================
--
-- The initial prep_sample_global_field seed registered fields that can and should
-- now come from the sequenced_pool. Only title and design_description are realistic
-- here, and both are optional, so the other seven are removed and
-- the two survivors are flipped to not-required.
--
-- Every inbound reference to prep_sample_global_field is ON DELETE RESTRICT
-- (prep_sample_study_field, prep_sample_metadata, prep_sample_field_exception,
-- prep_protocol_field), so this DELETE aborts the migration rather than
-- orphaning anything if a study, value, exception, or protocol already links one
-- of these fields. The deploy pre-check surfaces any such row before migrate.

DELETE FROM qiita.prep_sample_global_field
 WHERE internal_name IN (
    'alias',
    'library_name',
    'library_strategy',
    'library_source',
    'library_selection',
    'library_layout',
    'library_construction_protocol'
);

UPDATE qiita.prep_sample_global_field
   SET required = false
 WHERE internal_name IN ('title', 'design_description');


-- migrate:down

-- Restore the two survivors to their seeded required = true state.
UPDATE qiita.prep_sample_global_field
   SET required = true
 WHERE internal_name IN ('title', 'design_description');

-- Re-seed the seven removed fields with their original seed values
-- (created_by_idx=1 references the system principal seeded earlier).
INSERT INTO qiita.prep_sample_global_field (
    internal_name, display_name, data_type, default_tier, required, created_by_idx
) VALUES
    ('alias',                         'Alias',                         'text', 'public', true, 1),
    ('library_name',                  'Library name',                  'text', 'public', true, 1),
    ('library_strategy',              'Library strategy',              'text', 'public', true, 1),
    ('library_source',                'Library source',                'text', 'public', true, 1),
    ('library_selection',             'Library selection',             'text', 'public', true, 1),
    ('library_layout',                'Library layout',                'text', 'public', true, 1),
    ('library_construction_protocol', 'Library construction protocol', 'text', 'public', true, 1);
