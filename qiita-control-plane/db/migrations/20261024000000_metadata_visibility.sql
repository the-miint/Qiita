-- migrate:up

-- The metadata visibility rule as SQL. Two scalar functions compute a field's
-- tier, and the greater of that and the tiers of a value's exception rows. Per
-- entity kind, one set-returning function reports, for each EAV row, the tier
-- it requires, the tier the caller holds on its study, and whether the caller
-- may read it. The rule itself is defined in docs/architecture/data-model.md,
-- "Metadata visibility tiers"; this file implements it.
--
-- The caller's tiers arrive as parallel arrays (study_idx, tier) plus the tier
-- held on every study not in them.
--
-- Both visibility functions are LANGUAGE sql, STABLE, not STRICT, not SECURITY
-- DEFINER, carry no SET clause, and have a single SELECT for a body: those are
-- the conditions under which the planner inlines a set-returning function, so
-- a caller's `WHERE m.<entity>_idx = $n` reaches the base table's index rather
-- than filtering a materialized scan of every row. Columns in the bodies are
-- table-qualified because RETURNS TABLE names are in scope as parameters.

CREATE FUNCTION qiita.metadata_field_tier(
    global_default_tier qiita.tier,
    local_tier_override qiita.tier,
    study_default_tier qiita.tier
) RETURNS qiita.tier
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$ SELECT COALESCE(global_default_tier, local_tier_override, study_default_tier) $$;

COMMENT ON FUNCTION qiita.metadata_field_tier(qiita.tier, qiita.tier, qiita.tier) IS
    'The tier a metadata field requires, before any per-value exception. '
    'Definition: docs/architecture/data-model.md, "Metadata visibility tiers".';

-- GREATEST ignores NULLs, so a value with no exception row requires its field
-- tier, and an exception below the field tier changes nothing.
CREATE FUNCTION qiita.metadata_required_tier(
    field_tier qiita.tier,
    global_exception_tier qiita.tier,
    local_exception_tier qiita.tier
) RETURNS qiita.tier
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$ SELECT GREATEST(field_tier, global_exception_tier, local_exception_tier) $$;

COMMENT ON FUNCTION qiita.metadata_required_tier(qiita.tier, qiita.tier, qiita.tier) IS
    'The greater of a metadata field''s tier and the tier_override of any '
    '*_field_exception row on one value of that field. Definition: '
    'docs/architecture/data-model.md, "Metadata visibility tiers".';

CREATE FUNCTION qiita.biosample_metadata_visibility(
    caller_study_idxs bigint[],
    caller_study_tiers qiita.tier[],
    caller_default_tier qiita.tier
) RETURNS TABLE (
    idx bigint,
    biosample_idx bigint,
    biosample_study_field_idx bigint,
    global_field_idx bigint,
    study_idx bigint,
    required_tier qiita.tier,
    caller_tier qiita.tier,
    visible boolean,
    value_text text,
    value_numeric numeric,
    value_boolean boolean,
    value_date date,
    value_terminology_term_idx bigint,
    value_missing_reason_idx bigint,
    updated_at timestamptz
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $$
    SELECT m.idx,
           m.biosample_idx,
           m.biosample_study_field_idx,
           m.global_field_idx,
           sf.study_idx,
           r.required_tier,
           r.caller_tier,
           r.caller_tier >= r.required_tier,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_text END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_numeric END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_boolean END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_date END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_terminology_term_idx END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_missing_reason_idx END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.updated_at END
      FROM qiita.biosample_metadata m
      JOIN qiita.biosample_study_field sf ON sf.idx = m.biosample_study_field_idx
      JOIN qiita.study s ON s.idx = sf.study_idx
      LEFT JOIN qiita.biosample_global_field gf ON gf.idx = m.global_field_idx
      LEFT JOIN qiita.biosample_field_exception eg
             ON eg.biosample_idx = m.biosample_idx
            AND eg.global_field_idx = m.global_field_idx
      LEFT JOIN qiita.biosample_field_exception el
             ON el.biosample_idx = m.biosample_idx
            AND el.biosample_study_field_idx = m.biosample_study_field_idx
      LEFT JOIN unnest(caller_study_idxs, caller_study_tiers) AS ct(study_idx, tier)
             ON ct.study_idx = sf.study_idx
     -- The owner-biosample-id floor (docs/architecture/data-model.md). The
     -- literal is repositories.biosample.OWNER_BIOSAMPLE_ID_TIER_OVERRIDE,
     -- which a test pins it to.
     CROSS JOIN LATERAL (VALUES (
               GREATEST(
                   qiita.metadata_required_tier(
                       qiita.metadata_field_tier(gf.default_tier, sf.tier_override, s.default_tier),
                       eg.tier_override,
                       el.tier_override
                   ),
                   CASE WHEN m.is_owner_biosample_id THEN 'member'::qiita.tier END
               ),
               COALESCE(ct.tier, caller_default_tier)
           )) AS r(required_tier, caller_tier)
$$;

COMMENT ON FUNCTION qiita.biosample_metadata_visibility(bigint[], qiita.tier[], qiita.tier) IS
    'Every biosample_metadata row with the tier it requires, the tier the caller '
    'holds on its study, and whether the caller may read it; the value columns '
    'and updated_at are NULL on a row the caller may not read. Rule: '
    'docs/architecture/data-model.md, "Metadata visibility tiers".';

CREATE FUNCTION qiita.prep_sample_metadata_visibility(
    caller_study_idxs bigint[],
    caller_study_tiers qiita.tier[],
    caller_default_tier qiita.tier
) RETURNS TABLE (
    idx bigint,
    prep_sample_idx bigint,
    prep_sample_study_field_idx bigint,
    global_field_idx bigint,
    study_idx bigint,
    required_tier qiita.tier,
    caller_tier qiita.tier,
    visible boolean,
    value_text text,
    value_numeric numeric,
    value_boolean boolean,
    value_date date,
    value_terminology_term_idx bigint,
    value_missing_reason_idx bigint,
    updated_at timestamptz
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $$
    SELECT m.idx,
           m.prep_sample_idx,
           m.prep_sample_study_field_idx,
           m.global_field_idx,
           sf.study_idx,
           r.required_tier,
           r.caller_tier,
           r.caller_tier >= r.required_tier,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_text END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_numeric END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_boolean END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_date END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_terminology_term_idx END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.value_missing_reason_idx END,
           CASE WHEN r.caller_tier >= r.required_tier THEN m.updated_at END
      FROM qiita.prep_sample_metadata m
      JOIN qiita.prep_sample_study_field sf ON sf.idx = m.prep_sample_study_field_idx
      JOIN qiita.study s ON s.idx = sf.study_idx
      LEFT JOIN qiita.prep_sample_global_field gf ON gf.idx = m.global_field_idx
      LEFT JOIN qiita.prep_sample_field_exception eg
             ON eg.prep_sample_idx = m.prep_sample_idx
            AND eg.global_field_idx = m.global_field_idx
      LEFT JOIN qiita.prep_sample_field_exception el
             ON el.prep_sample_idx = m.prep_sample_idx
            AND el.prep_sample_study_field_idx = m.prep_sample_study_field_idx
      LEFT JOIN unnest(caller_study_idxs, caller_study_tiers) AS ct(study_idx, tier)
             ON ct.study_idx = sf.study_idx
     CROSS JOIN LATERAL (VALUES (
               qiita.metadata_required_tier(
                   qiita.metadata_field_tier(gf.default_tier, sf.tier_override, s.default_tier),
                   eg.tier_override,
                   el.tier_override
               ),
               COALESCE(ct.tier, caller_default_tier)
           )) AS r(required_tier, caller_tier)
$$;

COMMENT ON FUNCTION qiita.prep_sample_metadata_visibility(bigint[], qiita.tier[], qiita.tier) IS
    'The prep_sample_metadata twin of qiita.biosample_metadata_visibility; same '
    'columns, same rule (a prep_sample has no owner-biosample-id value).';

-- The original table COMMENTs said an exception "downgrades visibility to a
-- specified tier_override", which reads as setting the value's tier. It can
-- only increase it.
COMMENT ON TABLE qiita.biosample_field_exception IS
    'Per-(biosample, field) visibility overrides. Used to restrict the audience for '
    'specific metadata values that need narrower visibility than their field''s general '
    'policy (e.g., a description field where one biosample accidentally received PII). '
    'An exception increases the value''s required tier to tier_override; one at or below '
    'the field''s tier changes nothing. Exceptions on globally-linked metadata are keyed '
    'on global_field_idx so they follow the value across studies; exceptions on purely '
    'study-local metadata are keyed on biosample_study_field_idx. Rule: '
    'docs/architecture/data-model.md, "Metadata visibility tiers".';

COMMENT ON TABLE qiita.prep_sample_field_exception IS
    'Per-(prep_sample, field) visibility overrides. Used to restrict '
    'the audience for specific metadata values that need narrower visibility '
    'than their field''s general policy. An exception increases the value''s required '
    'tier to tier_override; one at or below the field''s tier changes nothing. '
    'Exceptions on globally-linked metadata are keyed on global_field_idx so they '
    'follow the value across studies; exceptions on purely study-local metadata are '
    'keyed on prep_sample_study_field_idx. Rule: docs/architecture/data-model.md, '
    '"Metadata visibility tiers".';

-- migrate:down

COMMENT ON TABLE qiita.prep_sample_field_exception IS
    'Per-(prep_sample, field) visibility overrides. Used to restrict '
    'the audience for specific metadata values that need narrower visibility '
    'than their field''s general policy. An exception downgrades visibility '
    'to a specified tier_override. Exceptions on globally-linked metadata '
    'are keyed on global_field_idx so they follow the value across studies; '
    'exceptions on purely study-local metadata are keyed on '
    'prep_sample_study_field_idx.';

COMMENT ON TABLE qiita.biosample_field_exception IS
    'Per-(biosample, field) visibility overrides. Used to restrict the audience for '
    'specific metadata values that need narrower visibility than their field''s general '
    'policy (e.g., a description field where one biosample accidentally received PII). '
    'An exception downgrades visibility to a specified tier_override. Exceptions on '
    'globally-linked metadata are keyed on global_field_idx so they follow the value '
    'across studies; exceptions on purely study-local metadata are keyed on '
    'biosample_study_field_idx.';

DROP FUNCTION qiita.prep_sample_metadata_visibility(bigint[], qiita.tier[], qiita.tier);
DROP FUNCTION qiita.biosample_metadata_visibility(bigint[], qiita.tier[], qiita.tier);
DROP FUNCTION qiita.metadata_required_tier(qiita.tier, qiita.tier, qiita.tier);
DROP FUNCTION qiita.metadata_field_tier(qiita.tier, qiita.tier, qiita.tier);
