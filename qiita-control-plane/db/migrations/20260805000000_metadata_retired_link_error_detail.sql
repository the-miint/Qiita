-- migrate:up

-- =============================================================================
-- Tag the retired-link rejections on *_metadata with a structured DETAIL so a
-- route can identify them.
--
-- The MESSAGE stays human-readable; everything a route acts on goes in DETAIL as
-- comma-separated key=value pairs. The `trigger` key carries the raising
-- function's name -- a stable schema identifier -- so a route decides WHICH
-- rejection this is without matching message prose, and the idx keys let it name
-- the entity and study. Both rejection branches carry the same trigger key: a
-- caller that reaches either one has no writable link to the study, which is one
-- outcome on the wire.
--
-- Only the two functions change. Both the INSERT and the UPDATE triggers execute
-- them, so both statement kinds gain the DETAIL at once, and every ERRCODE stays
-- 'P0001' -- what already catches these errors keeps catching them.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.biosample_metadata_reject_if_link_retired()
RETURNS TRIGGER AS $$
DECLARE
    link_retired BOOLEAN;
    field_study_idx BIGINT;
    rejection_detail TEXT;
BEGIN
    SELECT bsf.study_idx
      INTO field_study_idx
      FROM qiita.biosample_study_field bsf
     WHERE bsf.idx = NEW.biosample_study_field_idx;

    SELECT bts.retired
      INTO link_retired
      FROM qiita.biosample_to_study bts
     WHERE bts.biosample_idx = NEW.biosample_idx
       AND bts.study_idx = field_study_idx;

    -- Built once: both rejections below report the same trigger and pair.
    rejection_detail := format(
        'trigger=biosample_metadata_reject_if_link_retired, biosample_idx=%s, study_idx=%s',
        NEW.biosample_idx, field_study_idx
    );

    IF link_retired IS NULL THEN
        RAISE EXCEPTION 'biosample_metadata refers to (biosample=%, study=%) but no biosample_to_study row exists',
            NEW.biosample_idx, field_study_idx
            USING ERRCODE = 'P0001', DETAIL = rejection_detail;
    END IF;

    IF link_retired = true THEN
        RAISE EXCEPTION 'biosample_metadata cannot be written: biosample_to_study(%, %) is retired',
            NEW.biosample_idx, field_study_idx
            USING ERRCODE = 'P0001', DETAIL = rejection_detail;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_reject_if_link_retired()
RETURNS TRIGGER AS $$
DECLARE
    link_retired BOOLEAN;
    field_study_idx BIGINT;
    rejection_detail TEXT;
BEGIN
    SELECT sssf.study_idx
      INTO field_study_idx
      FROM qiita.prep_sample_study_field sssf
     WHERE sssf.idx = NEW.prep_sample_study_field_idx;

    SELECT ssts.retired
      INTO link_retired
      FROM qiita.prep_sample_to_study ssts
     WHERE ssts.prep_sample_idx = NEW.prep_sample_idx
       AND ssts.study_idx = field_study_idx;

    -- Built once: both rejections below report the same trigger and pair.
    rejection_detail := format(
        'trigger=prep_sample_metadata_reject_if_link_retired, prep_sample_idx=%s, study_idx=%s',
        NEW.prep_sample_idx, field_study_idx
    );

    IF link_retired IS NULL THEN
        RAISE EXCEPTION 'prep_sample_metadata refers to (prep_sample=%, study=%) but no prep_sample_to_study row exists',
            NEW.prep_sample_idx, field_study_idx
            USING ERRCODE = 'P0001', DETAIL = rejection_detail;
    END IF;

    IF link_retired = true THEN
        RAISE EXCEPTION 'prep_sample_metadata cannot be written: prep_sample_to_study(%, %) is retired',
            NEW.prep_sample_idx, field_study_idx
            USING ERRCODE = 'P0001', DETAIL = rejection_detail;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- migrate:down

-- Restore both bodies without the DETAIL clause. The triggers are untouched in
-- either direction, so the down leaves the same two functions in place with the
-- rejections carrying no structured DETAIL.

CREATE OR REPLACE FUNCTION qiita.biosample_metadata_reject_if_link_retired()
RETURNS TRIGGER AS $$
DECLARE
    link_retired BOOLEAN;
    field_study_idx BIGINT;
BEGIN
    SELECT bsf.study_idx
      INTO field_study_idx
      FROM qiita.biosample_study_field bsf
     WHERE bsf.idx = NEW.biosample_study_field_idx;

    SELECT bts.retired
      INTO link_retired
      FROM qiita.biosample_to_study bts
     WHERE bts.biosample_idx = NEW.biosample_idx
       AND bts.study_idx = field_study_idx;

    IF link_retired IS NULL THEN
        RAISE EXCEPTION 'biosample_metadata refers to (biosample=%, study=%) but no biosample_to_study row exists',
            NEW.biosample_idx, field_study_idx;
    END IF;

    IF link_retired = true THEN
        RAISE EXCEPTION 'biosample_metadata cannot be written: biosample_to_study(%, %) is retired',
            NEW.biosample_idx, field_study_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_reject_if_link_retired()
RETURNS TRIGGER AS $$
DECLARE
    link_retired BOOLEAN;
    field_study_idx BIGINT;
BEGIN
    SELECT sssf.study_idx
      INTO field_study_idx
      FROM qiita.prep_sample_study_field sssf
     WHERE sssf.idx = NEW.prep_sample_study_field_idx;

    SELECT ssts.retired
      INTO link_retired
      FROM qiita.prep_sample_to_study ssts
     WHERE ssts.prep_sample_idx = NEW.prep_sample_idx
       AND ssts.study_idx = field_study_idx;

    IF link_retired IS NULL THEN
        RAISE EXCEPTION 'prep_sample_metadata refers to (prep_sample=%, study=%) but no prep_sample_to_study row exists',
            NEW.prep_sample_idx, field_study_idx;
    END IF;

    IF link_retired = true THEN
        RAISE EXCEPTION 'prep_sample_metadata cannot be written: prep_sample_to_study(%, %) is retired',
            NEW.prep_sample_idx, field_study_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
