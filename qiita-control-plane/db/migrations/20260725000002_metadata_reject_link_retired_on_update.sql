-- migrate:up

-- =============================================================================
-- Extend the non-retired-link invariant on *_metadata from INSERT to value
-- UPDATE.
--
-- The invariant is that no metadata may be written for an (entity, study) pair
-- whose *_to_study link is retired. The check's verdict rests on that link's
-- retired flag, which lives outside the metadata row and can flip to true long
-- after the row was inserted, so freezing the row's own key columns does not
-- freeze the verdict. An overwrite is a new write and needs re-checking; before
-- metadata values could be overwritten in place, INSERT was the only way for
-- new data to arrive, and guarding it was sufficient.
--
-- Re-checking in the DB rather than only before the write is what makes the
-- guard atomic with the write: a caller that checks the link, then writes,
-- leaves a window in which the link can be retired between the two statements.
--
-- Both trigger functions read only NEW.*, which Postgres populates on UPDATE as
-- well, so they are reused here unchanged. The column list matches the
-- *_metadata_apply_field_contract_update triggers.
-- =============================================================================

CREATE TRIGGER biosample_metadata_reject_if_link_retired_update
    BEFORE UPDATE OF biosample_study_field_idx, value_text, value_numeric,
                     value_boolean, value_date, value_terminology_term_idx,
                     value_missing_reason_idx
        ON qiita.biosample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.biosample_metadata_reject_if_link_retired();

COMMENT ON TRIGGER biosample_metadata_reject_if_link_retired_update
    ON qiita.biosample_metadata IS
    'Rejects a value overwrite whose biosample_to_study link is retired. '
    'Scoped to the value columns and the source field on purpose: firing on '
    'every UPDATE would also reject the global_field_idx write done by '
    'biosample_study_field_propagate_global_link, which must keep working for '
    'a biosample whose link to the field-owning study is retired.';

CREATE TRIGGER prep_sample_metadata_reject_if_link_retired_update
    BEFORE UPDATE OF prep_sample_study_field_idx, value_text, value_numeric,
                     value_boolean, value_date, value_terminology_term_idx,
                     value_missing_reason_idx
        ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_reject_if_link_retired();

COMMENT ON TRIGGER prep_sample_metadata_reject_if_link_retired_update
    ON qiita.prep_sample_metadata IS
    'Rejects a value overwrite whose prep_sample_to_study link is retired. '
    'Scoped to the value columns and the source field on purpose: firing on '
    'every UPDATE would also reject the global_field_idx write done by '
    'prep_sample_study_field_propagate_global_link, which must keep working for '
    'a prep_sample whose link to the field-owning study is retired.';


-- migrate:down

DROP TRIGGER IF EXISTS biosample_metadata_reject_if_link_retired_update
    ON qiita.biosample_metadata;
DROP TRIGGER IF EXISTS prep_sample_metadata_reject_if_link_retired_update
    ON qiita.prep_sample_metadata;

-- The trigger functions are left in place: the INSERT triggers still use them.
