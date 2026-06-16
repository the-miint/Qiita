-- migrate:up

-- collection_date holds ISO8601-format dates including partial values (e.g. a bare
-- year like '2025'), which a formal date column cannot represent. Rebind the
-- global field from 'date' to 'text' so the value lands in
-- biosample_metadata.value_text. The guarded rebind refuses the flip if any
-- biosample_metadata rows already reference the field.
SELECT qiita.rebind_biosample_global_field_data_type(
    ARRAY['collection_date'],
    'text');


-- migrate:down

SELECT qiita.rebind_biosample_global_field_data_type(
    ARRAY['collection_date'],
    'date');
