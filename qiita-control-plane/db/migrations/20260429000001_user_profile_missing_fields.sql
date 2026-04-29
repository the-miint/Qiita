-- migrate:up

-- =============================================================================
-- qiita.user_profile_missing_fields — diagnostic helper for profile_complete
-- =============================================================================
-- The `profile_complete` generated column on qiita.user (defined in
-- 20260426000000_auth.sql:79) returns TRUE iff affiliation, address, and
-- phone are all non-empty. Routes that need to surface *which* fields are
-- empty (currently `POST /auth/pat`'s 422 body) used to duplicate the
-- field list in Python, which would silently rot if a 4th required field
-- were added to the generated column. Calling this function from SQL
-- keeps the field list in exactly one place — this migration — alongside
-- the generated column expression.
--
-- Add a new required field by:
--   1. Adding it to the `profile_complete` GENERATED expression on
--      qiita.user (would be a column ADD + a generated-column update via
--      a follow-up migration).
--   2. Adding the matching `CASE WHEN <new> = '' THEN '<new>' END` line
--      to the ARRAY below.

CREATE FUNCTION qiita.user_profile_missing_fields(
    affiliation TEXT, address TEXT, phone TEXT
) RETURNS TEXT[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT array_remove(ARRAY[
        CASE WHEN affiliation = '' THEN 'affiliation' END,
        CASE WHEN address     = '' THEN 'address'     END,
        CASE WHEN phone       = '' THEN 'phone'       END
    ], NULL)
$$;


-- migrate:down

DROP FUNCTION IF EXISTS qiita.user_profile_missing_fields(TEXT, TEXT, TEXT);
