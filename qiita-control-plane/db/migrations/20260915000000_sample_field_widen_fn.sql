-- migrate:up

-- =============================================================================
-- WIDEN A STUDY-LOCAL FIELD TO TEXT
-- =============================================================================
--
-- Declares one study-local field 'text' and moves every value already stored
-- through it into value_text, returning how many metadata rows moved. Text is
-- the only target: every other type can fail to hold a value already stored,
-- so no other direction is expressible here.
--
-- The two statements are not separable. The metadata field-contract trigger
-- checks a row's populated value column against its field's declared type only
-- when the metadata row itself is written, so flipping the declaration alone
-- would leave every existing row populated in a column the declaration no
-- longer names -- and reads decode the column the declaration names, so those
-- values would come back NULL with nothing raised. That is the failure this
-- function exists to prevent, which is why it refuses rather than flips when it
-- cannot also move the values.
--
-- One function for both stacks, taking its two tables and the metadata table's
-- study-field FK column as arguments, in the manner of
-- tg_propagate_unique_in_study: the stacks differ only in those identifiers.
-- The tables arrive as regclass, so an input that names no table is rejected by
-- the cast before a statement runs, and each renders back as the identifier
-- Postgres itself resolved rather than as text this function assembled. The
-- column, having no such type, is quoted through %I. Callers pass frozen
-- constants; an identifier reaching here from request data would be an
-- injection, and no caller may construct one.
--
-- The declared type is read under a lock held to the caller's commit, so every
-- decision below is made against the stored row rather than against a
-- parameter the caller asserted.
--
-- A move also excludes concurrent metadata writers for the caller's
-- transaction, and refuses to run for a caller that has not bounded its wait
-- for them; the reasoning is on the lock itself. The cost is real: the lock is
-- table-wide, so the whole system's metadata writes stall for the move, and
-- the move itself already locks up to two rows per value it moves: the
-- metadata row, and the parent sample its touch trigger bumps. A widen is
-- administrative and rare, which is what makes that trade payable.
--
-- What is refused is what can never be done, not what is already done: a field
-- whose type is not this study's to change, and a type with no text form. A
-- field already declared text returns zero rows moved. Refusals carry a
-- machine-readable `widen` key in the error DETAIL, so a caller distinguishes
-- them without matching on message prose -- including the one raised for a
-- field that is not there, which is a broken precondition rather than a
-- refusal, and is tagged so that it cannot be mistaken for one.

CREATE FUNCTION qiita.widen_study_field_to_text(
    p_study_field_table  REGCLASS,
    p_metadata_table     REGCLASS,
    p_study_field_column TEXT,
    p_study_field_idx    BIGINT
) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_found_idx  BIGINT;
    v_data_type  qiita.field_data_type;
    v_source_col TEXT;
    v_value_expr TEXT;
    v_lock_timeout_ms BIGINT;
    n_moved      BIGINT;
BEGIN
    -- The primary key comes back alongside the type purely to report presence:
    -- EXECUTE leaves FOUND untouched, and data_type is itself NULL on a row
    -- that exists and is globally linked, so neither can stand in for it.
    EXECUTE format(
        'SELECT idx, data_type FROM %s WHERE idx = $1 FOR UPDATE',
        p_study_field_table
    ) INTO v_found_idx, v_data_type USING p_study_field_idx;

    -- Tagged like the refusals below, but not for the same purpose: a caller
    -- addressing a field is expected to have established it exists, so reaching
    -- this is a defect in the caller rather than an answer for an end user. The
    -- tag is what lets a caller tell it apart and refuse to map it to one.
    IF v_found_idx IS NULL THEN
        RAISE EXCEPTION 'study field % not found in %',
            p_study_field_idx, p_study_field_table
          USING DETAIL = format(
              'widen=not_found, study_field_idx=%s', p_study_field_idx);
    END IF;

    -- data_type is NULL exactly on a globally linked row: the
    -- *_study_field_inheritance_consistent CHECK pins it NULL whenever the
    -- global FK is set, so no second column is needed to tell the two apart.
    -- Such a field's type belongs to the global registry every linked study
    -- reads through, and is not one study's to change.
    IF v_data_type IS NULL THEN
        RAISE EXCEPTION
            'study field % is linked to a global field; its data_type is not study-local',
            p_study_field_idx
          USING DETAIL = format(
              'widen=globally_linked, study_field_idx=%s', p_study_field_idx);
    END IF;

    -- Already in the target state, which is a success rather than a conflict:
    -- text values are held in value_text, so there is nothing to flip and
    -- nothing to move. Nothing is written, so the row's optimistic-concurrency
    -- tag is left as the caller found it.
    IF v_data_type = 'text' THEN
        RETURN 0;
    END IF;

    -- Closed mapping from the declared type to its value column and the
    -- expression rendering that column as text. Each expression reproduces the
    -- form the write path stores, so a widened value re-parses unchanged:
    -- Postgres renders a numeric in the text form it stores, and a boolean as
    -- the two words the text parser accepts. to_char rather than a cast for a
    -- date, whose cast output follows the DateStyle setting.
    -- 'terminology' is absent deliberately: its value is a reference into a
    -- controlled vocabulary, with no text form that is not either an opaque id
    -- or a label that may be revised.
    CASE v_data_type
        WHEN 'numeric' THEN
            v_source_col := 'value_numeric';
            v_value_expr := 'value_numeric::text';
        WHEN 'boolean' THEN
            v_source_col := 'value_boolean';
            v_value_expr := 'value_boolean::text';
        WHEN 'date' THEN
            v_source_col := 'value_date';
            v_value_expr := 'to_char(value_date, ''YYYY-MM-DD'')';
        ELSE
            RAISE EXCEPTION 'data_type % cannot be widened to text', v_data_type
              USING DETAIL = format(
                  'widen=unwidenable_type, study_field_idx=%s, data_type=%s',
                  p_study_field_idx, v_data_type);
    END CASE;

    -- Every answer above is read from the field row alone, so the lock is taken
    -- only once a move is certain: a no-op or a refusal must not stall the
    -- write path, nor be turned into the refusal below.
    --
    -- SHARE ROW EXCLUSIVE is the weakest mode that conflicts with the ROW
    -- EXCLUSIVE a writer takes at statement start while also conflicting with
    -- itself: under plain SHARE two concurrent widens would proceed together
    -- and then deadlock upgrading for their own UPDATEs. Reads take ACCESS
    -- SHARE and are unaffected.
    --
    -- Without it a metadata INSERT in flight reads the pre-flip declaration
    -- through the contract trigger and lands in the column the declaration is
    -- about to stop naming -- unreadable, with nothing raised. With it the
    -- writer either commits first and is moved, or cannot start until this
    -- transaction ends, when the contract trigger judges it against the new
    -- declaration and rejects the stale column. That rests on the writer
    -- running at READ COMMITTED, where a statement blocked on the lock takes
    -- its snapshot when it finally runs; at REPEATABLE READ a writer whose
    -- transaction began earlier would wait and then read its original
    -- snapshot, reopening the window.
    --
    -- The lock is table-wide while the widen is field-scoped, so it stalls
    -- every metadata write in the system until this transaction commits. That
    -- is bounded by a caller-supplied lock_timeout rather than by a value
    -- chosen here, and a caller that supplies none is refused: an unbounded
    -- wait would hold a row lock on the study field and queue every metadata
    -- write until someone noticed.
    --
    -- pg_settings reports the value in the GUC's base unit, so it compares as
    -- a number; current_setting would return a unit-bearing display string.
    -- Zero is the disabled state, not a short bound.
    SELECT setting::BIGINT INTO v_lock_timeout_ms
      FROM pg_settings WHERE name = 'lock_timeout';

    IF v_lock_timeout_ms = 0 THEN
        RAISE EXCEPTION 'widening a study field requires a bounded lock_timeout'
          USING ERRCODE = '55000',
                HINT = 'SET LOCAL lock_timeout in the same transaction as the widen';
    END IF;

    -- same-pattern-ok: tg_propagate_unique_in_study locks the same tables for
    -- the same reason; the mode is the invariant and must not differ.
    EXECUTE format(
        'LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE', p_metadata_table);

    -- Declaration first: the field-contract trigger fires on the value move
    -- below and reads the field's type as it stands then, so a move that ran
    -- first would be refused for populating value_text under the old type.
    EXECUTE format(
        'UPDATE %s SET data_type = ''text'' WHERE idx = $1',
        p_study_field_table
    ) USING p_study_field_idx;

    -- Both SET expressions read the row as it was, so the source column is
    -- rendered before it is cleared and exactly one value column stays
    -- populated throughout. Rows holding a missing-value marker carry no typed
    -- value and are passed over by the source-column filter.
    -- v_value_expr interpolates as %s because it is an expression rather than
    -- an identifier; it comes from the mapping above and never from an argument.
    EXECUTE format(
        'WITH moved AS ('
        '  UPDATE %s SET value_text = %s, %I = NULL'
        '   WHERE %I = $1 AND %I IS NOT NULL'
        '   RETURNING 1'
        ') SELECT count(*) FROM moved',
        p_metadata_table, v_value_expr, v_source_col,
        p_study_field_column, v_source_col
    ) INTO n_moved USING p_study_field_idx;

    RETURN n_moved;
END $$;

COMMENT ON FUNCTION qiita.widen_study_field_to_text(REGCLASS, REGCLASS, TEXT, BIGINT) IS
    'Declares one study-local sample field ''text'' and moves every value stored '
    'through it into value_text, returning the number of metadata rows moved. '
    'A field already declared text moves nothing and returns zero. Refuses a '
    'globally linked field and a type with no text form, each tagging its error '
    'DETAIL with a `widen` key, and neither takes a lock. An idx naming no row '
    'breaks the caller''s precondition and is tagged the same way. Requires the '
    'caller''s transaction and, for a move, a bounded lock_timeout, raising '
    'SQLSTATE 55000 without one. A move holds the field row, the metadata table '
    'against concurrent writers, and up to two rows per value moved -- the '
    'metadata row and its parent sample -- all to the caller''s commit.';


-- migrate:down

DROP FUNCTION IF EXISTS qiita.widen_study_field_to_text(REGCLASS, REGCLASS, TEXT, BIGINT);
