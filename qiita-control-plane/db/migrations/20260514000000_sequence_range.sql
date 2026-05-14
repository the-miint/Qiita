-- migrate:up
-- =============================================================================
-- SEQUENCE RANGE
-- =============================================================================
-- The data plane stores raw sequencing reads keyed by a globally-unique
-- sequence_idx BIGINT rather than a read_id VARCHAR. Allocation of those
-- BIGINTs is centralised in the control plane (matching the rest of the
-- idx hierarchy) and exposed through qiita.mint_sequence_range below.
--
-- One row per prep_sample (1:1). Kind-pinned to processing_kind='sequenced'
-- via the composite-FK + GENERATED ALWAYS AS pattern, so a non-sequenced
-- prep_sample can never have a sequence_range. Ranges are immutable
-- post-mint (no UPDATE path is offered) and disappear on parent delete
-- via ON DELETE CASCADE; once consumed by the sequence, an idx range is
-- never returned to the free pool, even if its row was removed.

CREATE SEQUENCE qiita.sequence_idx_seq AS bigint MINVALUE 1 NO CYCLE;

COMMENT ON SEQUENCE qiita.sequence_idx_seq IS
    'Free-standing bigint sequence allocated by qiita.mint_sequence_range. '
    'Not OWNED BY any column — a dropped sequence_range row does not '
    'return its allocation to the pool.';


CREATE TABLE qiita.sequence_range (
    idx                 BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    prep_sample_idx     BIGINT NOT NULL,

    -- Pinned to 'sequenced'; participates in the composite FK so this
    -- row can only attach to a prep_sample whose processing_kind matches.
    -- Identical idiom to qiita.sequenced_sample.processing_kind.
    processing_kind     qiita.processing_kind
                        GENERATED ALWAYS AS
                        ('sequenced'::qiita.processing_kind) STORED,

    sequence_idx_start  BIGINT NOT NULL,
    sequence_idx_stop   BIGINT NOT NULL,

    created_by_idx      BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- 1:1 with prep_sample.
    CONSTRAINT sequence_range_prep_sample_idx_unique UNIQUE (prep_sample_idx),

    -- Composite FK drags processing_kind along so the parent's kind must
    -- match the literal-pinned kind on this row. ON DELETE CASCADE
    -- removes the range when the prep_sample is deleted; the underlying
    -- sequence_idx range is NOT recycled.
    CONSTRAINT sequence_range_prep_sample_fk
        FOREIGN KEY (prep_sample_idx, processing_kind)
        REFERENCES qiita.prep_sample (idx, processing_kind)
        ON DELETE CASCADE,

    CONSTRAINT sequence_range_start_lte_stop
        CHECK (sequence_idx_start <= sequence_idx_stop),
    CONSTRAINT sequence_range_positive
        CHECK (sequence_idx_start >= 1)
);

COMMENT ON TABLE qiita.sequence_range IS
    'Pre-minted bigint range assigned to a prep_sample for raw-read '
    'storage in the data plane. Immutable post-mint; one row per '
    'prep_sample (1:1); kind-pinned to processing_kind=''sequenced'' '
    'via composite FK + GENERATED ALWAYS AS. Rows are removed only via '
    'ON DELETE CASCADE from prep_sample; the underlying sequence_idx '
    'range is never recycled.';

COMMENT ON COLUMN qiita.sequence_range.sequence_idx_start IS
    'Inclusive lower bound of the bigint range assigned to this '
    'prep_sample. All sequence_idx values in [start, stop] are reserved '
    'for raw reads belonging to prep_sample_idx.';

COMMENT ON COLUMN qiita.sequence_range.sequence_idx_stop IS
    'Inclusive upper bound of the bigint range. See sequence_idx_start.';


-- ---------------------------------------------------------------------------
-- mint_sequence_range
-- ---------------------------------------------------------------------------
-- Allocates p_count contiguous bigints from qiita.sequence_idx_seq,
-- records the assignment in qiita.sequence_range, and returns the new
-- row. Uses a transaction-scoped advisory lock to serialise the
-- nextval/setval pair across concurrent callers — each call holds the
-- lock for at most a handful of microseconds, so contention is bounded.
--
-- Error paths the route layer maps:
--   - p_count <= 0 → SQLSTATE 22023 (RaiseError) → HTTP 400
--   - duplicate p_prep_sample_idx → UniqueViolationError → HTTP 409
--   - unknown / wrong-kind p_prep_sample_idx → FKViolationError → HTTP 404
CREATE OR REPLACE FUNCTION qiita.mint_sequence_range(
    p_prep_sample_idx bigint,
    p_count bigint,
    p_principal_idx bigint
) RETURNS qiita.sequence_range
LANGUAGE plpgsql AS $$
DECLARE
    v_start bigint;
    v_stop  bigint;
    v_row   qiita.sequence_range;
BEGIN
    IF p_count <= 0 THEN
        RAISE EXCEPTION 'count must be positive (got %)', p_count
            USING ERRCODE = '22023';
    END IF;

    -- Transaction-scoped advisory lock keyed on the sequence's name hash;
    -- released automatically at COMMIT/ROLLBACK.
    PERFORM pg_advisory_xact_lock(hashtext('qiita.sequence_idx_seq')::bigint);

    v_start := nextval('qiita.sequence_idx_seq');
    v_stop  := v_start + p_count - 1;
    PERFORM setval('qiita.sequence_idx_seq', v_stop);

    INSERT INTO qiita.sequence_range
        (prep_sample_idx, sequence_idx_start, sequence_idx_stop, created_by_idx)
        VALUES (p_prep_sample_idx, v_start, v_stop, p_principal_idx)
        RETURNING * INTO v_row;

    RETURN v_row;
END;
$$;

COMMENT ON FUNCTION qiita.mint_sequence_range(bigint, bigint, bigint) IS
    'Atomically allocates p_count contiguous bigints from '
    'qiita.sequence_idx_seq and records the assignment in '
    'qiita.sequence_range. Raises SQLSTATE 22023 when p_count <= 0; '
    'propagates UniqueViolation (duplicate prep_sample_idx) and '
    'ForeignKeyViolation (unknown or wrong-kind prep_sample_idx) '
    'from the underlying INSERT.';


-- migrate:down
DROP FUNCTION IF EXISTS qiita.mint_sequence_range(bigint, bigint, bigint);
DROP TABLE IF EXISTS qiita.sequence_range;
DROP SEQUENCE IF EXISTS qiita.sequence_idx_seq;
