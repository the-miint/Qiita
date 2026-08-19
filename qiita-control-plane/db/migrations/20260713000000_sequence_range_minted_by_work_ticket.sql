-- migrate:up
-- =============================================================================
-- SEQUENCE RANGE: record the work_ticket that minted the range
-- =============================================================================
-- The mint is one-shot per prep_sample, and a reads job mints its range and THEN
-- does the heavy durable write — so a mid-step crash leaves an orphaned range and
-- the next attempt's re-mint 409s. The job recovers by reading the range back and
-- reusing it, which is what makes the step idempotent across runner retries.
--
-- But "reuse the existing range" is only safe when the range belongs to a prior
-- ATTEMPT OF THE SAME work_ticket. If a DIFFERENT ticket hits that 409, the
-- sample's reads are already registered in the lake, and reusing the range would
-- register them a second time — DuckLake has no uniqueness, so the reads silently
-- double. Nothing else can tell the two cases apart: the submit-time
-- disallow-without-delete gate only blocks NON-terminal tickets, so a COMPLETED
-- sample can be resubmitted, and the job's output lives in a per-ticket workspace
-- it cannot see across tickets.
--
-- Recording the minting ticket makes the distinction explicit and checkable at the
-- one place that has both facts in hand.
-- Deliberately NO foreign key. This column is an identity TOKEN we compare for
-- equality, not a relationship we navigate: the only read is "did MY ticket mint
-- this?". A FK would buy nothing and cost two things — a mint whose ticket row is
-- absent would raise ForeignKeyViolationError, which the route already maps to a
-- misleading 404 ("prep_sample not found"); and a ticket delete would have to
-- either cascade (unthinkable — the range's sequence_idx values are in the lake) or
-- SET NULL. A dangling or NULL value already reads as "not mine" and fails closed,
-- which is the safe direction, so referential integrity adds no safety here.
ALTER TABLE qiita.sequence_range
    ADD COLUMN minted_by_work_ticket_idx BIGINT,
    ADD CONSTRAINT sequence_range_minted_by_positive
        CHECK (minted_by_work_ticket_idx IS NULL OR minted_by_work_ticket_idx > 0);

COMMENT ON COLUMN qiita.sequence_range.minted_by_work_ticket_idx IS
    'The work_ticket whose step minted this range. A reads job may reuse an '
    'existing range ONLY when this matches its own work_ticket_idx (a retry of '
    'the same step). A different ticket means the reads are already loaded and '
    'must be deleted before re-ingest. NULL = provenance unknown (a row the '
    'backfill could not attribute unambiguously); callers treat NULL as NOT-mine '
    'and refuse to reuse it, which is the safe reading — it is exactly '
    'disallow-without-delete.';

-- Backfill.
--
-- Attribute a range ONLY to a ticket that could actually have minted it. Two
-- conditions, and both are necessary:
--
--   TIME. A range a ticket minted is created AFTER that ticket. A range the ticket
--   merely COLLIDED with (mint -> 409 -> FAILED) predates it.
--
--   UNIQUENESS ACROSS BOTH LOADER SHAPES. Ranges are minted by per-sample loaders
--   (bam-to-parquet / fastq-to-parquet, prep_sample-scoped) AND by the pool loader
--   (bcl-convert -> ingest_reads, sequenced_pool-scoped, minting one range per sample
--   in the pool). A sample can have candidates of both shapes, so the count MUST span
--   both. Counting only one shape fails OPEN: an Illumina sample whose range was
--   minted by its pool ingest, and which also carries a stray early per-sample loader
--   ticket, would look "unambiguous" to a per-sample-only count and get credited to
--   the stray ticket — and a later `ticket run` on it would then "recognise" the range
--   as its own, reuse it, and register the sample's reads a SECOND time.
--
-- Anything with zero or multiple candidates stays NULL, which reads as "not mine" and
-- fails closed.
WITH candidate AS (
    SELECT sr.idx AS sequence_range_idx, wt.work_ticket_idx
      FROM qiita.sequence_range sr
      JOIN qiita.work_ticket wt
        ON wt.prep_sample_idx = sr.prep_sample_idx
       AND wt.action_id IN ('bam-to-parquet', 'fastq-to-parquet')
     WHERE sr.created_at >= wt.created_at

    UNION ALL

    SELECT sr.idx AS sequence_range_idx, wt.work_ticket_idx
      FROM qiita.sequence_range sr
      JOIN qiita.sequenced_sample ss
        ON ss.prep_sample_idx = sr.prep_sample_idx
      JOIN qiita.work_ticket wt
        ON wt.sequenced_pool_idx = ss.sequenced_pool_idx
       AND wt.action_id = 'bcl-convert'
     WHERE sr.created_at >= wt.created_at
),
unambiguous AS (
    SELECT sequence_range_idx, min(work_ticket_idx) AS work_ticket_idx
      FROM candidate
     GROUP BY sequence_range_idx
    HAVING count(*) = 1
)
UPDATE qiita.sequence_range sr
   SET minted_by_work_ticket_idx = u.work_ticket_idx
  FROM unambiguous u
 WHERE u.sequence_range_idx = sr.idx;

-- The mint function gains the ticket. Its argument list changes, so this is a DROP
-- + CREATE rather than a CREATE OR REPLACE (a new signature would otherwise be an
-- overload, leaving the un-attributing 3-arg version callable).
DROP FUNCTION qiita.mint_sequence_range(bigint, bigint, bigint);

CREATE FUNCTION qiita.mint_sequence_range(
    p_prep_sample_idx bigint,
    p_count bigint,
    p_principal_idx bigint,
    p_work_ticket_idx bigint
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
        (prep_sample_idx, sequence_idx_start, sequence_idx_stop, created_by_idx,
         minted_by_work_ticket_idx)
        VALUES (p_prep_sample_idx, v_start, v_stop, p_principal_idx,
                p_work_ticket_idx)
        RETURNING * INTO v_row;

    RETURN v_row;
END;
$$;

COMMENT ON FUNCTION qiita.mint_sequence_range(bigint, bigint, bigint, bigint) IS
    'Atomically allocates p_count contiguous bigints from '
    'qiita.sequence_idx_seq and records the assignment in '
    'qiita.sequence_range, stamped with p_work_ticket_idx (the ticket whose '
    'step minted it — a reads job may only reuse a range its OWN ticket '
    'minted). Raises SQLSTATE 22023 when p_count <= 0; propagates '
    'UniqueViolation (duplicate prep_sample_idx) and ForeignKeyViolation '
    '(unknown or wrong-kind prep_sample_idx) from the underlying INSERT.';

-- migrate:down
DROP FUNCTION qiita.mint_sequence_range(bigint, bigint, bigint, bigint);

CREATE FUNCTION qiita.mint_sequence_range(
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

ALTER TABLE qiita.sequence_range
    DROP CONSTRAINT sequence_range_minted_by_positive;
ALTER TABLE qiita.sequence_range DROP COLUMN minted_by_work_ticket_idx;
