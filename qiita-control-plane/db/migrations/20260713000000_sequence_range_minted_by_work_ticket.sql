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
-- The ONLY safe inference is "this ticket minted this range", and the tell is
-- TIME: a range a ticket minted is created AFTER that ticket. A range the ticket
-- merely COLLIDED with (mint → 409 → FAILED) predates it. Without that guard the
-- backfill fails OPEN in the one direction that matters: an Illumina sample whose
-- reads were loaded by a pool `ingest_reads` ticket, then hit by a stray
-- `fastq-to-parquet` submission that 409'd and FAILED, would have its range stamped
-- with that FAILED ticket — and a later `ticket run` on it would then "recognise"
-- the range as its own, reuse it, and register the sample's reads a SECOND time.
-- `sr.created_at >= wt.created_at` excludes exactly that case.
--
-- Two shapes of loader ticket mint ranges, so both are attributed:
--   1. per-sample loaders (bam-to-parquet / fastq-to-parquet) — work_ticket is
--      prep_sample-scoped, so it joins on prep_sample_idx directly. This is what
--      attributes the PacBio tickets whose retries need to reuse their own range.
--   2. the pool loader (bcl-convert → ingest_reads) — work_ticket is
--      sequenced_pool-scoped and mints a range per sample in the pool, so it joins
--      through sequenced_sample. Attributing these matters: a pool ingest that
--      crashed mid-fan-out must still be able to retry and reuse the ranges it
--      already minted, which a NULL would forbid.
--
-- `count(*) = 1` keeps the attribution unambiguous; anything else stays NULL, which
-- reads as "not mine" and fails closed.
UPDATE qiita.sequence_range sr
   SET minted_by_work_ticket_idx = wt.work_ticket_idx
  FROM qiita.work_ticket wt
 WHERE wt.prep_sample_idx = sr.prep_sample_idx
   AND wt.action_id IN ('bam-to-parquet', 'fastq-to-parquet')
   AND sr.created_at >= wt.created_at
   AND (
        SELECT count(*)
          FROM qiita.work_ticket w2
         WHERE w2.prep_sample_idx = sr.prep_sample_idx
           AND w2.action_id IN ('bam-to-parquet', 'fastq-to-parquet')
           AND sr.created_at >= w2.created_at
       ) = 1;

UPDATE qiita.sequence_range sr
   SET minted_by_work_ticket_idx = wt.work_ticket_idx
  FROM qiita.sequenced_sample ss
  JOIN qiita.work_ticket wt
    ON wt.sequenced_pool_idx = ss.sequenced_pool_idx
   AND wt.action_id = 'bcl-convert'
 WHERE ss.prep_sample_idx = sr.prep_sample_idx
   AND sr.minted_by_work_ticket_idx IS NULL
   AND sr.created_at >= wt.created_at
   AND (
        SELECT count(*)
          FROM qiita.work_ticket w2
         WHERE w2.sequenced_pool_idx = ss.sequenced_pool_idx
           AND w2.action_id = 'bcl-convert'
           AND sr.created_at >= w2.created_at
       ) = 1;

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
