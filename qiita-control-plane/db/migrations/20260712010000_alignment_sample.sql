-- migrate:up

-- =============================================================================
-- ALIGNMENT SAMPLE (per-(alignment, sample) completion gate)
-- =============================================================================
-- The completion gate for bulk-block sharded alignment — the twin of
-- qiita.mask_sample (whose own comment names alignment as a future consumer).
-- A sample's alignment is assembled by several blocks (a large sample tiles
-- across consecutive blocks), so a sample can sit in a real PARTIAL state where
-- only some covering blocks have landed their alignment rows. A downstream
-- consumer must read completion as a FIRST-CLASS state, not infer it from the
-- presence of rows (alignment rows are NOT 1:1 with reads — cross-shard routing
-- and paired-end mates both multiply rows per read — so a row count says nothing
-- about coverage).
--
-- alignment_sample makes per-(alignment_idx, prep_sample) completion explicit:
-- materialized 'pending' when the planner tiles the sample into blocks, flipped
-- 'completed' only when every covering block has finished (the reconcile step).
-- The DELETE-gated resubmission path resets it (disallow-without-delete).
--
-- `state` is a deliberate TEXT + CHECK (no Postgres ENUM / Pydantic twin) — the
-- gate has exactly two states and no wire surface, so it stays out of the
-- enum-parity discipline (the TEXT/CHECK carve-out in CLAUDE.md), mirroring
-- qiita.mask_sample.

CREATE TABLE qiita.alignment_sample (
    -- The alignment identity (qiita.alignment_definition). ON DELETE CASCADE —
    -- the gate rows are derived from the alignment and are meaningless without
    -- it (and the DELETE path relies on this to reset resubmission).
    alignment_idx    BIGINT NOT NULL REFERENCES qiita.alignment_definition(alignment_idx) ON DELETE CASCADE,

    -- The prep_sample supertype. RESTRICT mirrors mask_sample / work_ticket /
    -- block_member: a sample with a live gate row can't be hard-deleted out from
    -- under it.
    prep_sample_idx  BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,

    state            TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'completed')),

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Bumped on every UPDATE by qiita.set_updated_at() (the pending → completed flip).
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (alignment_idx, prep_sample_idx)
);

COMMENT ON TABLE qiita.alignment_sample IS
    'Per-(alignment_idx, prep_sample) completion gate for bulk-block sharded '
    'alignment. Materialized ''pending'' at plan time, flipped ''completed'' at '
    'reconcile once every covering block finished. Consumers read ONLY '
    '''completed'' samples — alignment rows are NOT 1:1 with reads, so presence '
    'of rows must never be read as "done". Twin of qiita.mask_sample.';

CREATE TRIGGER alignment_sample_set_updated_at
    BEFORE UPDATE ON qiita.alignment_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- migrate:down

DROP TRIGGER IF EXISTS alignment_sample_set_updated_at ON qiita.alignment_sample;
DROP TABLE IF EXISTS qiita.alignment_sample;
