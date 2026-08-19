-- migrate:up

-- =============================================================================
-- MASK SAMPLE (per-(mask, sample) completion gate)
-- =============================================================================
-- The completion gate for bulk-block read masking. Under the per-sample
-- ticket model a sample's read_mask rows were all-or-nothing, so "no read_mask
-- row ⇒ excluded" was an implicit, safe guarantee. Block-as-ticket destroys
-- that: a sample's mask is assembled by several blocks, so a sample can sit in
-- a real PARTIAL state (some covering blocks done, others still running) where
-- the read_mask rows are incomplete. Reading absence-of-row as "pass" would
-- then silently truncate a masked-read export and leak un-filtered host reads.
--
-- mask_sample makes per-(mask_idx, prep_sample) completion a FIRST-CLASS state:
-- materialized 'pending' when the planner tiles the sample into blocks, flipped
-- 'completed' only when every covering block has finished and the per-sample
-- rollup is done (the reconcile step). Consumers of masked reads (the
-- masked-read export path) read this gate and consume only 'completed' samples.
--
-- `state` is a deliberate TEXT + CHECK (no Postgres ENUM / Pydantic twin) — the
-- gate has exactly two states and no wire surface, so it stays out of the
-- enum-parity discipline (the TEXT/CHECK carve-out in CLAUDE.md).

CREATE TABLE qiita.mask_sample (
    -- The filtering identity (qiita.mask_definition). ON DELETE CASCADE — the
    -- gate rows are derived from the mask and are meaningless without it.
    mask_idx         BIGINT NOT NULL REFERENCES qiita.mask_definition(mask_idx) ON DELETE CASCADE,

    -- The prep_sample supertype. RESTRICT mirrors work_ticket / block_member:
    -- a sample with a live gate row can't be hard-deleted out from under it.
    prep_sample_idx  BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,

    state            TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'completed')),

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Bumped on every UPDATE by qiita.set_updated_at() (the pending → completed flip).
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (mask_idx, prep_sample_idx)
);

COMMENT ON TABLE qiita.mask_sample IS
    'Per-(mask_idx, prep_sample) completion gate for bulk-block read masking. '
    'Materialized ''pending'' at plan time, flipped ''completed'' at reconcile '
    'once every covering block finished + the per-sample rollup ran. Consumers '
    '(masked-read export, alignment) read ONLY ''completed'' samples — absence '
    'of a read_mask row must never be read as "pass".';

CREATE TRIGGER mask_sample_set_updated_at
    BEFORE UPDATE ON qiita.mask_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- migrate:down

DROP TRIGGER IF EXISTS mask_sample_set_updated_at ON qiita.mask_sample;
DROP TABLE IF EXISTS qiita.mask_sample;
