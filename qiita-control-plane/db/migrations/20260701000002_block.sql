-- migrate:up

-- =============================================================================
-- BLOCK CORE (bulk-block read masking)
-- =============================================================================
-- A `block` is the compute unit for bulk-block read masking: a fixed
-- ~10M-read slice drawn from prep_samples that share one filtering identity
-- (mask_idx), processed as a single work ticket / SLURM job. This decouples the
-- COMPUTE unit (a block) from the ACCOUNTING unit (per-sample completion,
-- reconciled afterward from the per-row columns the `read` data already
-- carries). The core is deliberately WHY-agnostic — it records only WHAT a
-- block is (`block`) and which sample sub-ranges it covers (`block_member`);
-- the WHY (mask_idx) stays on qiita.work_ticket.mask_idx, not here. So nothing
-- about a block presupposes read masking: any consumer that keys its own WHY off
-- the work_ticket reuses this core unchanged.
--
-- `state` is an intentional TEXT + CHECK, not a Postgres ENUM: a block is an
-- internal compute-lifecycle record with no Pydantic wire twin that needs a
-- CREATE TYPE, so it stays out of the enum-parity discipline (see the TEXT/CHECK
-- carve-out in CLAUDE.md). The set mirrors the non-terminal/terminal shape of
-- the work-ticket lifecycle minus the queue/no_data states a block never sees.

CREATE TABLE qiita.block (
    block_idx        BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- The work_ticket that runs this block. NULLABLE to break the
    -- mint-ordering cycle: the block is minted FIRST (so the ticket's
    -- scope_target can reference block_idx), the ticket is created scoped to
    -- the block, then this column is back-filled. ON DELETE CASCADE — deleting
    -- the ticket removes its block. The reverse edge
    -- (qiita.work_ticket.block_idx → block) is a deferred NO ACTION so the
    -- mutual reference does not deadlock a cascading delete.
    work_ticket_idx  BIGINT REFERENCES qiita.work_ticket(work_ticket_idx) ON DELETE CASCADE,

    state            TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'processing', 'completed', 'failed')),

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Bumped on every UPDATE by qiita.set_updated_at().
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE qiita.block IS
    'Compute unit for bulk-block read masking: a fixed ~10M-read slice from '
    'prep_samples sharing one mask_idx, run as one work ticket. WHY-agnostic '
    'core (the mask_idx lives on qiita.work_ticket.mask_idx, not here) so it '
    'presupposes nothing about read masking and is reusable by any '
    'work_ticket-keyed consumer. work_ticket_idx is NULLABLE + back-filled to '
    'break the block↔ticket mint-ordering cycle.';

COMMENT ON COLUMN qiita.block.work_ticket_idx IS
    'The work_ticket running this block. NULL between block mint and ticket '
    'create, then back-filled. ON DELETE CASCADE; the reverse work_ticket.block_idx '
    'edge is a deferred NO ACTION so the pair does not deadlock on delete.';

-- UNIQUE (partial, excluding the pre-back-fill NULLs): a block runs on exactly
-- ONE ticket, so at most one block may point at any given work_ticket. This
-- nails the 1:1 block↔ticket invariant at write time — without it a divergent
-- back-fill linking two blocks to one ticket would, on that ticket's delete,
-- cascade-delete a block still referenced by a *different* live ticket and
-- abort the DELETE with an FK violation. Doubles as the "find the block for
-- this ticket" lookup index.
CREATE UNIQUE INDEX block_work_ticket_idx
    ON qiita.block (work_ticket_idx)
    WHERE work_ticket_idx IS NOT NULL;

CREATE TRIGGER block_set_updated_at
    BEFORE UPDATE ON qiita.block
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- =============================================================================
-- BLOCK MEMBER (block ↔ sample cover-map)
-- =============================================================================
-- One row per (block, prep_sample): the contiguous [min_sequence_idx,
-- max_sequence_idx] sub-range of that sample's reads this block covers. A
-- sample larger than one block splits across consecutive blocks (each carrying
-- a disjoint sub-range); a small sample contributes its whole range to one
-- block. This is the metadata the planner computes at submit time (pure
-- arithmetic over qiita.sequence_range — no read data touched) and the map the
-- reconcile step walks to roll up per-sample completion.

CREATE TABLE qiita.block_member (
    block_idx        BIGINT NOT NULL REFERENCES qiita.block(block_idx) ON DELETE CASCADE,

    -- The prep_sample supertype (matches qiita.work_ticket.prep_sample_idx and
    -- the read table's prep_sample_idx column). RESTRICT so a sample with
    -- outstanding block membership can't be hard-deleted out from under it,
    -- mirroring work_ticket.prep_sample_idx.
    prep_sample_idx  BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,

    -- Inclusive contiguous sequence_idx bounds of this sample's slice in the
    -- block. The read table is stored one file per sample sorted by
    -- sequence_idx, so a [min,max] pair is exactly the sub-range to export.
    min_sequence_idx BIGINT NOT NULL,
    max_sequence_idx BIGINT NOT NULL,

    PRIMARY KEY (block_idx, prep_sample_idx),
    CONSTRAINT block_member_range_ordered CHECK (min_sequence_idx <= max_sequence_idx)
);

COMMENT ON TABLE qiita.block_member IS
    'block ↔ prep_sample cover-map: the contiguous [min_sequence_idx, '
    'max_sequence_idx] slice of a sample this block covers. Computed at submit '
    'time (arithmetic over qiita.sequence_range); walked at reconcile to roll '
    'up per-sample completion. A large sample splits across consecutive blocks '
    'with disjoint sub-ranges.';

-- Reconcile cover-map lookup: "which blocks cover this sample?" keys on
-- prep_sample_idx (the PK leads with block_idx, so a separate index is needed).
CREATE INDEX block_member_prep_sample_idx
    ON qiita.block_member (prep_sample_idx);


-- migrate:down

DROP TABLE IF EXISTS qiita.block_member;
DROP TRIGGER IF EXISTS block_set_updated_at ON qiita.block;
DROP TABLE IF EXISTS qiita.block;
