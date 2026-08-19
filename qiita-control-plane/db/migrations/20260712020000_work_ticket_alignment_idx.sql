-- migrate:up

-- The align block ticket's alignment identity. A block-scoped align ticket
-- carries the alignment_idx its plan resolved (minted at plan time as the
-- partition key, mirroring how a block-mask ticket carries mask_idx). It is what
-- (a) the runner keys "align run → masked reads" on (alignment_idx IS NOT NULL
-- signals the masked-read export path), (b) has_incomplete_covering_alignment_block
-- JOINs to find a sample's covering blocks, and (c) reconcile-alignment-block
-- reads. Nullable: every non-align ticket (all reference/prep_sample/pool tickets
-- and the block-MASK tickets) leaves it NULL and is unaffected; existing rows read
-- NULL with no backfill. ON DELETE SET NULL so deleting an alignment detaches the
-- ticket cleanly (mirrors work_ticket.mask_idx).
--
-- An align block ticket carries BOTH mask_idx (the host-depletion mask its input
-- reads were masked under — the masked-read export filters on it) AND
-- alignment_idx (the align config identity). The two are orthogonal.
ALTER TABLE qiita.work_ticket
    ADD COLUMN alignment_idx BIGINT
        REFERENCES qiita.alignment_definition(alignment_idx) ON DELETE SET NULL;

COMMENT ON COLUMN qiita.work_ticket.alignment_idx IS
    'The align block ticket''s alignment identity '
    '(qiita.alignment_definition.alignment_idx), resolved at plan time. NULL for '
    'every non-align ticket. ON DELETE SET NULL so deleting an alignment detaches '
    'the ticket. Signals the runner to stage MASKED reads and keys the '
    'covering-block completion gate.';

-- Partial index mirroring work_ticket.mask_idx: only align tickets carry an
-- alignment_idx, so a partial index avoids the NULL rows and supports the
-- covering-block completion lookup (has_incomplete_covering_alignment_block).
CREATE INDEX work_ticket_alignment_idx
    ON qiita.work_ticket (alignment_idx)
    WHERE alignment_idx IS NOT NULL;


-- migrate:down

DROP INDEX IF EXISTS qiita.work_ticket_alignment_idx;
ALTER TABLE qiita.work_ticket DROP COLUMN IF EXISTS alignment_idx;
