-- migrate:up

-- The read-mask ticket's minted mask. A read-mask / fastq-to-parquet ticket
-- mints (or dedups to) a qiita.mask_definition row when it runs; persisting the
-- resulting mask_idx here gives durable traceability from the ticket to its mask
-- and a cheap shared-mask guard (block deleting a mask still referenced by a
-- non-failed ticket) without re-deriving the params hash. Nullable: most tickets
-- never mint a mask, and existing rows read NULL with no backfill at migrate
-- time. ON DELETE SET NULL so deleting a mask detaches the ticket cleanly rather
-- than blocking the delete or cascading it away.
ALTER TABLE qiita.work_ticket
    ADD COLUMN mask_idx BIGINT
        REFERENCES qiita.mask_definition(mask_idx) ON DELETE SET NULL;

COMMENT ON COLUMN qiita.work_ticket.mask_idx IS
    'The read-mask ticket''s minted mask (qiita.mask_definition.mask_idx). NULL '
    'for tickets that mint no mask. ON DELETE SET NULL so deleting a mask '
    'detaches the ticket. Backs the shared-mask delete guard.';

-- Partial index mirroring the scope-target arms: only a minority of tickets
-- carry a mask_idx, so a partial index avoids the NULL rows and supports the
-- shared-mask guard's "any non-failed ticket referencing this mask?" lookup.
CREATE INDEX work_ticket_mask_idx
    ON qiita.work_ticket (mask_idx)
    WHERE mask_idx IS NOT NULL;


-- migrate:down

DROP INDEX IF EXISTS qiita.work_ticket_mask_idx;
ALTER TABLE qiita.work_ticket DROP COLUMN IF EXISTS mask_idx;
