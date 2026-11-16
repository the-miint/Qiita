-- migrate:up

-- The processing (assembly run config) a ticket minted. The runner writes it right
-- after it mints the processing (`runner._processing._persist_processing_idx`), as it
-- writes mask_idx and alignment_idx, so what a ticket produced is read from the
-- ticket rather than recomputed from its context. NULL for every ticket that mints
-- no processing, and for one that ended before its mint. ON DELETE SET NULL mirrors
-- work_ticket.mask_idx and work_ticket.alignment_idx.
ALTER TABLE qiita.work_ticket
    ADD COLUMN processing_idx BIGINT
        REFERENCES qiita.processing(processing_idx) ON DELETE SET NULL;

COMMENT ON COLUMN qiita.work_ticket.processing_idx IS
    'The processing (qiita.processing.processing_idx) this ticket minted, written by '
    'the runner right after the mint. NULL for a ticket that mints none or ended '
    'before its mint. ON DELETE SET NULL so deleting a processing detaches the ticket.';

-- Partial, as work_ticket.mask_idx's and work_ticket.alignment_idx's are: only
-- tickets that mint a processing carry one.
CREATE INDEX work_ticket_processing_idx
    ON qiita.work_ticket (processing_idx)
    WHERE processing_idx IS NOT NULL;


-- migrate:down

DROP INDEX IF EXISTS qiita.work_ticket_processing_idx;
ALTER TABLE qiita.work_ticket DROP COLUMN IF EXISTS processing_idx;
