-- migrate:up
-- =============================================================================
-- upload.source_filename — the basename of the file the client streamed
-- =============================================================================
-- The work_ticket submit gate requires a fastq's basename to be the
-- prep_sample's `sequenced_pool_item_id` followed by `_` or `.`
-- (`_check_fastq_filename_prefix`), which ties the R1/R2 pair to the
-- sequenced_sample row and catches a submission that picked up the wrong
-- sample's reads.
--
-- A file that arrives as an upload has no path in `action_context` — the
-- runner resolves `{prefix}_upload_idx` to a staging path the submitter never
-- chose (`uploads/{idx}/upload.parquet`) — so the rule had nothing to read and
-- went vacuous exactly on the route a regular user takes. This column carries
-- the client's own basename so the same rule applies to both routes.
--
-- Descriptive, like `sha256`: it is the client's claim about what it sent, and
-- it names a file on the client's machine that this system never opens. What
-- it gates is the pairing between an upload and a sequenced_sample, which is
-- the submitter's own assertion either way.
--
-- Nullable: every row that predates this migration has no filename, and an
-- upload feeding a workflow with no filename rule (a reference FASTA, a tree)
-- has no reason to send one. The submit gate skips a NULL rather than
-- refusing it — same shape as the NULL `sequenced_pool_item_id` arm it
-- already has.

-- A basename, enforced as one: no separator, not empty. Nothing opens this
-- value, so the CHECK is about keeping the column's meaning true rather than
-- about traversal — a stored `../x` would simply fail the prefix rule, but it
-- would also make the column a lie.
ALTER TABLE qiita.upload
    ADD COLUMN source_filename TEXT
        CHECK (
            source_filename IS NULL
            OR (source_filename <> '' AND position('/' IN source_filename) = 0)
        );

COMMENT ON COLUMN qiita.upload.source_filename IS
    'Client-claimed basename of the uploaded file. Descriptive, like sha256. '
    'Read by the work_ticket submit gate so the fastq filename-prefix rule '
    'applies to upload-fed submissions as well as path-fed ones.';

-- migrate:down
ALTER TABLE qiita.upload DROP COLUMN source_filename;
