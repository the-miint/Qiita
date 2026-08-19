-- Backfill qiita.work_ticket.mask_idx from action_context for tickets that name a
-- mask in their context but not in the column.
--
-- That is the state a CONSUMING ticket is left in: long-read-assembly assembles the
-- read_masked pass-set named by its action_context mask_idx, so the value reaches it
-- as submitter input rather than as something the runner mints. What the column is
-- for, and what a NULL costs, is stated at the shared-mask guard in
-- qiita_control_plane.cli.admin.mask.
--
-- Not scoped to an action_id: the condition is a property of the row — it names a
-- mask in one place and not the other — and any row in that state is invisible to
-- the guard for the same reason. long-read-assembly is the only action that puts a
-- bare `mask_idx` key in action_context today (read-mask and fastq-to-parquet mint
-- theirs and declare no such context key; align uses `align_mask_idx`), so that is
-- what this reaches.
--
-- The join is on TEXT, `md.mask_idx::text`, so no untrusted value is ever cast: a
-- context holding a non-integer, or an integer too large for bigint, matches no row
-- instead of aborting the migration mid-deploy. Rows naming a mask that no longer
-- exists drop out of the same join — NULL is what ON DELETE SET NULL would have left
-- had the value been written before the mask was deleted.

-- migrate:up

UPDATE qiita.work_ticket wt
   SET mask_idx = md.mask_idx
  FROM qiita.mask_definition md
 WHERE wt.mask_idx IS NULL
   AND wt.action_context ->> 'mask_idx' = md.mask_idx::text;

-- Supersede the applied 20260624110000 COMMENT, which described the minting path
-- only. Re-stated rather than edited: an applied migration is never changed.
COMMENT ON COLUMN qiita.work_ticket.mask_idx IS
    'The mask this ticket DEPENDS ON (qiita.mask_definition.mask_idx) — minted by '
    'it (read-mask, fastq-to-parquet) or consumed by it (long-read-assembly, which '
    'assembles the mask''s read_masked pass-set). NULL for tickets that touch no '
    'mask. ON DELETE SET NULL so deleting a mask detaches the ticket. Backs the '
    'shared-mask delete guard, which does not distinguish the two: a mask a '
    'non-failed ticket depends on is undeletable either way.';

-- migrate:down

-- No-op. The backfill is irreversible: a down-migration cannot distinguish rows it
-- wrote from rows the runner later persisted for the same tickets, so it must NULL
-- none of them. The superseded column COMMENT is not restored either.
SELECT 1;
