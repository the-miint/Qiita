-- Backfill qiita.work_ticket.processing_idx for the long-read-assembly tickets that
-- the runner started before it recorded the column.
--
-- A started ticket is one that carries mask_idx: the runner records the consumed
-- mask before it mints the processing. Its processing is the row whose params equal
-- the params the runner minted for it, which `_build_processing_params` (runner
-- `_processing.py`) builds for long-read-assembly as exactly
--   {workflow: action_id, version: action_version, mask_idx, assembler}
-- with `assembler` the ticket's action_context value, or the action's context_schema
-- default when the context leaves it unset or empty. This statement rebuilds that
-- object as the builder stands at this migration, so it runs once against the
-- builder that minted every row it reads.
--
-- jsonb equality ignores key order, and jsonb_strip_nulls drops an unset assembler
-- as the builder does. A ticket whose params match no row (one that ended before its
-- mint, where no other ticket minted the same params) stays NULL, which is what the
-- runner would have left. params_hash is unique, so at most one row matches.

-- migrate:up

UPDATE qiita.work_ticket wt
   SET processing_idx = p.processing_idx
  FROM qiita.action a, qiita.processing p
 WHERE wt.action_id = 'long-read-assembly'
   AND wt.processing_idx IS NULL
   AND wt.mask_idx IS NOT NULL
   AND a.action_id = wt.action_id
   AND a.version = wt.action_version
   AND p.params = jsonb_strip_nulls(jsonb_build_object(
         'workflow', wt.action_id,
         'version', wt.action_version,
         'mask_idx', wt.mask_idx,
         'assembler', coalesce(
           nullif(wt.action_context ->> 'assembler', ''),
           a.context_schema -> 'properties' -> 'assembler' ->> 'default')));

-- migrate:down

-- No-op. A down-migration cannot tell the rows this wrote from the ones the runner
-- wrote later for the same tickets, so it NULLs none of them.
SELECT 1;
