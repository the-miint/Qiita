-- Backfill qiita.work_ticket.processing_idx for the long-read-assembly tickets that
-- the runner started before it recorded the column.
--
-- A started ticket carries mask_idx: the runner records the consumed mask before it
-- mints the processing. The processing it minted has the params that the runner builds
-- for long-read-assembly from the ticket, exactly
--   {workflow: action_id, version: action_version, mask_idx, assembler}
-- with `assembler` the ticket's action_context value, or the action's context_schema
-- default when the context leaves it unset or empty; this statement rebuilds that
-- object. The builder does not change between this migration and the deploy's
-- re-run of the statement after the restart.
--
-- The params name no prep_sample, so one row is shared by every prep_sample's ticket
-- under that mask and assembler. The statement also requires the assembly_sample row
-- that the runner writes for (processing, prep_sample) right after the mint, so a
-- ticket is matched only to a processing that was minted for its own prep_sample. A
-- ticket of that prep_sample that ended between recording the mask and the mint
-- (its masked reads failed to stage) still matches when another ticket of the same
-- prep_sample minted those params. A ticket that matches nothing stays NULL.
--
-- jsonb equality ignores key order, and jsonb_strip_nulls drops an unset assembler
-- as the builder does. params_hash is unique, so at most one row matches.

-- migrate:up

UPDATE qiita.work_ticket wt
   SET processing_idx = p.processing_idx
  FROM qiita.action a, qiita.processing p, qiita.assembly_sample s
 WHERE wt.action_id = 'long-read-assembly'
   AND wt.processing_idx IS NULL
   AND wt.mask_idx IS NOT NULL
   AND a.action_id = wt.action_id
   AND a.version = wt.action_version
   AND s.processing_idx = p.processing_idx
   AND s.prep_sample_idx = wt.prep_sample_idx
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
