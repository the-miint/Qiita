-- Backfill qiita.mask_sample for the per-sample mask-model workflows.
--
-- Historically only the block masking path (read-mask-block → reconcile-block)
-- wrote qiita.mask_sample; the per-sample mask-model workflows (read-mask and the
-- mask-model fastq-to-parquet, both of which mint a mask_idx and write read_mask)
-- never did. Consumers (masked-read export, long-read-assembly input, alignment)
-- now require a 'completed' gate row and treat its ABSENCE as "not masked-complete"
-- (the first-class completion contract). Without this backfill, every
-- already-completed per-sample mask — which has no gate row — would be refused by
-- the tightened readers. Populate a 'completed' gate row for each completed
-- per-sample mask-model ticket from the self-describing work_ticket (its bound
-- mask_idx + prep_sample scope target). Both action_ids are matched: `read-mask`
-- and `fastq-to-parquet` (the ingest-and-mask path — a live workflow, not a legacy
-- alias). The mask_idx IS NOT NULL filter naturally excludes the pre-mask-model
-- fastq-to-parquet versions (1.0.0–1.2.0), which physically dropped reads and never
-- minted a mask_idx. Idempotent (ON CONFLICT DO NOTHING) so it composes with any
-- per-sample finalize-mask-sample or block-path row already present.
--
-- NOTE: dbmate applies a migration once and never re-runs it. The deploy-window
-- edge — a ticket that completes between `make migrate` and the service restart,
-- under the old code that doesn't yet write the gate — is closed by re-running the
-- EQUIVALENT idempotent SQL by hand post-restart (per the deploy checklist), NOT by
-- re-applying this migration. On a fresh DB (empty work_ticket) this is a no-op, so
-- the test tier applies it cleanly.

-- migrate:up
INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)
SELECT wt.mask_idx, wt.prep_sample_idx, 'completed'
FROM qiita.work_ticket wt
WHERE wt.action_id IN ('read-mask', 'fastq-to-parquet')
  AND wt.scope_target_kind = 'prep_sample'
  AND wt.state = 'completed'
  AND wt.mask_idx IS NOT NULL
ON CONFLICT (mask_idx, prep_sample_idx) DO NOTHING;

-- migrate:down
-- Irreversible data backfill: a down-migration cannot distinguish rows this
-- backfill inserted from rows the per-sample finalize-mask-sample action (or the
-- block path) later wrote for the same pairs, so it must not delete any. No-op.
SELECT 1;
