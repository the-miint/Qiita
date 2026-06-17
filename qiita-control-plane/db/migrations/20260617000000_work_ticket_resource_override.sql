-- Per-run resource override for a work ticket's SLURM steps.
--
-- Nullable JSONB carrying the optional `ResourceOverride` (currently just
-- `mem_gb`) a privileged caller (wet_lab_admin / system_admin) supplies at
-- submission to raise the per-step memory floor for one run without editing
-- the workflow YAML. NULL (the default) means "no override — use each step's
-- YAML baseline_resources verbatim". Read by the runner at dispatch
-- (`_resolve_baseline_for_step` applies max(baseline.mem_gb, override.mem_gb),
-- still clamped to the action ceiling); persisted here so a control-plane
-- restart re-attaches in-flight work with the same override. Additive and
-- backfill-free — every existing row reads as NULL.

-- migrate:up
ALTER TABLE qiita.work_ticket
  ADD COLUMN resource_override JSONB;

-- migrate:down
ALTER TABLE qiita.work_ticket
  DROP COLUMN resource_override;
