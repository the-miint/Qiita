-- migrate:up

-- Partial unique index that backs find-or-create on the pool-mint route:
-- POST /sequencing-run/{run}/sequenced-pool keys idempotency on
-- (sequencing_run_idx, run_preflight_filename) so the bundled
-- qiita-submit-bcl-convert CLI is retry-safe. The partial WHERE keeps
-- no-preflight pools (where filename IS NULL) outside the constraint so
-- they can still be freely inserted.
CREATE UNIQUE INDEX IF NOT EXISTS sequenced_pool_one_per_run_and_filename
    ON qiita.sequenced_pool (sequencing_run_idx, run_preflight_filename)
    WHERE run_preflight_filename IS NOT NULL;

-- migrate:down

DROP INDEX IF EXISTS qiita.sequenced_pool_one_per_run_and_filename;
