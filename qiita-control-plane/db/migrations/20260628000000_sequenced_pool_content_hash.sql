-- migrate:up

-- Content-key the sequenced_pool find-or-create (defect 1 of the duplicate-pool
-- bug). The pool was deduplicated on (sequencing_run_idx, run_preflight_filename)
-- via sequenced_pool_one_per_run_and_filename, so a byte-identical preflight
-- re-uploaded under a different basename minted a BRAND-NEW pool instead of
-- reusing the existing one — the source of the observed run-15 duplicate (two
-- pools, same preflight bytes, two filenames). Identity must be the preflight
-- CONTENT, not its name.
--
-- The content index is added ALONGSIDE the existing filename index; BOTH are
-- permanent. The control plane is repointed to ON CONFLICT on the content hash,
-- so re-uploading the same bytes under any name reuses the pool. The filename
-- index is deliberately retained as an independent uniqueness rule: two
-- genuinely-distinct pools in a run differ in BOTH content and filename, so a
-- collision on the filename alone (same name, different content) is a 409 by
-- design — the operator renames. (Same content + same name is the CLI's
-- idempotent retry: the content index handles it as a reuse, the filename
-- collision never surfaces.)
--
-- One-time deploy note: during the migrate→restart window the pre-restart CP
-- still does ON CONFLICT on filename, so a same-content/different-filename POST
-- (the run-15 trigger) hits this new content index as an *unnamed* arbiter and
-- raises a transient 500 instead of its prior (buggy) 201. That fails SAFE — no
-- duplicate row is created, the input is rare, the window short — and the
-- restarted CP handles the same POST as a 200 reuse.
--
-- run_preflight_sha256 is a STORED generated column: Postgres computes
-- sha256(run_preflight_blob) (built-in since PG11, no pgcrypto) for every row,
-- including the existing ones, so there is no separate backfill. sha256(NULL)
-- is NULL, so no-preflight pools (blob NULL) fall outside the partial index's
-- predicate exactly as they fall outside the filename index today.
--
-- IMPORTANT — the CREATE UNIQUE INDEX below FAILS if any run already holds two
-- pools with byte-identical preflights (e.g. the run-15 duplicate). Resolve
-- those with `qiita delete-sequenced-pool --force` BEFORE running this migration
-- (see DEPLOY_CHECKLIST.md for the pre-check query).

ALTER TABLE qiita.sequenced_pool
  ADD COLUMN run_preflight_sha256 BYTEA
    GENERATED ALWAYS AS (sha256(run_preflight_blob)) STORED;

COMMENT ON COLUMN qiita.sequenced_pool.run_preflight_sha256 IS
    'SHA-256 of run_preflight_blob, computed in-DB (NULL when no preflight). '
    'The find-or-create identity key for a pool within a run — re-uploading the '
    'same preflight bytes under any filename resolves to the same pool. '
    'run_preflight_filename is descriptive only.';

CREATE UNIQUE INDEX sequenced_pool_one_per_run_and_hash
    ON qiita.sequenced_pool (sequencing_run_idx, run_preflight_sha256)
    WHERE run_preflight_sha256 IS NOT NULL;

-- migrate:down

DROP INDEX IF EXISTS qiita.sequenced_pool_one_per_run_and_hash;
ALTER TABLE qiita.sequenced_pool DROP COLUMN IF EXISTS run_preflight_sha256;
