-- migrate:up
-- =============================================================================
-- UPLOAD (generic Arrow-data staging domain)
-- =============================================================================
-- A `qiita.upload` row is a handle on staged Arrow data — bytes the client
-- streamed via Flight DoPut into the shared filesystem. The domain is
-- intentionally content-agnostic: there is no reference_idx, no role enum,
-- no FASTA-specific column. Consumer-side scoping (which workflow consumes
-- the upload_idx, with what authorization gate) lives in the work_ticket
-- that references the upload, not on this row.
--
-- State machine:
--   pending  → mint_slot inserted the row; DoPut not yet acknowledged.
--   ready    → client called POST /upload/{idx}/done; staged file is final.
--   consumed → a workflow runner read the upload as an input.
--   failed   → future: the data plane reported a write error, or a cleanup
--              sweep aged out a stale pending row. No transition path
--              currently lands this state.
--
-- Source-of-truth for the staging path is `Settings.upload_staging_root +
-- upload_idx` — not stored on the row. Keeps the schema small and lets a
-- deploy-time root rename land without a migration. Audit consumers that
-- need the path compute it from the same Settings the runner reads.

CREATE TABLE qiita.upload (
    upload_idx       BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'ready', 'consumed', 'failed')),

    -- Free-form human label. Optional. Helpful for admin views and for
    -- correlating a CLI invocation with a row at debug time.
    description      TEXT,

    -- Recorded on /done from the client's claim, which the client forwarded
    -- from the data plane's PutResult body. Descriptive — not used to gate
    -- any access. The workflow that consumes this upload runs its own
    -- content-addressing pass over the file's actual bytes; if the claim is
    -- wrong, the consuming workflow surfaces the inconsistency.
    sha256           TEXT,
    row_count        BIGINT CHECK (row_count IS NULL OR row_count >= 0),
    bytes_received   BIGINT CHECK (bytes_received IS NULL OR bytes_received >= 0),

    created_by_idx   BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Set on transition out of `pending`. NULL while pending; pinned at
    -- transition time so a later state change (consumed → archived, etc.)
    -- doesn't overwrite the original completion timestamp.
    completed_at     TIMESTAMPTZ,

    -- Defence in depth: the route layer derives completed_at via SET to
    -- now() at the same moment status moves off `pending`. If a future
    -- code path bypasses the route and writes a `ready`/`consumed`/`failed`
    -- row without populating completed_at, the constraint catches it.
    CONSTRAINT upload_terminal_has_completed_at
        CHECK (status = 'pending' OR completed_at IS NOT NULL)
);

COMMENT ON TABLE qiita.upload IS
    'Generic Arrow-data staging slot. Minted by POST /api/v1/upload and '
    'fulfilled by Flight DoPut against the data plane. The row carries no '
    'consumer-specific fields — workflow scoping happens on the work_ticket '
    'that references the upload_idx, not on this row.';

COMMENT ON COLUMN qiita.upload.sha256 IS
    'Client-claimed sha256 of the staged Parquet, forwarded from the data '
    'plane''s PutResult body. Descriptive only; the consuming workflow '
    'computes its own content-addressing pass and surfaces mismatches.';

-- Lookup by owner + status is the natural shape for a future
-- "show me my pending uploads" admin view. Cheap to add the index now;
-- the view itself is deferred.
CREATE INDEX upload_created_by_status_idx
    ON qiita.upload (created_by_idx, status);


-- migrate:down

DROP TABLE IF EXISTS qiita.upload;
