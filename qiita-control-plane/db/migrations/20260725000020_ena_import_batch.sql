-- Batch multi-study ENA import: a `qiita.ena_import_batch` row is
-- one `POST /api/v1/ena-import-batch` submission (a list of ENA study
-- accessions); one `qiita.ena_import_batch_item` row per accession tracks
-- its own resolve/register/download-submit progress independently, so one
-- accession's failure never affects its siblings -- mirrors the
-- per-run isolation `ena_import.registration.register_ena_study` already
-- guarantees, one level up.
--
-- Both tables are additive and reversible; no CREATE TYPE (matches
-- `upload.status` -- TEXT/CHECK, not a Postgres ENUM; see CLAUDE.md
-- "Enum parity").
--
-- State machine (qiita.ena_import_batch_item.state, mirrored by
-- qiita_common.models.ena_import.BatchItemState):
--   pending     -> INSERTed alongside the batch; not yet picked up.
--   resolving   -> the background task is resolving study/runs/attributes
--                  (via MiintEnaResolver) for this accession.
--   registered  -> ena_import.registration.register_ena_study succeeded;
--                  study_idx is set.
--   downloading -> one download-ena-study work_ticket was submitted per
--                  pool register_ena_study created
--                  (download_work_ticket_idxs populated).
--   done        -> a DISPLAY-ONLY state: rolled up on demand at GET time
--                  from download_work_ticket_idxs' qiita.work_ticket.state
--                  (all terminal-success). The batch driver itself never
--                  writes 'done' -- see routes/ena_import.py.
--   failed      -> resolve, register, or ticket-submission raised for this
--                  accession; failure_reason carries why. Never the whole
--                  batch -- only this item.

-- migrate:up

CREATE TABLE qiita.ena_import_batch (
    idx                         BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- The admin (wet_lab_admin / system_admin) who submitted the batch.
    -- Also the owner_idx / caller_idx `register_ena_study` uses for every
    -- study this batch creates, and the principal
    -- `submit_work_ticket_core` enforces the download-ena-study action's
    -- own audience against for every download ticket this batch submits.
    submitted_by_principal_idx  BIGINT NOT NULL
        REFERENCES qiita.principal(idx) ON DELETE RESTRICT,

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Transport pinned into every download-ena-study ticket this batch
    -- submits. Only 'http' is supported today -- no Aspera key-staging in
    -- this compute environment; a single-value CHECK so a future transport
    -- is a deliberate migration, not silent drift.
    download_method               TEXT NOT NULL DEFAULT 'http'
        CHECK (download_method IN ('http'))
);

COMMENT ON TABLE qiita.ena_import_batch IS
    'One POST /api/v1/ena-import-batch submission: a list of ENA study '
    'accessions fanned out into one qiita.ena_import_batch_item per '
    'accession.';


CREATE TABLE qiita.ena_import_batch_item (
    idx                       BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Parent batch. CASCADE -- an item's history belongs to its batch,
    -- mirrors qiita.work_ticket_step's CASCADE on work_ticket_idx.
    batch_idx                 BIGINT NOT NULL
        REFERENCES qiita.ena_import_batch(idx) ON DELETE CASCADE,

    ena_study_accession       TEXT NOT NULL
        CHECK (length(ena_study_accession) BETWEEN 1 AND 255),

    -- Mirrored by qiita_common.models.ena_import.BatchItemState. TEXT/CHECK,
    -- not a Postgres ENUM -- see CLAUDE.md "Enum parity". Keep both sides
    -- in sync by hand.
    state                     TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN
            ('pending', 'resolving', 'registered', 'downloading', 'done', 'failed')),

    -- Set only while/when state = 'failed'; cleared (NULL) on any
    -- subsequent non-failed transition (e.g. a startup-reconcile re-drive
    -- from 'pending').
    failure_reason            TEXT,

    -- Set once register_ena_study succeeds for this accession. Nullable --
    -- unset while pending/resolving, and stays unset on a 'failed' item
    -- that never reached registration.
    study_idx                 BIGINT REFERENCES qiita.study(idx) ON DELETE RESTRICT,

    -- True when THIS item's registration is the one that created study_idx.
    -- An import may only add to a study some import created: a study Qiita
    -- created natively and later deposited carries a bioproject_accession too,
    -- so matching on the accession alone would silently merge foreign ENA
    -- samples into curated data. The driver refuses when no item records having
    -- created the matched study.
    study_created             BOOLEAN NOT NULL DEFAULT false,

    -- One work_ticket_idx per sequenced_pool register_ena_study created for
    -- this study (one per distinct platform) -- appended as the batch driver
    -- submits each pool's download-ena-study ticket via submit_work_ticket_core,
    -- so a crash mid-submit leaves the tickets already sent recorded here (a
    -- REGISTERED item is re-driven at startup, which reuses them). Array, not a
    -- join table: a small, per-item fan-out the batch driver produces. It is
    -- read back at GET-time (fetch_batch_status rolls up each ticket's state);
    -- there is no FK (Postgres cannot FK an array's elements), so a deleted
    -- work_ticket would leave a dangling idx that the rollup treats as
    -- non-terminal -- work_ticket rows are not deleted in this flow.
    download_work_ticket_idxs BIGINT[] NOT NULL DEFAULT '{}',

    -- Per-run registration outcomes for this item's study, one JSON object per
    -- run: {run_accession, status, failure_reason, missing_required}. Written
    -- once register_ena_study runs (mirrors
    -- qiita_common.models.ena_import.RunImportOutcome). Surfaced by
    -- GET /ena-import-batch/{idx} so per-run failures and harmonization gaps
    -- (checklist-required fields ENA did not supply) are visible, not dropped.
    run_outcomes              JSONB NOT NULL DEFAULT '[]',

    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE qiita.ena_import_batch_item IS
    'One ENA/SRA study accession within a qiita.ena_import_batch, tracked '
    'independently through resolve -> register -> download-submit so one '
    'accession''s failure never affects its siblings. state mirrors '
    'qiita_common.models.ena_import.BatchItemState.';

COMMENT ON COLUMN qiita.ena_import_batch_item.download_work_ticket_idxs IS
    'One work_ticket_idx per sequenced_pool this item''s study registered '
    'into (one per distinct platform). GET /ena-import-batch/{idx} rolls up '
    'these tickets'' qiita.work_ticket.state on demand to report this item '
    'as done / downloading / failed(download), without mutating this row.';

CREATE INDEX ena_import_batch_item_batch_idx_idx
    ON qiita.ena_import_batch_item (batch_idx);

-- Backs the import-created guard: before registering into a study that already
-- exists, the driver asks whether any item created it. Partial -- only the
-- created rows are ever probed.
CREATE INDEX ena_import_batch_item_study_created_idx
    ON qiita.ena_import_batch_item (study_idx) WHERE study_created;

-- One item per (batch, study accession): create_ena_import_batch already
-- de-duplicates accessions in Python before insert, so this makes that a DB
-- invariant too -- it is what bounds concurrent registration to one writer per
-- study within a batch.
ALTER TABLE qiita.ena_import_batch_item
    ADD CONSTRAINT ena_import_batch_item_unique_accession_per_batch
    UNIQUE (batch_idx, ena_study_accession);

-- Startup reconcile (reconcile_inflight_batches) re-drives every item that has
-- not reached a terminal/self-owning state -- pending/resolving AND registered
-- (a registered item still owes its download-ticket submissions, which the
-- REGISTERED->downloading window can crash before finishing). This index makes
-- that scan cheap.
CREATE INDEX ena_import_batch_item_state_idx
    ON qiita.ena_import_batch_item (state)
    WHERE state IN ('pending', 'resolving', 'registered');

CREATE TRIGGER ena_import_batch_item_set_updated_at
    BEFORE UPDATE ON qiita.ena_import_batch_item
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- migrate:down

DROP TRIGGER IF EXISTS ena_import_batch_item_set_updated_at ON qiita.ena_import_batch_item;
DROP TABLE IF EXISTS qiita.ena_import_batch_item;
DROP TABLE IF EXISTS qiita.ena_import_batch;
