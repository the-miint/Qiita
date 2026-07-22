-- Batch multi-study ENA import: a `qiita.ena_import_batch` row is
-- one `POST /api/v1/ena-import-batch` submission (a list of ENA/SRA study
-- accessions); one `qiita.ena_import_batch_item` row per accession tracks
-- its own resolve/register/download-submit progress independently, so one
-- accession's failure never affects its siblings (T06-3) -- mirrors the
-- per-run isolation `ena_import.registration.register_ena_study` already
-- guarantees, one level up.
--
-- Both tables are additive and reversible; no CREATE TYPE (matches
-- `sequenced_sample.source_archive` / `resolver_kind` and `upload.status` --
-- TEXT/CHECK, not a Postgres ENUM; see CLAUDE.md "Enum parity").
--
-- State machine (qiita.ena_import_batch_item.state, mirrored by
-- qiita_common.models.ena_import.BatchItemState):
--   pending     -> INSERTed alongside the batch; not yet picked up.
--   resolving   -> the background task is resolving study/runs/attributes
--                  (ena_import.get_resolver) for this accession.
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

    -- Which EnaResolver backend every item in this batch resolves through.
    -- Mirrors qiita_control_plane.ena_import.{BACKEND_MIINT,BACKEND_HTTP}.
    resolver_backend             TEXT NOT NULL DEFAULT 'miint'
        CHECK (resolver_backend IN ('miint', 'http')),

    -- Which public archive this batch's reads/metadata come from. Mirrors
    -- qiita_common.models.ena.SourceArchive.
    source_archive               TEXT NOT NULL DEFAULT 'ena'
        CHECK (source_archive IN ('ena', 'sra')),

    -- Transport pinned into every download-ena-study ticket this batch
    -- submits. Only 'http' is supported today -- no Aspera key-staging in
    -- this compute environment (see ARCHITECTURE.md's ENA Study Import
    -- download-ticket-granularity decision); a single-value CHECK so a
    -- future transport addition is a deliberate migration, not a silent
    -- drift.
    download_method               TEXT NOT NULL DEFAULT 'http'
        CHECK (download_method IN ('http'))
);

COMMENT ON TABLE qiita.ena_import_batch IS
    'One POST /api/v1/ena-import-batch submission: a list of ENA/SRA study '
    'accessions fanned out into one qiita.ena_import_batch_item per '
    'accession. resolver_backend/source_archive/download_method mirror '
    'qiita_common.models.ena_import.BatchImportRequest.';


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

    -- One work_ticket_idx per sequenced_pool register_ena_study created for
    -- this study (one per distinct platform, R3) -- populated once the
    -- batch driver submits that pool's download-ena-study ticket via
    -- submit_work_ticket_core. Array, not a join table: this is a small,
    -- write-once-per-item fan-out the batch driver itself produces, not an
    -- independently-queried relationship.
    download_work_ticket_idxs BIGINT[] NOT NULL DEFAULT '{}',

    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE qiita.ena_import_batch_item IS
    'One ENA/SRA study accession within a qiita.ena_import_batch, tracked '
    'independently through resolve -> register -> download-submit so one '
    'accession''s failure never affects its siblings (T06-3). state mirrors '
    'qiita_common.models.ena_import.BatchItemState.';

COMMENT ON COLUMN qiita.ena_import_batch_item.download_work_ticket_idxs IS
    'One work_ticket_idx per sequenced_pool this item''s study registered '
    'into (one per distinct platform). GET /ena-import-batch/{idx} rolls up '
    'these tickets'' qiita.work_ticket.state on demand to report this item '
    'as done / downloading / failed(download), without mutating this row.';

CREATE INDEX ena_import_batch_item_batch_idx_idx
    ON qiita.ena_import_batch_item (batch_idx);

-- Startup reconcile (reconcile_inflight_batches) re-drives every item still
-- pending/resolving; this index makes that scan cheap.
CREATE INDEX ena_import_batch_item_state_idx
    ON qiita.ena_import_batch_item (state)
    WHERE state IN ('pending', 'resolving');

CREATE TRIGGER ena_import_batch_item_set_updated_at
    BEFORE UPDATE ON qiita.ena_import_batch_item
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- migrate:down

DROP TRIGGER IF EXISTS ena_import_batch_item_set_updated_at ON qiita.ena_import_batch_item;
DROP TABLE IF EXISTS qiita.ena_import_batch_item;
DROP TABLE IF EXISTS qiita.ena_import_batch;
