-- migrate:up

-- Email-notification bookkeeping on qiita.work_ticket plus a general-purpose
-- email evidence table. A terminal work_ticket (completed / no_data / failed)
-- with notified_at IS NULL is the "email owed" signal the in-process notify
-- sweeper consumes; the three terminal transitions in the runner leave the
-- column untouched, so no runner change is needed to emit the signal.

ALTER TABLE qiita.work_ticket
    ADD COLUMN notified_at     TIMESTAMPTZ,
    ADD COLUMN notify_attempts INT NOT NULL DEFAULT 0;

COMMENT ON COLUMN qiita.work_ticket.notified_at IS
    'When the originator was emailed about this ticket''s terminal outcome. '
    'NULL on a terminal (completed/no_data/failed) ticket means "email owed"; '
    'the notify sweeper stamps it once a digest covering the ticket is sent, or '
    'when the ticket is drained without emailing (gate opt-out, stale past '
    'NOTIFY_MAX_AGE_SECONDS, or dead-letter). The /run redrive resets it to '
    'NULL so a resurrected ticket re-notifies its true final outcome.';

COMMENT ON COLUMN qiita.work_ticket.notify_attempts IS
    'Count of failed notify sends for this ticket. Bounded by '
    'NOTIFY_MAX_ATTEMPTS: once reached, the sweeper writes a dead_letter '
    'receipt and stamps notified_at instead of retrying forever. Reset to 0 by '
    'the /run redrive alongside notified_at.';


-- General-purpose evidence record for every email the system sends. Any email
-- writes one row (auditable, PI-facing mail); it survives a work_ticket DELETE
-- so the evidence persists independent of the ticket that occasioned it.
--
-- status is TEXT + CHECK, NOT a Postgres ENUM — same deliberate carve-out as
-- upload.status / reference.status (see CLAUDE.md "Enum parity"). It is
-- mirrored by qiita_common.models.EmailReceiptStatus; keep the two value sets
-- in sync by hand (a light parity test reads this CHECK via
-- pg_get_constraintdef).
CREATE TABLE qiita.email_receipt (
    idx                     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Which Jinja bundle produced the mail, and the kwargs it rendered
    -- against (includes work_ticket_idxs so a "did we email about ticket Y?"
    -- containment query hits the GIN index below).
    template_name           TEXT NOT NULL,
    template_context        JSONB NOT NULL,

    -- Recipient. recipient_email is the rendered destination (evidence);
    -- recipient_principal_idx ties it back to a principal when the mail was
    -- addressed to one (nullable — a future non-principal recipient sets it
    -- NULL). RESTRICT is not needed: the receipt is history, so a principal
    -- delete simply orphans nothing (FK left as default NO ACTION).
    recipient_email         CITEXT NOT NULL,
    recipient_principal_idx BIGINT REFERENCES qiita.principal(idx),

    -- Rendered bytes actually sent — the evidence.
    subject                 TEXT NOT NULL,
    body_text               TEXT NOT NULL,
    body_html               TEXT,

    -- Delivery lifecycle. Mirrored by qiita_common.models.EmailReceiptStatus.
    status                  TEXT NOT NULL DEFAULT 'pending'
        CONSTRAINT email_receipt_status_check
        CHECK (status IN ('pending', 'sent', 'failed', 'dead_letter')),

    -- 'smtp' | 'noop' | 'capture' — proves whether sending was live.
    transport               TEXT NOT NULL,
    -- RFC Message-ID / relay id → ties this receipt to provider logs.
    provider_message_id     TEXT,
    attempts                INT NOT NULL DEFAULT 0,
    error                   TEXT,
    -- Content hash of the rendered template revision, for reproducibility.
    template_sha            TEXT,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at                 TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE qiita.email_receipt IS
    'Durable evidence record for every email the system sends. General-purpose '
    '(any mail writes one); survives a work_ticket DELETE. status is TEXT+CHECK '
    'mirrored by qiita_common.models.EmailReceiptStatus.';

COMMENT ON CONSTRAINT email_receipt_status_check ON qiita.email_receipt IS
    'Allowed status values: pending, sent, failed, dead_letter. Mirrored by the '
    'Python twin qiita_common.models.EmailReceiptStatus; keep the two value sets '
    'in sync by hand.';

-- "List a principal's mail" audit query.
CREATE INDEX email_receipt_recipient_idx
    ON qiita.email_receipt (recipient_principal_idx);

-- Answer "did we email about work_ticket Y?" with a containment query
-- `template_context @> '{"work_ticket_idxs":[Y]}'`. Default jsonb_ops GIN — the
-- `->` expression form would NOT hit this index; only `@>` containment does.
CREATE INDEX email_receipt_template_context_idx
    ON qiita.email_receipt USING gin (template_context);

CREATE TRIGGER email_receipt_set_updated_at
    BEFORE UPDATE ON qiita.email_receipt
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- First-deploy guard: mark all pre-existing terminal tickets notified so the
-- first sweep doesn't email history. The set_updated_at trigger bumps
-- updated_at on these already-terminal rows — accepted: they immediately
-- become notified_at IS NOT NULL (excluded from every sweep), so the only cost
-- is an ETag bump on dead rows no in-flight PATCH cares about. (Deliberately
-- NOT SET LOCAL session_replication_role = replica to preserve updated_at:
-- that needs superuser and would turn a least-priv prod migration role red
-- with "permission denied to set parameter" — a CI-green/prod-red trap.)
UPDATE qiita.work_ticket
    SET notified_at = now()
    WHERE state IN ('completed', 'no_data', 'failed');


-- migrate:down

DROP TABLE IF EXISTS qiita.email_receipt;

ALTER TABLE qiita.work_ticket
    DROP COLUMN IF EXISTS notify_attempts,
    DROP COLUMN IF EXISTS notified_at;
