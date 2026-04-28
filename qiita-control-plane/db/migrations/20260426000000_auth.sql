-- migrate:up

CREATE EXTENSION IF NOT EXISTS citext;


-- =============================================================================
-- DISABLED STATE on existing principal table
-- =============================================================================
-- Adds the temporary-block primitive alongside the existing retired BOOLEAN.
-- The auth layer rejects login / token-use when either is true; only retired
-- is terminal. Disabling does NOT auto-revoke tokens; admin can bulk-revoke
-- separately if desired. principal_not_both_disabled_and_retired forbids
-- the simultaneous-true case at the schema level.

ALTER TABLE qiita.principal
    ADD COLUMN disabled         BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN disabled_at      TIMESTAMPTZ,
    ADD COLUMN disabled_by_idx  BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    ADD COLUMN disable_reason   TEXT,
    ADD CONSTRAINT principal_disabled_consistent CHECK (
        (disabled = false
            AND disabled_at IS NULL
            AND disabled_by_idx IS NULL
            AND disable_reason IS NULL)
        OR
        (disabled = true
            AND disabled_at IS NOT NULL
            AND disabled_by_idx IS NOT NULL)
    ),
    ADD CONSTRAINT principal_not_both_disabled_and_retired
        CHECK (NOT (disabled AND retired)),
    -- The system principal (idx=1) must remain in the active state.
    -- Disabling or retiring it would block automatic system-generated audit
    -- events that record it as the actor.
    ADD CONSTRAINT principal_system_principal_always_active
        CHECK (idx <> 1 OR (NOT disabled AND NOT retired));


-- =============================================================================
-- SYSTEM PRINCIPAL (seeded at idx=1)
-- =============================================================================
-- The system principal acts as:
--   * Backfill target for pre-auth historical FKs (e.g.
--     `qiita.references.created_by_idx`).
--   * Audit-event "actor" for system-generated events (e.g., automatic
--     token revocation on retirement, fired by a DB trigger).
-- It is neither a user nor a service_account (no subtype row exists for it,
-- enforced by CHECK (principal_idx <> 1) on each subtype). It cannot
-- authenticate. The deferred self-FK on principal.created_by_idx lets
-- created_by_idx=1 reference itself within this transaction (dbmate wraps
-- each migration in a transaction).
-- system_role='system_admin' is for audit-log self-documentation; it grants
-- no capability since the principal has no credentials.

INSERT INTO qiita.principal (idx, display_name, system_role, created_by_idx)
    OVERRIDING SYSTEM VALUE
    VALUES (1, 'system', 'system_admin', 1);
SELECT setval(pg_get_serial_sequence('qiita.principal', 'idx'), 1);


-- =============================================================================
-- USER SUBTYPE (humans authenticating via OIDC)
-- =============================================================================

CREATE TABLE qiita.user (
    principal_idx              BIGINT PRIMARY KEY
                               REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    -- The system principal must remain bare. Sentinel containment.
    CHECK (principal_idx <> 1),
    email                      CITEXT NOT NULL UNIQUE,
    affiliation                TEXT NOT NULL DEFAULT '',
    address                    TEXT NOT NULL DEFAULT '',
    phone                      TEXT NOT NULL DEFAULT '',
    receive_processing_emails  BOOLEAN NOT NULL DEFAULT TRUE,
    orcid                      VARCHAR(19)
        CHECK (orcid IS NULL OR orcid ~ '^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$'),
    profile_complete           BOOLEAN GENERATED ALWAYS AS
        (affiliation <> '' AND address <> '' AND phone <> '') STORED,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reuse the shared updated_at trigger function defined in
-- 20260423000001_studies.sql; do not redefine.
CREATE TRIGGER user_set_updated_at
    BEFORE UPDATE ON qiita.user
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


CREATE TABLE qiita.user_identities (
    principal_idx  BIGINT NOT NULL REFERENCES qiita.user(principal_idx) ON DELETE RESTRICT,
    issuer         TEXT NOT NULL,
    subject        TEXT NOT NULL,
    linked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (issuer, subject)
);
CREATE INDEX user_identities_principal_idx ON qiita.user_identities(principal_idx);


-- =============================================================================
-- SERVICE_ACCOUNT SUBTYPE (workers / cron)
-- =============================================================================

CREATE TABLE qiita.service_account (
    principal_idx  BIGINT PRIMARY KEY
                   REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    -- Sentinel containment.
    CHECK (principal_idx <> 1),
    name           TEXT NOT NULL UNIQUE,
    description    TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- =============================================================================
-- SUBTYPE MUTUAL EXCLUSION
-- =============================================================================
-- A principal is at most one of {user, service_account}. The trigger fires
-- BEFORE INSERT on each subtype table; updates and deletes are unaffected
-- because subtype rows don't change kind in place — kind transitions require
-- explicit DELETE + INSERT.

CREATE FUNCTION qiita.tg_principal_subtype_exclusion() RETURNS trigger AS $$
BEGIN
    -- Serialize concurrent INSERTs that target the same principal_idx into
    -- the two subtype tables. Without this, two parallel transactions —
    -- INSERT INTO qiita.user (principal_idx=N) and
    -- INSERT INTO qiita.service_account (principal_idx=N) —
    -- would both pass their EXISTS check (each sees the other table empty
    -- because the other transaction has not committed yet) and both would
    -- succeed, leaving the principal as both a user AND a service_account.
    -- The advisory lock is held for the duration of the transaction; the
    -- second-arriving INSERT blocks until the first commits, then its
    -- EXISTS check sees the row and raises.
    PERFORM pg_advisory_xact_lock(NEW.principal_idx);

    IF TG_TABLE_NAME = 'user' AND EXISTS (
        SELECT 1 FROM qiita.service_account WHERE principal_idx = NEW.principal_idx
    ) THEN
        RAISE EXCEPTION 'principal % is already a service_account; cannot also be a user',
            NEW.principal_idx;
    END IF;
    IF TG_TABLE_NAME = 'service_account' AND EXISTS (
        SELECT 1 FROM qiita.user WHERE principal_idx = NEW.principal_idx
    ) THEN
        RAISE EXCEPTION 'principal % is already a user; cannot also be a service_account',
            NEW.principal_idx;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_subtype_exclusion
    BEFORE INSERT ON qiita.user
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_principal_subtype_exclusion();

CREATE TRIGGER service_account_subtype_exclusion
    BEFORE INSERT ON qiita.service_account
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_principal_subtype_exclusion();


-- =============================================================================
-- API TOKENS (opaque qk_ tokens; PATs for humans, service tokens for workers)
-- =============================================================================

CREATE TABLE qiita.api_tokens (
    token_idx      BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    principal_idx  BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    -- The system principal cannot hold tokens.
    CHECK (principal_idx <> 1),
    token_hash     BYTEA NOT NULL UNIQUE,
    label          TEXT NOT NULL,
    scopes         TEXT[] NOT NULL DEFAULT '{}',
    expires_at     TIMESTAMPTZ,
    revoked_at     TIMESTAMPTZ,
    last_used_at   TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX api_tokens_hash_active   ON qiita.api_tokens(token_hash) WHERE revoked_at IS NULL;
CREATE INDEX api_tokens_principal_idx ON qiita.api_tokens(principal_idx);
-- Cannot prune by expiry in the partial-index predicate (now() is non-IMMUTABLE);
-- expiry is checked at verify time. Do not "improve" this.


-- =============================================================================
-- AUTH EVENTS (immutable audit log)
-- =============================================================================

CREATE TABLE qiita.auth_events (
    event_idx           BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    event_type          TEXT NOT NULL,
        -- oidc_login | oidc_create_principal | oidc_create_principal_email_conflict | email_drift
        -- token_mint | token_use | token_revoke | token_verify_failure
        -- system_role_change | principal_disabled | principal_enabled | principal_retired
    principal_idx       BIGINT REFERENCES qiita.principal(idx),
    actor_principal_idx BIGINT REFERENCES qiita.principal(idx),
    detail              JSONB NOT NULL DEFAULT '{}',
    occurred_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX auth_events_principal_idx_time ON qiita.auth_events(principal_idx, occurred_at);
CREATE INDEX auth_events_type_time          ON qiita.auth_events(event_type, occurred_at);

-- Trigger: auth_events is append-only. The whole point is forensic integrity;
-- a route handler that "just needs to fix one detail" defeats it.
CREATE FUNCTION qiita.tg_auth_events_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'qiita.auth_events is append-only';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER auth_events_no_update BEFORE UPDATE ON qiita.auth_events
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_auth_events_immutable();
CREATE TRIGGER auth_events_no_delete BEFORE DELETE ON qiita.auth_events
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_auth_events_immutable();


-- =============================================================================
-- TOKEN REVOCATION ON RETIREMENT
-- =============================================================================
-- Disabling does NOT auto-revoke; only retirement does (terminal).

CREATE FUNCTION qiita.tg_revoke_tokens_on_retire() RETURNS trigger AS $$
BEGIN
    IF NEW.retired = true AND OLD.retired = false THEN
        UPDATE qiita.api_tokens SET revoked_at = now()
          WHERE principal_idx = NEW.idx AND revoked_at IS NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER principal_retire_revoke_tokens
    AFTER UPDATE OF retired ON qiita.principal
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_revoke_tokens_on_retire();


-- migrate:down
-- The system principal seed is removed here so that 'down then up' cycles
-- back to a clean state during development. Later migrations FK other rows
-- to idx=1 (e.g. `qiita.references.created_by_idx`); when those exist this
-- DELETE will fail with a foreign-key error until they are taken down too,
-- which is the standard sequential-down expectation. The dependency is
-- therefore captured implicitly by FK enforcement rather than by a
-- leave-it-in-place comment that breaks the down/up loop.

DROP TRIGGER IF EXISTS principal_retire_revoke_tokens ON qiita.principal;
DROP FUNCTION IF EXISTS qiita.tg_revoke_tokens_on_retire();
DROP TRIGGER IF EXISTS auth_events_no_delete ON qiita.auth_events;
DROP TRIGGER IF EXISTS auth_events_no_update ON qiita.auth_events;
DROP FUNCTION IF EXISTS qiita.tg_auth_events_immutable();
DROP TRIGGER IF EXISTS service_account_subtype_exclusion ON qiita.service_account;
DROP TRIGGER IF EXISTS user_subtype_exclusion ON qiita.user;
DROP FUNCTION IF EXISTS qiita.tg_principal_subtype_exclusion();
DROP TRIGGER IF EXISTS user_set_updated_at ON qiita.user;
DROP TABLE IF EXISTS qiita.auth_events;
DROP TABLE IF EXISTS qiita.api_tokens;
DROP TABLE IF EXISTS qiita.service_account;
DROP TABLE IF EXISTS qiita.user_identities;
DROP TABLE IF EXISTS qiita.user;
DELETE FROM qiita.principal WHERE idx = 1;
ALTER TABLE qiita.principal
    DROP CONSTRAINT IF EXISTS principal_system_principal_always_active,
    DROP CONSTRAINT IF EXISTS principal_not_both_disabled_and_retired,
    DROP CONSTRAINT IF EXISTS principal_disabled_consistent,
    DROP COLUMN IF EXISTS disable_reason,
    DROP COLUMN IF EXISTS disabled_by_idx,
    DROP COLUMN IF EXISTS disabled_at,
    DROP COLUMN IF EXISTS disabled;
-- citext extension intentionally not dropped: may be used elsewhere.
