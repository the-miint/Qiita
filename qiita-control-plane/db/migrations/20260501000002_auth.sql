-- migrate:up

-- =============================================================================
-- USER SUBTYPE (humans authenticating via OIDC)
-- =============================================================================
-- `user` is SQL-reserved — PostgreSQL parses bare `user` as the `USER`
-- function (equivalent to `CURRENT_USER`). The schema-qualified form
-- `qiita.user` is unambiguous in DDL/DML positions and needs no double
-- quoting, which is why DDL like `CREATE TABLE qiita.user (...)`, queries
-- like `SELECT ... FROM qiita.user`, and FK references compile cleanly.
-- Always use the qualified form; never bare `user`.

CREATE TABLE qiita.user (
    principal_idx              BIGINT PRIMARY KEY
                               REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    -- The system principal must remain bare. Sentinel containment.
    CHECK (principal_idx <> 1),
    email                      CITEXT NOT NULL UNIQUE,
    -- Free-form profile fields use NOT NULL DEFAULT '' so the
    -- `profile_complete` generated column below can rely on simple
    -- `<> ''` comparisons under two-valued logic. If these were nullable,
    -- the generated expression would need `IS NOT NULL AND <> ''` per
    -- field to dodge SQL's three-valued NULL semantics.
    affiliation                TEXT NOT NULL DEFAULT '',
    address                    TEXT NOT NULL DEFAULT '',
    phone                      TEXT NOT NULL DEFAULT '',
    receive_processing_emails  BOOLEAN NOT NULL DEFAULT TRUE,
    -- orcid uses NULL (not '') because the format has no meaningful empty
    -- value — an ORCID is either present and matches the pattern, or it's
    -- absent. Nullable + CHECK-on-not-null is the natural shape here.
    orcid                      VARCHAR(19)
        CHECK (orcid IS NULL OR orcid ~ '^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$'),
    profile_complete           BOOLEAN GENERATED ALWAYS AS
        (affiliation <> '' AND address <> '' AND phone <> '') STORED,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER user_set_updated_at
    BEFORE UPDATE ON qiita.user
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- The `profile_complete` generated column on qiita.user returns TRUE iff
-- affiliation, address, and phone are all non-empty. Routes that need to
-- surface *which* fields are empty (currently `POST /auth/pat`'s 422 body)
-- used to duplicate the field list in Python, which would silently rot if a
-- 4th required field were added to the generated column. Calling this
-- function from SQL keeps the field list in exactly one place — this
-- migration — alongside the generated column expression.
--
-- Add a new required field by:
--   1. Adding it to the `profile_complete` GENERATED expression on
--      qiita.user (would be a column ADD + a generated-column update via
--      a follow-up migration).
--   2. Adding the matching `CASE WHEN <new> = '' THEN '<new>' END` line
--      to the ARRAY below.

CREATE FUNCTION qiita.user_profile_missing_fields(
    affiliation TEXT, address TEXT, phone TEXT
) RETURNS TEXT[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT array_remove(ARRAY[
        CASE WHEN affiliation = '' THEN 'affiliation' END,
        CASE WHEN address     = '' THEN 'address'     END,
        CASE WHEN phone       = '' THEN 'phone'       END
    ], NULL)
$$;


-- Links an external OIDC identity to one of our principals. The auth
-- resolver creates a row here on first login — mapping the JWT's
-- `iss` + `sub` claims to the new qiita.user — and looks it up on every
-- subsequent login. Email changes at the IdP therefore don't break the
-- link, because we look up by (iss, sub), not by email.
--
-- issuer  — the OIDC `iss` claim (the IdP / realm URL).
-- subject — the OIDC `sub` claim: an opaque, stable per-user identifier
--           issued by the IdP. NOT an email; deliberately opaque so it
--           survives upstream profile changes.
--
-- (issuer, subject) is the PK so the same upstream identity can never
-- link to two principals.
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
    -- A token's scopes are checked by `require_scope(...)` guards on every
    -- protected route. An empty array means the bearer authenticates (proves
    -- identity to `get_current_principal`) but every scoped guard returns
    -- 403 — i.e., an identity-only token with no operational permissions.
    -- The `DEFAULT '{}'` is a safety net: if a future mint path ever omits
    -- scopes, the resulting token is inert rather than failing the INSERT
    -- or defaulting to something permissive. Today's mint paths always pass
    -- scopes explicitly, so the default does not fire in practice.
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
-- Exhaustive within scope — every credential-affecting action gets a row.
-- Forensic gaps surface during incidents; the append-only trigger makes
-- retroactive backfill unsound; SOC 2 / ISO 27001 require it for credential
-- and privilege changes. The same standard does not extend to domain
-- tables — their mutations are high-volume and routine, so universal CDC
-- there would be expensive noise.

CREATE TABLE qiita.auth_events (
    event_idx           BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    -- TEXT — not ENUM, not a controlled-vocab lookup table — for forward
    -- compatibility of this append-only audit log. Adding a new event type
    -- is a one-line edit to the AuthEventType StrEnum in
    -- qiita_common.auth_constants; the StrEnum already catches typos at
    -- every emit site, so DB-level enforcement adds friction without much
    -- additional safety. ENUM would cost `ALTER TYPE ... ADD VALUE` per
    -- new value (forward-only, awkward to revert); a controlled-vocab
    -- lookup table would cost a row-per-value migration plus drift risk
    -- if a writer ever emitted a value the lookup hadn't got yet. Either
    -- becomes the right shape if event types start needing per-type
    -- metadata (severity, pii_class), DB-queryable introspection by an
    -- auditor, or an operator-managed vocabulary — the project already
    -- uses controlled-vocab tables where those hold (qiita.study_tag,
    -- qiita.terminologies, qiita.metadata_checklists).
    event_type          TEXT NOT NULL,
        -- oidc_login | oidc_create_principal | oidc_create_principal_email_conflict | email_drift
        -- token_mint | token_use | token_revoke | token_verify_failure
        -- system_role_change | principal_disabled | principal_enabled | principal_retired
    -- principal_idx is the *subject* of the event — whose state is being
    -- changed or what the event is about. actor_principal_idx is *who
    -- performed the action* that produced this row. They're equal for
    -- self-service actions (e.g. self-PAT mint); they diverge for
    -- admin-on-behalf-of actions (admin disables a user → subject = the
    -- user, actor = the admin); actor_principal_idx is NULL for
    -- system-originated events with no human actor (OIDC first-login,
    -- automatic token revocation on principal retire).
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


-- =============================================================================
-- CLI LOGIN ONE-TIME CODES
-- =============================================================================
-- Single-use exchange codes that bridge the AuthRocket → /auth/handoff →
-- localhost-loopback → CLI flow. The handoff route mints a PAT, stores its
-- plaintext here under a freshly-generated `ot_code`, then redirects the
-- browser to the CLI's loopback with the plaintext ot_code in the query
-- string. The CLI POSTs the ot_code back to /auth/cli-exchange, which atomically
-- consumes the row and returns the PAT plaintext.
--
-- Plaintext PAT lives here briefly; the table is the only place qiita stores
-- a plaintext token at rest. The TTL is short (default 30s, capped via
-- CLI_LOGIN_CODE_TTL_SECONDS) so an intercepted ot_code expires almost
-- immediately. Single-use is enforced atomically via
-- `UPDATE … WHERE consumed_at IS NULL RETURNING …`.
--
-- ot_code is BYTEA holding SHA-256 of the plaintext code (32 bytes), matching
-- the api_tokens.token_hash convention — the wire-format ot_code is a
-- secrets.token_urlsafe(32) string, never stored in cleartext.

-- token_idx pins the row to the exact api_tokens row whose plaintext lives
-- here; without it /auth/cli-exchange would have to guess "the most recent
-- token for this principal," which races a parallel mint into the wrong
-- metadata payload. ON DELETE CASCADE so token revocation/expiry GC also
-- sweeps any matching unredeemed code.
CREATE TABLE qiita.cli_login_codes (
    ot_code         BYTEA PRIMARY KEY,
    principal_idx   BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE CASCADE,
    token_idx       BIGINT NOT NULL REFERENCES qiita.api_tokens(token_idx) ON DELETE CASCADE,
    plaintext_pat   TEXT NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    consumed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT cli_login_codes_consumed_after_created
        CHECK (consumed_at IS NULL OR consumed_at >= created_at),
    CONSTRAINT cli_login_codes_expires_after_created
        CHECK (expires_at > created_at)
);

-- Partial index: hot path is "find unconsumed codes near expiry" for
-- garbage-collection. Active rows are the only ones queried for redemption,
-- so excluding consumed rows keeps the index small.
CREATE INDEX cli_login_codes_expires_at
    ON qiita.cli_login_codes (expires_at)
    WHERE consumed_at IS NULL;

COMMENT ON TABLE qiita.cli_login_codes IS
    'Short-lived single-use codes for the qiita-admin login → CLI loopback handoff. '
    'Stores plaintext PAT briefly between handoff and CLI exchange. See docs/auth.md.';


-- migrate:down

DROP TABLE IF EXISTS qiita.cli_login_codes;
DROP TRIGGER IF EXISTS principal_retire_revoke_tokens ON qiita.principal;
DROP FUNCTION IF EXISTS qiita.tg_revoke_tokens_on_retire();
DROP TRIGGER IF EXISTS auth_events_no_delete ON qiita.auth_events;
DROP TRIGGER IF EXISTS auth_events_no_update ON qiita.auth_events;
DROP FUNCTION IF EXISTS qiita.tg_auth_events_immutable();
DROP TRIGGER IF EXISTS service_account_subtype_exclusion ON qiita.service_account;
DROP TRIGGER IF EXISTS user_subtype_exclusion ON qiita.user;
DROP FUNCTION IF EXISTS qiita.tg_principal_subtype_exclusion();
DROP TRIGGER IF EXISTS user_set_updated_at ON qiita.user;
DROP FUNCTION IF EXISTS qiita.user_profile_missing_fields(TEXT, TEXT, TEXT);
DROP TABLE IF EXISTS qiita.auth_events;
DROP TABLE IF EXISTS qiita.api_tokens;
DROP TABLE IF EXISTS qiita.service_account;
DROP TABLE IF EXISTS qiita.user_identities;
DROP TABLE IF EXISTS qiita.user;
