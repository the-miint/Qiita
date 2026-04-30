-- migrate:up

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
