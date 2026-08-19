-- migrate:up

-- cli_login_code holds the only plaintext token qiita stores at rest. The
-- column was NOT NULL and never cleared, so a consumed row — and an abandoned,
-- expired-but-unconsumed one — retained a directly-usable bearer PAT for the
-- api_token's full life (up to 90 days), long after the short (default 30s)
-- ot_code TTL. Allow NULL so the redemption path can scrub the plaintext the
-- moment it is handed to the CLI, and reclaim every already-leaked plaintext now.
ALTER TABLE qiita.cli_login_code ALTER COLUMN plaintext_pat DROP NOT NULL;

UPDATE qiita.cli_login_code
   SET plaintext_pat = NULL
 WHERE consumed_at IS NOT NULL
    OR expires_at <= now();

-- migrate:down

-- A scrubbed plaintext is unrecoverable, so restoring the NOT NULL invariant
-- means dropping the rows that were cleared. Consumed/expired codes are dead
-- anyway (they can never be redeemed), so this loses nothing usable.
DELETE FROM qiita.cli_login_code WHERE plaintext_pat IS NULL;
ALTER TABLE qiita.cli_login_code ALTER COLUMN plaintext_pat SET NOT NULL;
