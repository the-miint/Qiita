-- migrate:up

-- Add the 'indexing' lifecycle state. It sits between 'loading' and 'active'
-- and is entered only by the host-reference-add workflow while the rype index
-- is built. Regular references continue to go loading → active directly, so
-- both edges remain valid. Mirrors qiita_common.models.ReferenceStatus and the
-- VALID_STATUS_TRANSITIONS table. Status stays plain TEXT + CHECK (not a
-- Postgres ENUM), per the comment on the original reference migration.
ALTER TABLE qiita.reference
    DROP CONSTRAINT reference_status_check;

ALTER TABLE qiita.reference
    ADD CONSTRAINT reference_status_check
    CHECK (status IN ('pending', 'hashing', 'minting', 'loading', 'indexing', 'active', 'failed'));


-- migrate:down

-- Re-tighten to the original set. Fails loudly if any row is mid-indexing,
-- which is the correct behaviour — don't silently strand an 'indexing' row.
ALTER TABLE qiita.reference
    DROP CONSTRAINT reference_status_check;

ALTER TABLE qiita.reference
    ADD CONSTRAINT reference_status_check
    CHECK (status IN ('pending', 'hashing', 'minting', 'loading', 'active', 'failed'));
