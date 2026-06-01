-- migrate:up transaction:false
-- Postgres 12+ forbids using a newly-added ENUM value in any other statement
-- of the same transaction (SQLSTATE 55P04, "unsafe use of new value of enum
-- type"). dbmate sends every statement of a migration file to libpq in a
-- single Exec, which behaves as one implicit transaction even with the
-- transaction:false directive — so the ALTER TYPE ADD VALUE has to live in
-- its own migration file, with no other statement referencing the value.
-- The matching `'sequenced_pool'` arm of the CHECK constraint, the FK
-- column, and the indexes that reference both land in the next migration.

ALTER TYPE qiita.scope_target_kind ADD VALUE IF NOT EXISTS 'sequenced_pool';

-- migrate:down transaction:false
-- A Postgres ENUM value cannot be removed without recreating the type.
-- 'sequenced_pool' stays in the ENUM after down. Safe because the FK column
-- and the CHECK arm that reference it land in the next migration's down
-- block, so no row carries this kind by the time this down runs.
