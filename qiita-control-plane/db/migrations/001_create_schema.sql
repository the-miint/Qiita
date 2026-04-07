-- migrate:up
CREATE SCHEMA IF NOT EXISTS qiita;

-- migrate:down
DROP SCHEMA IF EXISTS qiita;
