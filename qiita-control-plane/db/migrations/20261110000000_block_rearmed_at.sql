-- migrate:up

-- When a completed align block was last re-armed by
-- `POST /alignment-definition/{idx}/rerun-block`. A re-armed block that has not
-- completed again is `pending` with `pending` gates, as a block that never finished
-- is; abandon-plan reads this column to tell them apart, because abandoning a
-- re-armed block purges rows its completed siblings registered. NULL for a block
-- never re-armed, and for every block before this migration.
ALTER TABLE qiita.block ADD COLUMN rearmed_at TIMESTAMPTZ;

COMMENT ON COLUMN qiita.block.rearmed_at IS
    'When rerun-block last re-armed this completed block; NULL if never. '
    'abandon-plan refuses a re-armed block that is not completed unless the caller '
    'passes allow_rearmed (actions/align_abandon.py).';

-- migrate:down

ALTER TABLE qiita.block DROP COLUMN rearmed_at;
