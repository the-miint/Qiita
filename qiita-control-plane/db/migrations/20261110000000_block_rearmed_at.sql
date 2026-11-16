-- migrate:up

-- When a completed align block was last re-armed by
-- `POST /alignment-definition/{idx}/rerun-block`; NULL for a block never re-armed,
-- and for every block before this migration. abandon-plan reads it
-- (actions/align_abandon.py says why).
ALTER TABLE qiita.block ADD COLUMN rearmed_at TIMESTAMPTZ;

COMMENT ON COLUMN qiita.block.rearmed_at IS
    'When rerun-block last re-armed this completed block; NULL if never. '
    'abandon-plan refuses a re-armed block that is not completed unless the caller '
    'passes allow_rearmed (actions/align_abandon.py).';

-- migrate:down

ALTER TABLE qiita.block DROP COLUMN rearmed_at;
