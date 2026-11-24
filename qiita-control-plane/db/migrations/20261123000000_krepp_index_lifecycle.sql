-- migrate:up

-- A krepp placement index is built outside Qiita and registered onto an existing
-- reference. Unlike the index types before it, it is not served until it has been
-- checked and activated, and a later build replaces it. This migration adds what
-- that needs.

-- 1. `krepp` joins the index_type allow-list (plain TEXT + CHECK;
--    INDEX_TYPE_KREPP in qiita_common.models is the Python constant).
ALTER TABLE qiita.reference_index
    DROP CONSTRAINT IF EXISTS reference_index_index_type_check;
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_index_type_check
    CHECK (index_type IN ('rype', 'minimap2', 'bowtie2', 'rype_router', 'krepp'));

-- 2. Lifecycle state. Every existing row is a built, served index, so it takes
--    'active'. The default stays for the index types that have no lifecycle;
--    register_index passes the state for krepp ('registered'), and only the
--    activation primitive moves a krepp row to 'active'
--    (qiita_common.models.ReferenceIndexState).
ALTER TABLE qiita.reference_index
    ADD COLUMN state TEXT NOT NULL DEFAULT 'active'
        CONSTRAINT reference_index_state_check
        CHECK (state IN ('registered', 'active', 'retired', 'removed'));
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_lifecycle_is_krepp
    CHECK (index_type = 'krepp' OR state = 'active');

-- 3. A krepp build's fs_path is relative to the orchestrator's PATH_DERIVED, and
--    names a sealed folder jobs read through the node cache
--    (qiita_common.models.NODE_CACHED_INDEX_TYPES); every other type's is a host path.
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_krepp_path_is_relative
    CHECK (index_type <> 'krepp' OR fs_path NOT LIKE '/%');

-- A krepp build's fs_path names its folder, and node-cache copies are deleted by it,
-- so two rows never name one folder.
CREATE UNIQUE INDEX reference_index_one_row_per_krepp_folder
    ON qiita.reference_index (fs_path)
    WHERE index_type = 'krepp';

-- At most one active krepp build per reference: activation retires the previous
-- one before activating the next, in one transaction.
CREATE UNIQUE INDEX reference_index_one_active_krepp
    ON qiita.reference_index (reference_idx)
    WHERE index_type = 'krepp' AND state = 'active';

-- 4. The krepp build a ticket is fixed to (repositories.reference_index says who
--    writes and reads it). ON DELETE SET NULL mirrors mask_idx, alignment_idx and
--    processing_idx.
ALTER TABLE qiita.work_ticket
    ADD COLUMN reference_index_idx BIGINT
        REFERENCES qiita.reference_index(reference_index_idx) ON DELETE SET NULL;

COMMENT ON COLUMN qiita.work_ticket.reference_index_idx IS
    'The krepp build (qiita.reference_index.reference_index_idx) this ticket is fixed '
    'to. NULL for a ticket that uses none. ON DELETE SET NULL so deleting a '
    'reference''s index rows detaches the ticket.';

CREATE INDEX work_ticket_reference_index_idx
    ON qiita.work_ticket (reference_index_idx)
    WHERE reference_index_idx IS NOT NULL;


-- migrate:down

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM qiita.reference_index WHERE index_type = 'krepp') THEN
        RAISE EXCEPTION 'krepp_index_lifecycle down: qiita.reference_index holds krepp rows; '
            'remove them first, or this down block drops the columns they depend on';
    END IF;
END
$$;

DROP INDEX IF EXISTS qiita.work_ticket_reference_index_idx;
ALTER TABLE qiita.work_ticket DROP COLUMN IF EXISTS reference_index_idx;
DROP INDEX IF EXISTS qiita.reference_index_one_active_krepp;
DROP INDEX IF EXISTS qiita.reference_index_one_row_per_krepp_folder;
ALTER TABLE qiita.reference_index DROP CONSTRAINT IF EXISTS reference_index_krepp_path_is_relative;
ALTER TABLE qiita.reference_index DROP CONSTRAINT IF EXISTS reference_index_lifecycle_is_krepp;
ALTER TABLE qiita.reference_index DROP COLUMN IF EXISTS state;
ALTER TABLE qiita.reference_index
    DROP CONSTRAINT IF EXISTS reference_index_index_type_check;
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_index_type_check
    CHECK (index_type IN ('rype', 'minimap2', 'bowtie2', 'rype_router'));
