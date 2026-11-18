-- migrate:up

-- A krepp placement index is built outside Qiita and registered onto an existing
-- reference. Unlike the index types before it, a krepp build is copied by the
-- cluster admins to every compute node's local disk, and it is not served until it
-- has been checked on a node and activated. This migration adds what that needs.

-- 1. `krepp` joins the index_type allow-list (plain TEXT + CHECK, as
--    20260711000000 explains; INDEX_TYPE_KREPP in qiita_common.models is the
--    Python constant).
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

-- 3. What fs_path is relative to. 'absolute': fs_path is a host path (every type
--    before krepp). 'node_local': fs_path is relative, and resolves under the
--    orchestrator's PATH_NODE_LOCAL on a compute node and under PATH_DERIVED for
--    the master copy, so one row names the same folder on every node.
ALTER TABLE qiita.reference_index
    ADD COLUMN fs_path_root TEXT NOT NULL DEFAULT 'absolute'
        CONSTRAINT reference_index_fs_path_root_check
        CHECK (fs_path_root IN ('absolute', 'node_local'));
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_krepp_is_node_local
    CHECK ((index_type = 'krepp') = (fs_path_root = 'node_local'));

-- At most one active krepp build per reference: activation retires the previous
-- one before activating the next, in one transaction.
CREATE UNIQUE INDEX reference_index_one_active_krepp
    ON qiita.reference_index (reference_idx)
    WHERE index_type = 'krepp' AND state = 'active';

-- 4. The krepp build a ticket is fixed to, written once by the runner when the
--    ticket first resolves its build, as it writes mask_idx, alignment_idx and
--    processing_idx. ON DELETE SET NULL mirrors those columns.
ALTER TABLE qiita.work_ticket
    ADD COLUMN reference_index_idx BIGINT
        REFERENCES qiita.reference_index(reference_index_idx) ON DELETE SET NULL;

COMMENT ON COLUMN qiita.work_ticket.reference_index_idx IS
    'The krepp build (qiita.reference_index.reference_index_idx) this ticket is fixed '
    'to, written by the runner the first time the ticket resolves its build and read '
    'back on every later run. NULL for a ticket that uses none. ON DELETE SET NULL so '
    'deleting a reference''s index rows detaches the ticket.';

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
ALTER TABLE qiita.reference_index DROP CONSTRAINT IF EXISTS reference_index_krepp_is_node_local;
ALTER TABLE qiita.reference_index DROP COLUMN IF EXISTS fs_path_root;
ALTER TABLE qiita.reference_index DROP COLUMN IF EXISTS state;
ALTER TABLE qiita.reference_index
    DROP CONSTRAINT IF EXISTS reference_index_index_type_check;
ALTER TABLE qiita.reference_index
    ADD CONSTRAINT reference_index_index_type_check
    CHECK (index_type IN ('rype', 'minimap2', 'bowtie2', 'rype_router'));
