-- migrate:up

-- =============================================================================
-- ASSEMBLY SAMPLE (per-(processing, sample) completion gate)
-- =============================================================================
-- The completion gate for long-read assembly, alongside qiita.mask_sample and
-- qiita.alignment_sample. Without it the only record of a sample's assembly
-- being done is qiita.work_ticket state, which leaves a consumer inferring it
-- from the presence of rows. That inference does not hold: the assembly tail
-- writes qiita.assembly_membership (Postgres) at write-assembly-membership, and
-- the DuckLake tables several entries later at register-files, so a ticket that
-- dies in between leaves membership rows for a run whose sequences were never
-- registered.
--
-- The gate is keyed on the RUN identity (qiita.processing), not on the ticket:
-- processing_idx is the canonical-params hash (mask_idx + assembler) that
-- qiita.assembly_membership and the DuckLake assembly tables are themselves
-- stamped with, so the gate and the data it gates share one key. A re-run under
-- the same params resolves to the same processing_idx and so to the same row.
--
-- Three states, because assembly has two distinct terminal outcomes:
--
--   'pending'   materialized by the runner when it mints the run's
--               processing_idx, before the step loop.
--   'completed' written by the terminal `finalize-assembly-sample` action.
--   'no_data'   the sample assembled no contig of any kind. The assembly_hash
--               step raises StepNoData, which lands the ticket in NO_DATA and
--               abandons the remaining entries, so the terminal action never
--               runs; the runner's StepNoData handler writes this state instead.
--
-- 'no_data' is a state value rather than a left-'pending' row or a 'completed'
-- one. Left 'pending', the row says "still running" about a run that has ended
-- and will never move again. Written 'completed', it contradicts the ticket,
-- which reads NO_DATA over the same unit — this workflow is prep_sample-scoped,
-- so ticket and gate row describe the same (run, sample).
--
-- `state` is a deliberate TEXT + CHECK (no Postgres ENUM, no Pydantic twin) —
-- the gate has no wire surface, so it stays out of the enum-parity discipline
-- (the TEXT/CHECK carve-out in CLAUDE.md), mirroring qiita.mask_sample and
-- qiita.alignment_sample.

CREATE TABLE qiita.assembly_sample (
    -- The run identity (qiita.processing). ON DELETE RESTRICT, where
    -- qiita.alignment_sample CASCADEs to its alignment_definition: one
    -- qiita.processing row is shared by every sample assembled under the same
    -- params, so a cascade would drop the gate for all of them at once, while
    -- the DuckLake rows stamped with that processing_idx stay — no FK reaches
    -- them, the catalog being a separate database — leaving assembled data with
    -- no completion state. There is no assembly DELETE path today; RESTRICT
    -- makes whoever builds one clear these rows explicitly.
    processing_idx   BIGINT NOT NULL
        REFERENCES qiita.processing(processing_idx) ON DELETE RESTRICT,

    -- The prep_sample supertype. RESTRICT mirrors mask_sample /
    -- alignment_sample / work_ticket / block_member: a sample with a live gate
    -- row can't be hard-deleted out from under it.
    prep_sample_idx  BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,

    state            TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'completed', 'no_data')),

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Bumped on every UPDATE by qiita.set_updated_at() (the pending -> terminal flip).
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (processing_idx, prep_sample_idx)
);

COMMENT ON TABLE qiita.assembly_sample IS
    'Per-(processing_idx, prep_sample) completion gate for long-read assembly. '
    'Materialized ''pending'' when the runner mints the run identity, then '
    'written ''completed'' by the terminal finalize-assembly-sample action or '
    '''no_data'' by the runner when assembly_hash found no contig of any kind. '
    'Completion is this column''s value: the presence of '
    'qiita.assembly_membership or DuckLake rows does not imply it, because the '
    'assembly tail writes those across several workflow entries. ''completed'' '
    'and ''no_data'' are terminal for the run; ''pending'' and a missing row '
    'both mean the run is not over. Twin of qiita.mask_sample / '
    'qiita.alignment_sample.';

-- The PRIMARY KEY leads with processing_idx, and every gate function in
-- repositories/assembly.py supplies both key columns, so it serves all of them.
-- The accesses that lead with prep_sample_idx instead come from the FK: the
-- ON DELETE RESTRICT check Postgres runs before deleting a qiita.prep_sample, and
-- the matching clear in actions/sequenced_pool.py's pool cascade. A composite
-- btree cannot be used on its non-leading column, so both scan the table.
--
-- Those two sites do not on their own establish that the index is needed:
-- qiita.mask_sample has the same FK and the same cascade clear with no such
-- index. It is here because the table is created empty, so it costs one
-- CREATE INDEX over zero rows. Single-column — prep_sample_idx is the whole
-- predicate at both sites; qiita.alignment_sample's equivalent carries a second
-- column for a GROUP BY this gate has no read for.
CREATE INDEX qiita_assembly_sample_prep_sample_idx
    ON qiita.assembly_sample (prep_sample_idx);

CREATE TRIGGER assembly_sample_set_updated_at
    BEFORE UPDATE ON qiita.assembly_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- migrate:down

DROP TRIGGER IF EXISTS assembly_sample_set_updated_at ON qiita.assembly_sample;
DROP TABLE IF EXISTS qiita.assembly_sample;
