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
-- THREE states, because assembly has two distinct terminal outcomes:
--
--   'pending'   materialized by the runner when it mints the run's
--               processing_idx, before the step loop.
--   'completed' written by the terminal `finalize-assembly-sample` action, which
--               runs after register-files has made the contigs durable.
--   'no_data'   the sample assembled no contig of any kind. The assembly_hash
--               step raises StepNoData, which lands the ticket in NO_DATA and
--               abandons the remaining entries, so the terminal action never
--               runs; the runner's StepNoData handler writes this state instead.
--
-- 'no_data' is a state VALUE rather than a left-'pending' row or a 'completed'
-- one. Left 'pending', the row says "still running" about a run that has ended
-- and will never move again. Written 'completed', it contradicts the ticket,
-- which reads NO_DATA over the same unit — this workflow is prep_sample-scoped,
-- so ticket and gate row describe the same (run, sample).
--
-- THE CONTRACT: {'completed', 'no_data'} is the terminal set. A consumer asking
-- whether the run is over reads both; 'pending' and absence both mean "not
-- over", and absence is never "assembled". A consumer that needs contigs
-- proceeds on 'completed' alone — under 'no_data' there are, by construction,
-- none. Stated in full at repositories/assembly.py::fetch_assembly_sample_state,
-- which consumers point to rather than restate.
--
-- `state` is a deliberate TEXT + CHECK (no Postgres ENUM, no Pydantic twin) —
-- the gate has no wire surface, so it stays out of the enum-parity discipline
-- (the TEXT/CHECK carve-out in CLAUDE.md), mirroring qiita.mask_sample and
-- qiita.alignment_sample.

CREATE TABLE qiita.assembly_sample (
    -- The run identity (qiita.processing). ON DELETE RESTRICT, where
    -- qiita.alignment_sample CASCADEs to its alignment_definition: one
    -- qiita.processing row is SHARED by every sample assembled under the same
    -- params, so a cascade would drop the gate for all of them at once, while
    -- the DuckLake rows stamped with that processing_idx stay — no FK reaches
    -- them, the catalog being a separate database. That leaves data with no
    -- completion state, which is what this table exists to remove. There is no
    -- assembly DELETE path today; RESTRICT makes whoever builds one clear these
    -- rows explicitly. Matches qiita.assembly_membership's own FK to
    -- qiita.processing (declared with no ON DELETE, i.e. NO ACTION).
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
    'Consumers read completion from this state, NEVER from the presence of '
    'qiita.assembly_membership or DuckLake rows -- the assembly tail writes '
    'those across several entries, so a partial footprint looks like a finished '
    'one. Twin of qiita.mask_sample / qiita.alignment_sample.';

COMMENT ON COLUMN qiita.assembly_sample.state IS
    'Gate state: ''pending'' | ''completed'' | ''no_data'' (TEXT + CHECK, not a '
    'Postgres ENUM). The terminal set is {completed, no_data}: a consumer asking '
    'whether the run is over reads both, one that needs contigs reads '
    '''completed'' alone. ''no_data'' means the assembler produced nothing for '
    'this (run, sample) -- an outcome, not an error -- and matches the '
    'work_ticket''s own NO_DATA over the same unit.';

CREATE TRIGGER assembly_sample_set_updated_at
    BEFORE UPDATE ON qiita.assembly_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- migrate:down

DROP TRIGGER IF EXISTS assembly_sample_set_updated_at ON qiita.assembly_sample;
DROP TABLE IF EXISTS qiita.assembly_sample;
