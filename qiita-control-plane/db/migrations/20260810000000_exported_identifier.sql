-- migrate:up

-- =============================================================================
-- EXPORTED IDENTIFIER (the public handle for one processed sample)
-- =============================================================================
-- A published artifact — a feature table, a BIOM file, the metadata beside them —
-- cannot carry our minted `*_idx` values. They mean nothing outside this system,
-- they expose our structure, and they are a handle we do not promise to keep.
-- This table mints the public alternative: `export_id`, of the form `QM<idx>`.
--
-- It exists because no accession can do the job. A biosample accession is not
-- enough — one biosample sequenced repeatedly has several prep_samples, so the
-- accession does not identify WHICH sequencing of it a row came from. An ENA run
-- accession would identify it, but is NULL until the data is submitted, which it
-- may never be at the time a table is published.
--
-- A row names one PROCESSED SAMPLE: a prep_sample plus the processing it went
-- through. (alignment_idx, prep_sample_idx) is that pair today, and it is unique
-- at rest by construction — qiita.alignment_sample is PRIMARY KEY (alignment_idx,
-- prep_sample_idx), and alignment_idx subsumes the whole config (reference,
-- aligner, mask_idx, shard-set) because qiita.alignment_definition deduplicates
-- on a SHA-256 of it.
--
-- FORWARD PLAN, and it is load-bearing rather than aspirational: other data and
-- processing types are coming, and they will NOT be identified by alignment_idx.
-- Each arrives as an `ALTER TABLE ... ADD COLUMN <kind>_idx BIGINT` plus its own
-- FK, and `exported_identifier_one_processing` below MUST be updated to name it.
-- The check is written with num_nonnulls precisely so that is a one-token edit
-- (same idiom as qiita.reference_exclusion's genome_idx/feature_idx check).

CREATE TABLE qiita.exported_identifier (
    idx              BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- The published handle. GENERATED, not written: Postgres refuses an INSERT
    -- that supplies a value ('cannot insert a non-DEFAULT value'), so export_id
    -- cannot be forged by a caller, cannot drift from idx, and cannot be edited
    -- after publication. There is deliberately no code path that composes this
    -- string — the database is the only author of a public identifier.
    export_id        VARCHAR GENERATED ALWAYS AS ('QM' || idx) STORED,

    -- The processing. Exactly one such column is non-null on a live row; see
    -- exported_identifier_one_processing and the FORWARD PLAN above.
    --
    -- ON DELETE SET NULL, matching work_ticket.alignment_idx, because an
    -- exported identifier OUTLIVES the thing it named. The alignment purge
    -- (admin-only, the disallow-without-delete escape hatch) must stay possible,
    -- and CASCADE would delete a published identifier while RESTRICT would make
    -- publishing block re-alignment forever. Detaching is the only option that
    -- keeps both. The trigger below is what makes the detach legal.
    alignment_idx    BIGINT REFERENCES qiita.alignment_definition(alignment_idx) ON DELETE SET NULL,

    -- The sample. RESTRICT, like every other prep_sample reference: a sample with
    -- a published identifier must not be hard-deleted out from under it.
    prep_sample_idx  BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,

    created_by_idx   BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Retirement, mirroring qiita.sequencing_run's four-column shape. A public
    -- identifier is never deleted; it is retired, so a citation that resolves
    -- today keeps resolving and says what happened.
    --
    -- retired_by_idx is NULLABLE where sequencing_run's is required, and the
    -- difference is deliberate: the common retirement here has no human author
    -- (the trigger below fires from inside an FK action), so NULL means "retired
    -- by the system". retire_reason is required instead — a published identifier
    -- going away without a stated reason is exactly what someone will need later.
    retired          BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx   BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at       TIMESTAMPTZ,
    retire_reason    TEXT,

    -- Exactly one processing identifier on a live row. A retired row is exempt
    -- because a detached row has lost its referent — which is the whole reason
    -- retirement exists here.
    CONSTRAINT exported_identifier_one_processing
        CHECK (retired OR num_nonnulls(alignment_idx) = 1),

    CONSTRAINT exported_identifier_retirement_consistent CHECK (
        (retired = false
            AND retired_at IS NULL
            AND retired_by_idx IS NULL
            AND retire_reason IS NULL)
        OR
        (retired = true
            AND retired_at IS NOT NULL
            AND retire_reason IS NOT NULL)
    )
);

COMMENT ON TABLE qiita.exported_identifier IS
    'Public handle (export_id, ''QM<idx>'') for one processed sample — a '
    'prep_sample plus its processing, (alignment_idx, prep_sample_idx) today. '
    'Exists because no accession identifies a processed sample: a biosample '
    'accession cannot say WHICH sequencing of it, and an ENA run accession is '
    'NULL until submission. Never deleted, only retired, so a published citation '
    'keeps resolving. Extended by ADD COLUMN per new processing type — the '
    'one_processing CHECK must be updated with it.';

COMMENT ON COLUMN qiita.exported_identifier.export_id IS
    'The published identifier, ''QM'' || idx. GENERATED ALWAYS: unforgeable by a '
    'caller, cannot drift from idx, immutable after publication.';

-- One LIVE identifier per processed sample, so the same (alignment, sample)
-- always resolves to the same export_id — the mint is an idempotent upsert on
-- this index. PARTIAL on `NOT retired` so a retired tuple can be re-minted with
-- a fresh identifier instead of colliding with its own history forever.
--
-- alignment_idx LEADS deliberately: the mint looks a cohort up as
-- `alignment_idx = $1 AND prep_sample_idx = ANY($2)`, so the equality binds the
-- leading column and the ANY is an index scan rather than a filter. (A trailing
-- leading-column mismatch is exactly the defect that made alignment_sample need
-- a follow-up index.) A live row can never have a NULL alignment_idx — the
-- one_processing CHECK forbids it — so NULL's index-distinctness cannot open a
-- duplicate here.
CREATE UNIQUE INDEX exported_identifier_live_processed_sample
    ON qiita.exported_identifier (alignment_idx, prep_sample_idx)
    WHERE NOT retired;

-- The public handle is the lookup key for anyone resolving a citation.
CREATE UNIQUE INDEX exported_identifier_export_id_unique
    ON qiita.exported_identifier (export_id);


-- ---------------------------------------------------------------------------
-- retire_detached_exported_identifier
-- ---------------------------------------------------------------------------
-- NOT a convenience: this trigger is what makes `ON DELETE SET NULL` legal.
-- Without it the FK action would null alignment_idx on a row whose `retired` is
-- still false, `exported_identifier_one_processing` would reject the UPDATE, and
-- the alignment purge would fail with a check violation instead of succeeding.
-- BEFORE UPDATE so the retirement lands in the same statement as the detach.
--
-- The reason text keeps the alignment_idx that was severed, because the column
-- no longer can. Note a purge is not always permanent: mint_alignment_definition
-- deduplicates on params_hash, so re-aligning the same config mints the SAME
-- alignment_idx back. The retired row stays retired and a fresh export_id is
-- minted beside it — the bytes were recomputed, so the honest answer is a new
-- identifier rather than silently re-pointing an old one.
CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_identifier()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.alignment_idx IS NULL AND OLD.alignment_idx IS NOT NULL AND NOT NEW.retired THEN
        NEW.retired := true;
        NEW.retired_at := now();
        NEW.retire_reason := format(
            'alignment_definition %s was purged; the processing this identifier named no longer exists',
            OLD.alignment_idx
        );
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION qiita.retire_detached_exported_identifier() IS
    'Retires an exported_identifier whose processing FK was just nulled by an '
    'ON DELETE SET NULL. Required for that FK action to satisfy the '
    'one_processing CHECK — without it an alignment purge fails.';

CREATE TRIGGER exported_identifier_retire_on_detach
    BEFORE UPDATE ON qiita.exported_identifier
    FOR EACH ROW EXECUTE FUNCTION qiita.retire_detached_exported_identifier();


-- migrate:down

DROP TRIGGER IF EXISTS exported_identifier_retire_on_detach ON qiita.exported_identifier;
DROP FUNCTION IF EXISTS qiita.retire_detached_exported_identifier();
DROP TABLE IF EXISTS qiita.exported_identifier;
