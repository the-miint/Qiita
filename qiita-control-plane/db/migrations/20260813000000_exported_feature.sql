-- migrate:up

-- =============================================================================
-- EXPORTED FEATURE (the public handle for one published feature-axis entity)
-- =============================================================================
-- The sibling of qiita.exported_identifier, for the other axis of a published
-- table: that one names the COLUMNS (processed samples), this one names the ROWS.
-- A feature table, its taxonomy sidecar and its sheared tree must label a row the
-- same way or they cannot be used together, so there is exactly one authority for
-- what that label is, and it is this table.
--
-- Unlike the sample axis, an accession usually EXISTS here, and the rule in
-- CLAUDE.md prefers a real accession over a handle we mint. So this is a hybrid:
--
--   * a genome carries qiita.genome.source_id, globally unique per source;
--   * a feature carries qiita.reference_membership.accession — the FASTA-header
--     read_id THAT reference used to name it, which is why the feature kind is
--     keyed on (reference_idx, feature_idx) and not on feature_idx alone. The same
--     bytes are one content-hashed feature_idx and can be named differently in two
--     references;
--   * anything with no accession, and anything whose accession is already
--     published by a DIFFERENT entity, falls back to a minted 'QF<idx>'.
--
-- THE FALLBACK IS NOT AN ERROR, and it cannot live in the generated column below:
-- a generated expression sees only its own row, and "is this string already
-- published" is a question about every other row. So the UNIQUE index on
-- export_feature_id states the invariant and the mint
-- (repositories/exported_feature.py) resolves it in two set-based passes — offer
-- every entity its accession, let ON CONFLICT DO NOTHING drop the ones a live row
-- already published, then re-offer exactly those with accession_published = false.
-- Nothing raises. Two concurrent mints of one accession resolve deterministically
-- instead of racing, and a collision costs a label rather than a request.
--
-- Why the accession is SNAPSHOTTED here instead of joined out of genome /
-- reference_membership at read time: a published identifier must not change under
-- a later edit to its source row, and — the load-bearing half — the uniqueness of
-- the published namespace has to be a database constraint. A client cannot assert
-- it: it can see the emitted set of ONE artifact, never the accession some other
-- caller published last week, and never a minted 'QF7' that a collaborator's
-- source_id happens to spell.
--
-- FORWARD PLAN, same discipline as qiita.exported_identifier: a further entity
-- kind arrives as ADD COLUMN plus its own FK, and FIVE things below must be updated
-- with it — the exported_feature_one_kind CHECK (written with num_nonnulls so that
-- is a one-token edit), the entity_kind CHECK and the kind-agrees CHECK, a partial
-- unique index of its own (the existing ones cannot cover a kind whose columns are
-- NULL, because a unique index treats every NULL as distinct), the detach trigger's
-- column list, and — the one that takes a judgement rather than an edit — which side
-- of the published-namespace predicate the new kind belongs on.

CREATE TABLE qiita.exported_feature (
    idx                 BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- The genome kind: one row per genome, reference-independent, because
    -- source_id is a property of the genome itself.
    --
    -- ON DELETE SET NULL, like qiita.exported_identifier.alignment_idx and for the
    -- same reason: a published identifier OUTLIVES what it named. The reference
    -- delete path hard-DELETEs qiita.genome and qiita.feature rows, so RESTRICT
    -- would make publishing block reference deletion forever while CASCADE would
    -- delete a published identifier. Detaching is the only option that keeps both;
    -- the trigger below is what makes the detach legal.
    genome_idx          BIGINT REFERENCES qiita.genome(genome_idx) ON DELETE SET NULL,

    -- The feature kind, as a pair. Both columns or neither on a live row.
    reference_idx       BIGINT REFERENCES qiita.reference(reference_idx) ON DELETE SET NULL,
    feature_idx         BIGINT REFERENCES qiita.feature(feature_idx) ON DELETE SET NULL,

    -- Which kind this row is, spelled out rather than inferred from the columns
    -- above, because the detach NULLs the very column that would answer — and the
    -- published-namespace index below has to keep telling the kinds apart AFTER
    -- retirement. That index is the only reader; see it for why it cares. Nothing
    -- updates this column, and on a live row it must agree with the id columns.
    entity_kind         TEXT NOT NULL,

    -- The candidate accession, recorded whether or not it won. Keeping it after a
    -- collision is what lets anyone answer "why does my table say QF77 instead of
    -- GCF_000006605" — collapsing this into a nullable "published accession" would
    -- make a collided row indistinguishable from one that never had an accession.
    accession           TEXT,

    -- Did the accession win. Not derivable from `accession IS NOT NULL` for the
    -- reason directly above.
    accession_published BOOLEAN NOT NULL DEFAULT false,

    -- The published handle. GENERATED, not written: Postgres refuses an INSERT
    -- that supplies a value, so it cannot be forged by a caller and cannot be
    -- edited after publication. There is deliberately no code path that composes
    -- this string.
    export_feature_id   VARCHAR GENERATED ALWAYS AS (
        CASE WHEN accession_published THEN accession ELSE 'QF' || idx END
    ) STORED,

    created_by_idx      BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Retirement, mirroring qiita.exported_identifier column for column, including
    -- the nullable retired_by_idx (the common retirement here has no human author
    -- — the trigger fires from inside an FK action) and the required retire_reason.
    retired             BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx      BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at          TIMESTAMPTZ,
    retire_reason       TEXT,

    -- Exactly one kind on a live row. A retired row is exempt because a detached
    -- row has lost its referent, which is the whole reason retirement exists here.
    CONSTRAINT exported_feature_one_kind
        CHECK (retired OR num_nonnulls(genome_idx, feature_idx) = 1),

    -- TEXT + CHECK rather than a Postgres ENUM, deliberately: this never crosses
    -- the wire, so there is no Python twin to keep in parity with.
    CONSTRAINT exported_feature_entity_kind_known
        CHECK (entity_kind IN ('genome', 'feature')),

    -- ...and it names the kind the columns actually hold, so it cannot be set to
    -- the wrong one at insert time and quietly change how the namespace index
    -- treats the row. Exempt once retired, when the columns are gone and this is
    -- the only remaining witness.
    CONSTRAINT exported_feature_kind_agrees_with_columns
        CHECK (retired OR (entity_kind = 'genome') = (genome_idx IS NOT NULL)),

    -- A feature identifier names the membership that accessioned it, so the pair
    -- travels together. Exempt on a retired row for the same reason.
    CONSTRAINT exported_feature_reference_pairs_with_feature
        CHECK (retired OR (feature_idx IS NULL) = (reference_idx IS NULL)),

    -- Publishing an accession requires having one. Without this, a true flag over a
    -- NULL accession would generate a NULL export_feature_id — an unnamed row in a
    -- published artifact.
    CONSTRAINT exported_feature_published_accession_exists
        CHECK (NOT accession_published OR accession IS NOT NULL),

    CONSTRAINT exported_feature_retirement_consistent CHECK (
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

COMMENT ON TABLE qiita.exported_feature IS
    'Public handle (export_feature_id) for one published feature-axis entity — a '
    'genome, or a (reference, feature) membership. Hybrid by design: the real '
    'accession wins wherever one exists and is unique, and a minted ''QF<idx>'' is '
    'the fallback for an entity with no accession or one whose accession another '
    'entity already published. Never deleted, only retired, so a published citation '
    'keeps resolving. The migration that creates this table is the single copy of '
    'the reasoning — why the accession is snapshotted, why the fallback cannot be a '
    'generated expression, why the namespace index treats a retired genome and a '
    'retired feature differently, and what a new entity kind has to update.';

COMMENT ON COLUMN qiita.exported_feature.export_feature_id IS
    'The published identifier: the accession when accession_published, else '
    '''QF'' || idx. GENERATED ALWAYS, so it is unforgeable by a caller and '
    'immutable after publication.';

COMMENT ON COLUMN qiita.exported_feature.entity_kind IS
    'Which kind of entity this row names. Redundant with the id columns while the '
    'row is live, and the only witness once a detach has NULLed them — which is '
    'what the published-namespace index needs it for.';

COMMENT ON COLUMN qiita.exported_feature.accession IS
    'The candidate accession (genome.source_id, or reference_membership.accession '
    'for the feature kind), snapshotted at mint time and kept even when it lost a '
    'collision — which is the only record of WHY a row publishes a minted handle.';

-- One LIVE identifier per entity, so the same entity always resolves to the same
-- export_feature_id and the mint is an idempotent upsert on these indexes.
--
-- ONE INDEX PER KIND, deliberately: a live genome-kind row has feature_idx NULL
-- and a unique index treats every NULL as distinct, so a single index over all the
-- kind columns would let two live rows for one genome coexist. Widening it with
-- NULLS NOT DISTINCT would instead make two DIFFERENT feature-kind rows collide.
--
-- PARTIAL on `NOT retired` so a retired tuple can be re-minted with a fresh
-- identifier rather than colliding with its own history forever.
CREATE UNIQUE INDEX exported_feature_live_genome
    ON qiita.exported_feature (genome_idx)
    WHERE NOT retired AND genome_idx IS NOT NULL;

CREATE UNIQUE INDEX exported_feature_live_reference_feature
    ON qiita.exported_feature (reference_idx, feature_idx)
    WHERE NOT retired AND feature_idx IS NOT NULL;

-- The published namespace, and the index the mint's fallback exists to satisfy: no
-- two rows in it may publish the same string, whichever half of the hybrid produced
-- it. 'QF' || idx cannot recur under any predicate — idx is unique by construction —
-- so everything below is about accessions.
--
-- THE PREDICATE IS ASYMMETRIC, and that is the point: a retired GENOME releases its
-- string, a retired FEATURE keeps it reserved forever. What differs is how much the
-- accession promises.
--
--   * genome.source_id is the SOURCE's name for an organism: GCF_000006605 means the
--     same thing whoever loads it. The common retirement here is AUTOMATIC — deleting
--     a reference detaches every identifier it accessioned — and re-loading that
--     reference re-creates the genome under a fresh genome_idx. Holding the string
--     would hand the re-published genome a minted 'QF<n>' purely because a reference
--     was once deleted, which is the outcome the hybrid exists to avoid.
--
--   * reference_membership.accession is a FASTA header from ONE load, and 'contig_5'
--     names nothing outside it. Releasing it would let an unrelated reference's
--     unrelated sequence publish 'contig_5' next year — one public label having named
--     two different sequences, which is precisely what this table promises cannot
--     happen. The detach NULLs feature_idx, so the row can no longer prove which
--     sequence it named; entity_kind is what survives to make the distinction.
--
-- The cost of reserving is real and accepted: re-loading a reference gives its
-- features minted handles rather than their old accessions, even where the header was
-- a globally meaningful accession all along. A label that is merely ugly is a much
-- smaller failure than a label that resolves to the wrong sequence.
CREATE UNIQUE INDEX exported_feature_export_feature_id_unique
    ON qiita.exported_feature (export_feature_id)
    WHERE NOT retired OR entity_kind = 'feature';


-- ---------------------------------------------------------------------------
-- retire_detached_exported_feature
-- ---------------------------------------------------------------------------
-- NOT a convenience: this trigger is what makes the `ON DELETE SET NULL` FKs
-- above legal. Without it, deleting a reference would null feature_idx on a row
-- whose `retired` is still false, exported_feature_one_kind would reject the
-- UPDATE, and the reference delete would fail with a check violation instead of
-- succeeding. BEFORE UPDATE so the retirement lands in the same statement as the
-- detach.
--
-- The reason text keeps the identifier that was severed, because the column no
-- longer can. It fires on any of the three entity columns going non-null -> null;
-- the FK actions are the only paths that produce one today.
CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_feature()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    severed TEXT;
BEGIN
    IF NEW.retired THEN
        RETURN NEW;
    END IF;
    IF NEW.genome_idx IS NULL AND OLD.genome_idx IS NOT NULL THEN
        severed := format('genome %s', OLD.genome_idx);
    ELSIF NEW.feature_idx IS NULL AND OLD.feature_idx IS NOT NULL THEN
        severed := format('feature %s of reference %s', OLD.feature_idx, OLD.reference_idx);
    ELSIF NEW.reference_idx IS NULL AND OLD.reference_idx IS NOT NULL THEN
        severed := format('reference %s', OLD.reference_idx);
    ELSE
        RETURN NEW;
    END IF;
    NEW.retired := true;
    NEW.retired_at := now();
    NEW.retire_reason := format(
        '%s was deleted; the entity this identifier named no longer exists', severed
    );
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION qiita.retire_detached_exported_feature() IS
    'Retires an exported_feature whose entity FK was just nulled by an ON DELETE '
    'SET NULL. Required for those FK actions to satisfy the one_kind and '
    'reference-pairs CHECKs — without it, deleting a reference or a genome fails.';

CREATE TRIGGER exported_feature_retire_on_detach
    BEFORE UPDATE ON qiita.exported_feature
    FOR EACH ROW EXECUTE FUNCTION qiita.retire_detached_exported_feature();


-- migrate:down

DROP TRIGGER IF EXISTS exported_feature_retire_on_detach ON qiita.exported_feature;
DROP FUNCTION IF EXISTS qiita.retire_detached_exported_feature();
DROP TABLE IF EXISTS qiita.exported_feature;
