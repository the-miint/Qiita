-- migrate:up

-- =============================================================================
-- EXPORTED PROCESSING (the public handle for one processing)
-- =============================================================================
-- The third of the export-identifier trio, and the smallest. qiita.exported_
-- identifier names a published table's COLUMNS (processed samples),
-- qiita.exported_feature names its ROWS, and this one names WHAT WAS DONE to
-- produce it — the thing a manifest has to cite for the table to be reproducible.
--
-- It exists because `alignment_idx` cannot be published. Coverage filtering makes
-- a feature table a function of the cohort rather than of its samples, so a
-- manifest is not optional documentation; and the only handle for the processing
-- was an internal `*_idx`, which is exactly what a published artifact may not
-- carry.
--
-- NO HYBRID HERE, unlike qiita.exported_feature. A processing is something we
-- performed, so no external authority has a name for it — there is no accession to
-- prefer and nothing to fall back from. The namespace is entirely minted, which is
-- why its unique index below is total rather than partial: 'QP' || idx is per-row
-- by construction and cannot recur, so a retired handle can occupy its string
-- forever at no cost.
--
-- The manifest cites this handle ALONGSIDE alignment_definition.params_hash, and
-- the two answer different questions. params_hash is content-derived, so two
-- deploys that ran the identical config agree on it — it answers "was this the same
-- processing?". This handle is per-row, so it answers "which processing, in the
-- system that published this?". Publishing `params` verbatim instead was rejected:
-- it carries reference_idx, mask_idx and shard_ids.
--
-- FORWARD PLAN, identical in shape to qiita.exported_identifier's because the two
-- extend together: another processing type arrives as
-- `ALTER TABLE ... ADD COLUMN <kind>_idx BIGINT` plus its own FK, and TWO things
-- below must be updated with it — the exported_processing_one_processing CHECK
-- (num_nonnulls, so a one-token edit) and a partial unique index of its own, since
-- the existing one cannot cover a kind whose alignment_idx is NULL (a unique index
-- treats every NULL as distinct, and NULLS NOT DISTINCT would instead make two
-- different new-kind processings collide). One index per kind.

CREATE TABLE qiita.exported_processing (
    idx                   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- The published handle. GENERATED, not written: Postgres refuses an INSERT
    -- that supplies a value, so it cannot be forged by a caller, cannot drift from
    -- idx, and cannot be edited after publication. No code path composes this string.
    export_processing_id  VARCHAR GENERATED ALWAYS AS ('QP' || idx) STORED,

    -- The processing. Exactly one such column is non-null on a live row; see the
    -- CHECK below and the FORWARD PLAN above.
    --
    -- ON DELETE SET NULL for the reason qiita.exported_identifier.alignment_idx
    -- documents at length: a published identifier outlives the thing it named, the
    -- admin alignment purge must stay possible, CASCADE would delete a published
    -- identifier and RESTRICT would block the purge forever. The trigger below is
    -- what makes the detach legal.
    alignment_idx         BIGINT REFERENCES qiita.alignment_definition(alignment_idx)
                              ON DELETE SET NULL,

    created_by_idx        BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Retirement, mirroring qiita.exported_identifier column for column, including
    -- the nullable retired_by_idx (the common retirement here has no human author —
    -- the trigger fires from inside an FK action) and the required retire_reason.
    retired               BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx        BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at            TIMESTAMPTZ,
    retire_reason         TEXT,

    -- Exactly one processing on a live row. A retired row is exempt because a
    -- detached row has lost its referent, which is the whole reason retirement
    -- exists here.
    CONSTRAINT exported_processing_one_processing
        CHECK (retired OR num_nonnulls(alignment_idx) = 1),

    CONSTRAINT exported_processing_retirement_consistent CHECK (
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

COMMENT ON TABLE qiita.exported_processing IS
    'Public handle (export_processing_id, ''QP<idx>'') for one processing — an '
    'alignment_definition today. What a published bundle''s manifest cites so the '
    'table is reproducible without carrying alignment_idx, alongside the '
    'content-derived params_hash (which says whether two processings were the same, '
    'where this says which one). Entirely minted: a processing is something we did, '
    'so no accession exists to prefer. Never deleted, only retired, so a published '
    'citation keeps resolving. Extended by ADD COLUMN per new processing type — '
    'which must update the one_processing CHECK AND add its own partial unique index.';

COMMENT ON COLUMN qiita.exported_processing.export_processing_id IS
    'The published identifier, ''QP'' || idx. GENERATED ALWAYS: unforgeable by a '
    'caller, cannot drift from idx, immutable after publication.';

-- One LIVE identifier per processing, so a bundle written twice cites the
-- processing the same way both times — the mint is an idempotent upsert on this
-- index.
--
-- PARTIAL on `NOT retired` so a retired row can be re-minted rather than colliding
-- with its own history forever. The case that needs it is a deliberate retirement
-- (published in error, an embargo) where alignment_idx stays attached; the purge
-- path nulls the column and so could never collide anyway.
CREATE UNIQUE INDEX exported_processing_live_processing
    ON qiita.exported_processing (alignment_idx)
    WHERE NOT retired;

-- The published handle is the lookup key for anyone resolving a citation. TOTAL,
-- for the reason the header gives: nothing here is reclaimable.
--
-- Honest about what it buys: with the handle generated as 'QP' || idx, uniqueness is
-- already implied by the PRIMARY KEY, so this index cannot currently be violated. It
-- is here to serve the lookup and to keep the guarantee attached to the published
-- column rather than inferred from the expression — so a later change to how the
-- handle is composed cannot quietly make two rows share one.
CREATE UNIQUE INDEX exported_processing_export_processing_id_unique
    ON qiita.exported_processing (export_processing_id);


-- ---------------------------------------------------------------------------
-- retire_detached_exported_processing
-- ---------------------------------------------------------------------------
-- The twin of qiita.retire_detached_exported_identifier, and not a convenience:
-- without it the FK action would null alignment_idx on a row whose `retired` is
-- still false, the one_processing CHECK would reject the UPDATE, and the alignment
-- purge would fail with a check violation instead of succeeding. BEFORE UPDATE so
-- the retirement lands in the same statement as the detach.
--
-- The reason text keeps the alignment_idx that was severed, because the column no
-- longer can. A purge is permanent as far as this identifier is concerned:
-- alignment_idx is an identity column and the purge hard-DELETEs the row, so
-- re-running the identical config mints a fresh alignment_idx and therefore a fresh
-- handle, while the retired one keeps naming what was purged.
CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_processing()
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

COMMENT ON FUNCTION qiita.retire_detached_exported_processing() IS
    'Retires an exported_processing whose processing FK was just nulled by an '
    'ON DELETE SET NULL. Required for that FK action to satisfy the '
    'one_processing CHECK — without it an alignment purge fails.';

CREATE TRIGGER exported_processing_retire_on_detach
    BEFORE UPDATE ON qiita.exported_processing
    FOR EACH ROW EXECUTE FUNCTION qiita.retire_detached_exported_processing();


-- migrate:down

DROP TRIGGER IF EXISTS exported_processing_retire_on_detach ON qiita.exported_processing;
DROP FUNCTION IF EXISTS qiita.retire_detached_exported_processing();
DROP TABLE IF EXISTS qiita.exported_processing;
