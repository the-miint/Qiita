-- CP-minted identity for a per-feature coverage measurement.
--
-- `coverage_idx` tags every row the data plane holds in `qiita_lake.coverage` — the
-- feature table of mean coverage depth. Same shape as `mask_definition` /
-- `alignment_definition`: a params-hash identity, deduplicated on the SHA-256 of the
-- canonical config JSON, so the same measurement config always resolves to the same
-- coverage_idx.
--
-- Deliberately NOT keyed by the processing_idx / processed_prep_sample_idx hierarchy —
-- that hierarchy is still deferred, and `alignment` made the same call. When it lands,
-- this definition folds into it (`canonical_params_hash` is already shared).
--
-- What is IN the hash, and why every one of them has to be:
--
--   reference_idx        the annotated reference being quantified. Different reference,
--                        different features.
--   aligner / preset     how the reads were placed on the parent.
--   min_identity         \  the MEASUREMENT gate. Both change WHICH reads contribute
--   min_aligned_fraction /  bases, so both change the number.
--   depth_mode           include_deletions vs exclude_deletions. Measurably moves the
--                        number (a deleted reference position inside the feature either
--                        counts as covered or does not).
--   mask_idx             which reads were measured at all.
--
-- A knob that changes the number and is NOT in the hash is the specific failure this
-- table exists to prevent: the job would compute differently while the identity stayed
-- the same, so the new rows would land under a coverage_idx whose stored params describe
-- the OLD measurement, and nothing would notice.

-- migrate:up
CREATE TABLE qiita.coverage_definition (
    coverage_idx   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- SHA-256 of the canonical config JSON (sorted keys, no whitespace), computed
    -- control-plane-side and passed to qiita.mint_coverage_definition.
    params_hash    BYTEA  NOT NULL UNIQUE,

    -- The full config, so a coverage_idx is self-describing without recomputing the hash.
    params         JSONB  NOT NULL,

    created_by_idx BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Defence in depth: a SHA-256 digest is fixed-width. A hand-INSERT that bypasses
    -- mint_coverage_definition still cannot store a truncated hash.
    CONSTRAINT coverage_definition_params_hash_len CHECK (octet_length(params_hash) = 32)
);

COMMENT ON TABLE qiita.coverage_definition IS
    'CP-minted identity of a per-feature coverage measurement. coverage_idx tags the data '
    'plane''s qiita_lake.coverage rows (mean depth per (prep_sample, feature)); '
    'deduplicated on params_hash (SHA-256 of the canonical config JSON) so the same '
    'reference + aligner + gate + depth mode + mask always resolves to one coverage_idx. '
    'Every knob that changes the NUMBER is in the hash, so changing one re-mints rather '
    'than silently reusing an idx whose params describe the old measurement.';

-- Mint-or-get, race-safe. Identical shape to qiita.mint_alignment_definition and
-- qiita.mint_mask_definition (ON CONFLICT DO NOTHING + re-select loop), so a concurrent
-- minter never gets a duplicate idx for the same config.
CREATE OR REPLACE FUNCTION qiita.mint_coverage_definition(
    p_params_hash bytea,
    p_params jsonb,
    p_principal_idx bigint
) RETURNS qiita.coverage_definition
LANGUAGE plpgsql AS $$
DECLARE
    v_row qiita.coverage_definition;
BEGIN
    IF octet_length(p_params_hash) <> 32 THEN
        RAISE EXCEPTION 'params_hash must be 32 bytes (SHA-256), got %',
            octet_length(p_params_hash)
            USING ERRCODE = '22023';
    END IF;

    LOOP
        SELECT * INTO v_row
            FROM qiita.coverage_definition
            WHERE params_hash = p_params_hash;
        IF FOUND THEN
            RETURN v_row;
        END IF;

        INSERT INTO qiita.coverage_definition (params_hash, params, created_by_idx)
            VALUES (p_params_hash, p_params, p_principal_idx)
            ON CONFLICT (params_hash) DO NOTHING
            RETURNING * INTO v_row;
        IF FOUND THEN
            RETURN v_row;
        END IF;
        -- A concurrent inserter won; loop and re-select.
    END LOOP;
END;
$$;

-- migrate:down
DROP FUNCTION IF EXISTS qiita.mint_coverage_definition(bytea, jsonb, bigint);
DROP TABLE qiita.coverage_definition;
