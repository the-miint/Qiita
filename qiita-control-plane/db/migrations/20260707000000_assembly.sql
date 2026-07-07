-- migrate:up

-- A `processing` identity for assembly (and future per-sample processing): the
-- SHA-256 of the canonical parameters (workflow + version + assembler + any
-- result-affecting knobs) minted/deduped control-plane-side, so the same
-- parameters always resolve to the same processing_idx fleet-wide. This is the
-- run discriminator (two runs of one prep_sample with different params get
-- distinct processing_idx; an identical re-run is idempotent) and the provenance
-- record (the params blob is self-describing). Mirrors qiita.mask_definition.
CREATE TABLE qiita.processing (
    processing_idx BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- SHA-256 of the canonical params JSON (sorted keys, no whitespace), computed
    -- CP-side (qiita_common.hashing.canonical_params_hash) and passed to
    -- qiita.mint_processing. The dedup key: same params -> same hash -> same id.
    params_hash    BYTEA   NOT NULL UNIQUE,

    workflow       TEXT    NOT NULL,   -- action_id, e.g. 'pacbio-processing'
    version        TEXT    NOT NULL,   -- e.g. '1.0.0'

    -- Full canonical params blob (workflow, version, assembler, ...), kept beside
    -- the hash so a processing_idx is self-describing without recomputing.
    params         JSONB   NOT NULL,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Defence in depth: the SHA-256 digest is fixed-width 32 bytes.
    CONSTRAINT processing_params_hash_len CHECK (octet_length(params_hash) = 32)
);

-- Idempotent mint, mirroring qiita.mint_mask_definition: return the existing row
-- for a params_hash, else insert and return it (concurrent-insert safe).
CREATE OR REPLACE FUNCTION qiita.mint_processing(
    p_params_hash bytea,
    p_workflow text,
    p_version text,
    p_params jsonb
) RETURNS qiita.processing
LANGUAGE plpgsql AS $$
DECLARE
    v_row qiita.processing;
BEGIN
    IF octet_length(p_params_hash) <> 32 THEN
        RAISE EXCEPTION 'params_hash must be 32 bytes (SHA-256), got %',
            octet_length(p_params_hash)
            USING ERRCODE = '22023';
    END IF;

    LOOP
        SELECT * INTO v_row FROM qiita.processing WHERE params_hash = p_params_hash;
        IF FOUND THEN
            RETURN v_row;
        END IF;

        INSERT INTO qiita.processing (params_hash, workflow, version, params)
            VALUES (p_params_hash, p_workflow, p_version, p_params)
            ON CONFLICT (params_hash) DO NOTHING
            RETURNING * INTO v_row;
        IF FOUND THEN
            RETURN v_row;
        END IF;
        -- Lost the insert race; loop back and SELECT the winner's row.
    END LOOP;
END;
$$;

COMMENT ON FUNCTION qiita.mint_processing(bytea, text, text, jsonb) IS
    'Idempotent mint of a qiita.processing row: returns the existing row when '
    'p_params_hash already exists, otherwise inserts and returns the new row. '
    'Raises SQLSTATE 22023 when p_params_hash is not 32 bytes.';

-- The association between a prep_sample's assembly RUN (processing_idx) and the
-- deduped contig features it produced — the assembly analogue of
-- qiita.reference_membership. A contig is a qiita.feature (content-hash deduped,
-- minted via the SHARED mint-features path — assembled contigs join the same
-- global feature space as reference sequences), so identical bytes collapse to
-- one feature_idx and feature_idx bridges assembly <-> reference/read data. This
-- junction records which features a (prep_sample, processing) run contains and in
-- which bin — a circular LCG genome or a refined MAG.
--
-- processing_idx in the key disambiguates runs: a re-run with different params
-- gets a fresh processing_idx, so bin_id ('bin.1', reused across samples AND
-- runs) never collides. `kind` is plain TEXT (value set 'LCG'/'MAG' today, in
-- flux — owned by the producer; a TEXT-backed Python twin needs no enum-parity).
CREATE TABLE qiita.assembly_membership (
    prep_sample_idx BIGINT NOT NULL REFERENCES qiita.prep_sample (idx) ON DELETE CASCADE,
    processing_idx  BIGINT NOT NULL REFERENCES qiita.processing (processing_idx),
    kind            TEXT   NOT NULL,
    bin_id          TEXT   NOT NULL,
    feature_idx     BIGINT NOT NULL REFERENCES qiita.feature (feature_idx),
    PRIMARY KEY (prep_sample_idx, processing_idx, kind, bin_id, feature_idx)
);

-- Feature-first lookup (which samples/runs/bins contain a given contig),
-- mirroring the reference_membership (feature_idx) index.
CREATE INDEX ON qiita.assembly_membership (feature_idx);

-- migrate:down

DROP TABLE qiita.assembly_membership;
DROP FUNCTION IF EXISTS qiita.mint_processing(bytea, text, text, jsonb);
DROP TABLE qiita.processing;
