-- migrate:up
-- =============================================================================
-- PROCESSING METHOD + PROCESSED PREP SAMPLE
-- =============================================================================
-- A processing method's identity is its config: the workflow name, version, and params.
-- The data plane's feature_counts table is keyed by a processing_idx minted here,
-- deduplicated on a SHA-256 of the canonical parameter JSON so the same config always
-- resolves to the same processing_idx fleet-wide. Mirrors the params-hash mechanism of
-- qiita.mask_definition (see qiita_common.hashing.canonical_params_hash).
--
-- processed_prep_sample_idx is the per-(processing, sample) leaf that scopes a
-- feature_counts row to one sample's run of one method.
--
-- As with mask_definition there is no advisory lock and no contiguous-range logic: both
-- idx columns are GENERATED-ALWAYS identities and dedup is an ON CONFLICT upsert.

CREATE TABLE qiita.processing_method (
    processing_idx   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- SHA-256 of the canonical parameter JSON (sorted keys, no whitespace), computed
    -- CP-side. The dedup key: the same params yield the same processing_idx.
    params_hash      BYTEA   NOT NULL UNIQUE,

    workflow_name    TEXT    NOT NULL,
    workflow_version TEXT    NOT NULL,

    -- Full parameter blob kept alongside the hash so a processing_idx is self-describing.
    params           JSONB   NOT NULL,

    created_by_idx   BIGINT  NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Defence in depth: the SHA-256 digest is fixed 32 bytes, so a hand-INSERT that
    -- bypasses mint_processing_method still can't store a truncated or oversized hash.
    CONSTRAINT processing_method_params_hash_len CHECK (octet_length(params_hash) = 32)
);

COMMENT ON TABLE qiita.processing_method IS
    'CP-minted processing-method identity. processing_idx tags the data plane''s '
    'feature_counts rows; deduplicated on params_hash (SHA-256 of the canonical '
    'parameter JSON) so the same workflow + version + params resolve to the same '
    'processing_idx fleet-wide. Mint via qiita.mint_processing_method (idempotent '
    'upsert on params_hash).';

COMMENT ON COLUMN qiita.processing_method.params_hash IS
    'SHA-256 (32 bytes) of the canonical parameter JSON (sorted keys, no '
    'whitespace), computed in qiita-common / the repository layer. The UNIQUE '
    'dedup key.';


CREATE TABLE qiita.processed_prep_sample (
    processed_prep_sample_idx BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    processing_idx  BIGINT NOT NULL
        REFERENCES qiita.processing_method(processing_idx) ON DELETE RESTRICT,
    prep_sample_idx BIGINT NOT NULL
        REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One result identity per (method, sample): a re-run resolves to the same idx.
    CONSTRAINT processed_prep_sample_unique UNIQUE (processing_idx, prep_sample_idx)
);

COMMENT ON TABLE qiita.processed_prep_sample IS
    'Per-(processing_method, prep_sample) result identity. '
    'processed_prep_sample_idx is the leaf identifier scoping a feature_counts '
    'row to one sample''s run of one method. Mint via '
    'qiita.mint_processed_prep_samples (idempotent on (processing_idx, '
    'prep_sample_idx)).';


-- ---------------------------------------------------------------------------
-- mint_processing_method
-- ---------------------------------------------------------------------------
-- Idempotent upsert keyed on params_hash: returns the existing row for a known config,
-- otherwise inserts and returns the new one. The hash is computed by the caller.
--
-- Concurrency: two callers racing the same params_hash both attempt the INSERT; ON
-- CONFLICT DO NOTHING no-ops the loser and the SELECT picks up the winner's row. The loop
-- re-tries the INSERT/SELECT pair to close the window where the conflicting row is visible
-- to ON CONFLICT but not yet to the SELECT under a weaker isolation level.
--
-- Unknown p_principal_idx propagates ForeignKeyViolation.
CREATE OR REPLACE FUNCTION qiita.mint_processing_method(
    p_params_hash bytea,
    p_workflow_name text,
    p_workflow_version text,
    p_params jsonb,
    p_principal_idx bigint
) RETURNS qiita.processing_method
LANGUAGE plpgsql AS $$
DECLARE
    v_row qiita.processing_method;
BEGIN
    IF octet_length(p_params_hash) <> 32 THEN
        RAISE EXCEPTION 'params_hash must be 32 bytes (SHA-256), got %',
            octet_length(p_params_hash)
            USING ERRCODE = '22023';
    END IF;

    LOOP
        -- fast path: the config already exists.
        SELECT * INTO v_row
            FROM qiita.processing_method
            WHERE params_hash = p_params_hash;
        IF FOUND THEN
            RETURN v_row;
        END IF;

        -- not present: try to insert. If a concurrent inserter wins, ON CONFLICT
        -- no-ops this and we loop to re-select the winner's row.
        INSERT INTO qiita.processing_method
            (params_hash, workflow_name, workflow_version, params, created_by_idx)
            VALUES (p_params_hash, p_workflow_name, p_workflow_version,
                    p_params, p_principal_idx)
            ON CONFLICT (params_hash) DO NOTHING
            RETURNING * INTO v_row;
        IF FOUND THEN
            RETURN v_row;
        END IF;
        -- lost the race; loop back and SELECT the winner's row.
    END LOOP;
END;
$$;

COMMENT ON FUNCTION qiita.mint_processing_method(bytea, text, text, jsonb, bigint) IS
    'Idempotent mint of a qiita.processing_method row: returns the existing row '
    'when p_params_hash already exists, otherwise inserts and returns the new '
    'row. Raises SQLSTATE 22023 when p_params_hash is not 32 bytes; propagates '
    'ForeignKeyViolation (unknown p_principal_idx) from the INSERT.';


-- ---------------------------------------------------------------------------
-- mint_processed_prep_samples
-- ---------------------------------------------------------------------------
-- Idempotent batch mint for a set of prep_sample_idx: inserts any missing (processing_idx,
-- prep_sample_idx) pairs then returns every row for the requested set, so a retry over the
-- same cohort returns the same idxs.
--
-- Unknown p_processing_idx or prep_sample_idx propagates ForeignKeyViolation.
CREATE OR REPLACE FUNCTION qiita.mint_processed_prep_samples(
    p_processing_idx bigint,
    p_prep_sample_idxs bigint[]
) RETURNS SETOF qiita.processed_prep_sample
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO qiita.processed_prep_sample (processing_idx, prep_sample_idx)
        SELECT p_processing_idx, unnest(p_prep_sample_idxs)
        ON CONFLICT (processing_idx, prep_sample_idx) DO NOTHING;

    RETURN QUERY
        SELECT * FROM qiita.processed_prep_sample
        WHERE processing_idx = p_processing_idx
          AND prep_sample_idx = ANY(p_prep_sample_idxs);
END;
$$;

COMMENT ON FUNCTION qiita.mint_processed_prep_samples(bigint, bigint[]) IS
    'Idempotent batch mint of qiita.processed_prep_sample rows for a cohort: '
    'inserts any missing (processing_idx, prep_sample_idx) pairs and returns '
    'every row for the requested prep_sample set. Propagates ForeignKeyViolation '
    'for an unknown processing_idx or prep_sample_idx.';


-- migrate:down
DROP FUNCTION IF EXISTS qiita.mint_processed_prep_samples(bigint, bigint[]);
DROP FUNCTION IF EXISTS qiita.mint_processing_method(bytea, text, text, jsonb, bigint);
DROP TABLE IF EXISTS qiita.processed_prep_sample;
DROP TABLE IF EXISTS qiita.processing_method;
