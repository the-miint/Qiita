-- migrate:up
-- =============================================================================
-- MASK DEFINITION
-- =============================================================================
-- A read mask's identity is its filtering CONFIG: the filter workflow + version
-- plus the host reference(s) and QC params it applies. The data plane's
-- read_mask / read_masked tables are keyed by a mask_idx that the control plane
-- mints here, deduplicated on a SHA-256 of the canonical config JSON so the same
-- config always resolves to the same mask_idx fleet-wide (idempotent mint). This
-- is the same params-hash dedup discipline as the documented processing_idx
-- hierarchy, scoped for now only to masks.
--
-- Unlike qiita.sequence_range, there is NO advisory lock and NO contiguous-range
-- logic: mask_idx is a plain GENERATED-ALWAYS identity, and the dedup is an
-- ordinary ON CONFLICT (params_hash) upsert. Concurrency is handled by the
-- UNIQUE (params_hash) constraint + the upsert's DO NOTHING / re-select.

CREATE TABLE qiita.mask_definition (
    mask_idx        BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- SHA-256 of the canonical config JSON (sorted keys, no whitespace),
    -- computed control-plane-side and passed to qiita.mint_mask_definition.
    -- The dedup key: same config -> same hash -> same mask_idx.
    params_hash     BYTEA   NOT NULL UNIQUE,

    filter_workflow TEXT    NOT NULL,
    filter_version  TEXT    NOT NULL,

    -- Full config blob: host references, QC settings, etc. Kept alongside the
    -- hash so a mask_idx is self-describing without recomputing the hash.
    params          JSONB   NOT NULL,

    created_by_idx  BIGINT  NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Defence in depth: the SHA-256 digest is fixed-width 32 bytes. A hand-INSERT
    -- or a future migration that bypasses mint_mask_definition still can't store
    -- a truncated/oversized hash.
    CONSTRAINT mask_definition_params_hash_len CHECK (octet_length(params_hash) = 32)
);

COMMENT ON TABLE qiita.mask_definition IS
    'CP-minted read-filtering config identity. mask_idx tags the data plane''s '
    'read_mask / read_masked rows; deduplicated on params_hash (SHA-256 of the '
    'canonical config JSON) so the same filter workflow + version + references + '
    'QC params resolve to the same mask_idx fleet-wide. Mint via '
    'qiita.mint_mask_definition (idempotent upsert on params_hash).';

COMMENT ON COLUMN qiita.mask_definition.params_hash IS
    'SHA-256 (32 bytes) of the canonical config JSON (sorted keys, no '
    'whitespace), computed in qiita-common / the repository layer. The UNIQUE '
    'dedup key.';


-- ---------------------------------------------------------------------------
-- mint_mask_definition
-- ---------------------------------------------------------------------------
-- Idempotent upsert keyed on params_hash. Returns the existing row when the
-- config has been minted before (same hash), otherwise inserts and returns the
-- new row. The hash is computed by the caller (no pgcrypto dependency) and
-- passed in; the function only enforces the dedup + returns the row.
--
-- Concurrency: two callers racing the same params_hash both attempt the INSERT;
-- ON CONFLICT (params_hash) DO NOTHING makes the loser's INSERT a no-op, and the
-- subsequent SELECT picks up the winner's row. The loop re-tries the
-- INSERT/SELECT pair once to close the (vanishingly small) window where the
-- conflicting row is visible to ON CONFLICT but not yet to the SELECT under a
-- weaker isolation level; under READ COMMITTED a single pass suffices, but the
-- loop costs nothing and removes the edge case entirely.
--
-- Error paths surfaced to the caller:
--   - unknown p_principal_idx -> ForeignKeyViolationError (the route maps it)
CREATE OR REPLACE FUNCTION qiita.mint_mask_definition(
    p_params_hash bytea,
    p_filter_workflow text,
    p_filter_version text,
    p_params jsonb,
    p_principal_idx bigint
) RETURNS qiita.mask_definition
LANGUAGE plpgsql AS $$
DECLARE
    v_row qiita.mask_definition;
BEGIN
    IF octet_length(p_params_hash) <> 32 THEN
        RAISE EXCEPTION 'params_hash must be 32 bytes (SHA-256), got %',
            octet_length(p_params_hash)
            USING ERRCODE = '22023';
    END IF;

    LOOP
        -- Fast path: the config already exists.
        SELECT * INTO v_row
            FROM qiita.mask_definition
            WHERE params_hash = p_params_hash;
        IF FOUND THEN
            RETURN v_row;
        END IF;

        -- Not present: try to insert. A concurrent inserter may win the race,
        -- in which case ON CONFLICT makes this a no-op and we loop to re-select.
        INSERT INTO qiita.mask_definition
            (params_hash, filter_workflow, filter_version, params, created_by_idx)
            VALUES (p_params_hash, p_filter_workflow, p_filter_version,
                    p_params, p_principal_idx)
            ON CONFLICT (params_hash) DO NOTHING
            RETURNING * INTO v_row;
        IF FOUND THEN
            RETURN v_row;
        END IF;
        -- Lost the race; loop back and SELECT the winner's row.
    END LOOP;
END;
$$;

COMMENT ON FUNCTION qiita.mint_mask_definition(bytea, text, text, jsonb, bigint) IS
    'Idempotent mint of a qiita.mask_definition row: returns the existing row '
    'when p_params_hash already exists, otherwise inserts and returns the new '
    'row. Raises SQLSTATE 22023 when p_params_hash is not 32 bytes; propagates '
    'ForeignKeyViolation (unknown p_principal_idx) from the INSERT.';


-- migrate:down
DROP FUNCTION IF EXISTS qiita.mint_mask_definition(bytea, text, text, jsonb, bigint);
DROP TABLE IF EXISTS qiita.mask_definition;
