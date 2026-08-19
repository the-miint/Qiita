-- migrate:up
-- =============================================================================
-- ALIGNMENT DEFINITION
-- =============================================================================
-- An alignment's identity is its CONFIG: which sharded reference it aligns
-- against, which sharded aligner, which host-depletion mask the input reads
-- carry, and the reference's current shard-set. The data plane's `alignment`
-- table is keyed by an alignment_idx the control plane mints here, deduplicated
-- on a SHA-256 of the canonical config JSON so the same config always resolves
-- to the same alignment_idx fleet-wide (idempotent mint). This is the exact
-- params-hash dedup discipline qiita.mask_definition uses — its Python and
-- Postgres twins are the model for this table.
--
-- The sorted DISTINCT reference_membership.shard_id set is part of the params
-- hash, so two configs differing only in which shards exist resolve to different
-- alignment_idx. ACCURACY / SCOPE: growing a reference is NOT supported today, so
-- this is deliberately NOT a working "growth foundation" yet — and two properties
-- would have to change first. (1) The shard COUNT is fixed at _SHARD_COUNT (1000),
-- so any reference with >= 1000 genome-bearing features has shard set [0..999]
-- regardless of size; the shard-set alone does not distinguish a grown reference.
-- (2) Shard assignment is NOT append-only: plan_shards is clear-first and
-- re-plan-safe (tile_by_lineage re-cuts the whole tape and write_shard_assignment
-- NULLs+re-stamps every membership row and clears the prior per-shard index rows),
-- so a re-plan can MOVE features across shards. Real growth support is deferred;
-- when it lands, the identity must key on something generation-stable (a plan
-- generation or the actual routed feature set), not this fixed-count shard-set.
--
-- alignment is keyed by alignment_idx, NOT by the processing_idx /
-- processed_prep_sample_idx hierarchy — that formal hierarchy is deferred; a
-- later milestone can fold this definition into it (canonical_params_hash is
-- already shared-ready). Like qiita.mask_definition there is NO advisory lock
-- and NO contiguous-range logic: alignment_idx is a plain GENERATED-ALWAYS
-- identity and the dedup is an ordinary ON CONFLICT (params_hash) upsert.

CREATE TABLE qiita.alignment_definition (
    alignment_idx   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- SHA-256 of the canonical config JSON (sorted keys, no whitespace),
    -- computed control-plane-side and passed to qiita.mint_alignment_definition.
    -- The dedup key: same config -> same hash -> same alignment_idx.
    params_hash     BYTEA   NOT NULL UNIQUE,

    -- Full config blob: reference_idx, aligner, mask_idx, sorted shard_ids.
    -- Kept alongside the hash so an alignment_idx is self-describing without
    -- recomputing the hash.
    params          JSONB   NOT NULL,

    created_by_idx  BIGINT  NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Defence in depth: the SHA-256 digest is fixed-width 32 bytes. A hand-INSERT
    -- or a future migration that bypasses mint_alignment_definition still can't
    -- store a truncated/oversized hash.
    CONSTRAINT alignment_definition_params_hash_len CHECK (octet_length(params_hash) = 32)
);

COMMENT ON TABLE qiita.alignment_definition IS
    'CP-minted sharded-alignment config identity. alignment_idx tags the data '
    'plane''s alignment rows; deduplicated on params_hash (SHA-256 of the '
    'canonical config JSON) so the same reference + aligner + mask + shard-set '
    'resolve to the same alignment_idx fleet-wide. The sorted shard-set is part of '
    'the hash (growth is not yet supported — see the migration comment for why this '
    'is not yet a growth foundation). Mint via qiita.mint_alignment_definition '
    '(idempotent upsert on params_hash). Twin discipline mirrors '
    'qiita.mask_definition.';

COMMENT ON COLUMN qiita.alignment_definition.params_hash IS
    'SHA-256 (32 bytes) of the canonical config JSON (sorted keys, no '
    'whitespace), computed in qiita-common / the repository layer. The UNIQUE '
    'dedup key.';


-- ---------------------------------------------------------------------------
-- mint_alignment_definition
-- ---------------------------------------------------------------------------
-- Idempotent upsert keyed on params_hash. Returns the existing row when the
-- config has been minted before (same hash), otherwise inserts and returns the
-- new row. The hash is computed by the caller (no pgcrypto dependency) and
-- passed in; the function only enforces the dedup + returns the row. Twin of
-- qiita.mint_mask_definition — same race handling (ON CONFLICT DO NOTHING +
-- re-select loop).
--
-- Error paths surfaced to the caller:
--   - unknown p_principal_idx -> ForeignKeyViolationError (the route maps it)
CREATE OR REPLACE FUNCTION qiita.mint_alignment_definition(
    p_params_hash bytea,
    p_params jsonb,
    p_principal_idx bigint
) RETURNS qiita.alignment_definition
LANGUAGE plpgsql AS $$
DECLARE
    v_row qiita.alignment_definition;
BEGIN
    IF octet_length(p_params_hash) <> 32 THEN
        RAISE EXCEPTION 'params_hash must be 32 bytes (SHA-256), got %',
            octet_length(p_params_hash)
            USING ERRCODE = '22023';
    END IF;

    LOOP
        -- Fast path: the config already exists.
        SELECT * INTO v_row
            FROM qiita.alignment_definition
            WHERE params_hash = p_params_hash;
        IF FOUND THEN
            RETURN v_row;
        END IF;

        -- Not present: try to insert. A concurrent inserter may win the race,
        -- in which case ON CONFLICT makes this a no-op and we loop to re-select.
        INSERT INTO qiita.alignment_definition (params_hash, params, created_by_idx)
            VALUES (p_params_hash, p_params, p_principal_idx)
            ON CONFLICT (params_hash) DO NOTHING
            RETURNING * INTO v_row;
        IF FOUND THEN
            RETURN v_row;
        END IF;
        -- Lost the race; loop back and SELECT the winner's row.
    END LOOP;
END;
$$;

COMMENT ON FUNCTION qiita.mint_alignment_definition(bytea, jsonb, bigint) IS
    'Idempotent mint of a qiita.alignment_definition row: returns the existing '
    'row when p_params_hash already exists, otherwise inserts and returns the '
    'new row. Raises SQLSTATE 22023 when p_params_hash is not 32 bytes; '
    'propagates ForeignKeyViolation (unknown p_principal_idx) from the INSERT.';


-- migrate:down
DROP FUNCTION IF EXISTS qiita.mint_alignment_definition(bytea, jsonb, bigint);
DROP TABLE IF EXISTS qiita.alignment_definition;
