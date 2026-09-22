-- migrate:up
-- =============================================================================
-- MASK DEFINITION — adapter hash scheme (expand phase)
-- =============================================================================
-- `resolved_qc.adapter_set_hash` inside qiita.mask_definition.params has two
-- possible derivations:
--
--   * the ORIGINAL form — SHA-256 of the serialized adapter Parquet's bytes. The
--     pyarrow writer stamps its own version into the file footer
--     (`created_by = "parquet-cpp-arrow version X.Y.Z"`), so the digest of the
--     same adapter sequences changes on every pyarrow bump: 19.0.0 ->
--     a1677d2f..., 21.0.0 -> 29a6a873..., 23.0.1 -> 53117c74... for one fixed
--     two-row table. Mask identity therefore tracked the writer version, and a
--     re-derivation under a different pyarrow resolved to a different mask.
--
--   * the CURRENT form ('sequence_hash_v1') — SHA-256 over the reference's
--     qiita.feature.sequence_hash values, sorted, read from Postgres. A function
--     of the adapter sequences alone.
--
-- This column records which derivation a row's stored adapter_set_hash came
-- from. It is deliberately OUTSIDE params: params is the hashed blob, so a key
-- added inside `resolved_qc` would change params_hash for EVERY mask fleet-wide,
-- including the lima/syndna and maskless ones that carry no adapter set at all.
--
-- NULL covers three cases, told apart by whether params carries an
-- adapter_set_hash at all:
--
--   * adapter_set_hash IS NULL — the config uses no adapter set (PacBio, or a
--     deploy with no configured reference). Both derivations agree on "no adapter
--     set", so there is no derivation to record and the row is permanently NULL.
--   * adapter_set_hash IS NOT NULL, row predates this column — the legacy
--     byte-derived form.
--   * adapter_set_hash IS NOT NULL, row minted during the deploy window —
--     migrations are applied out-of-band BEFORE the service restart, so old code
--     serves requests against the new column for the length of the deploy. Also
--     legacy-form. A column DEFAULT would stamp these as converted when they are
--     not, which is why there is none.
--
-- So the contract phase's gate is "no row with a non-NULL adapter_set_hash and a
-- NULL scheme", not a plain NOT NULL — the adapter-less rows never gain one.
--
-- Contract phase (a later migration + PR): once no adapter-bearing row is left
-- unstamped, the byte derivation and its fallback come out of the mint path, and
-- with them the block planner's data-plane DoGet, its staging directory, and the
-- AdapterMaterializationUnavailable 503.

ALTER TABLE qiita.mask_definition
    ADD COLUMN adapter_hash_scheme TEXT,
    -- Plain TEXT + CHECK, not a Postgres ENUM, mirroring reference.status /
    -- reference.kind: out of enum-parity scope (see CLAUDE.md, "Enum parity").
    ADD CONSTRAINT mask_definition_adapter_hash_scheme_known
        CHECK (adapter_hash_scheme IS NULL OR adapter_hash_scheme IN ('sequence_hash_v1'));

COMMENT ON COLUMN qiita.mask_definition.adapter_hash_scheme IS
    'Which derivation produced params.resolved_qc.adapter_set_hash. '
    '''sequence_hash_v1'' = SHA-256 over the reference''s sorted '
    'qiita.feature.sequence_hash values. NULL means either the config carries no '
    'adapter set (params.resolved_qc.adapter_set_hash is itself null — permanent, '
    'nothing to record) or the row still holds the original '
    'SHA-256-of-serialized-Parquet-bytes form, which tracked the pyarrow writer '
    'version. Outside params because params is the hashed blob.';

COMMENT ON CONSTRAINT mask_definition_adapter_hash_scheme_known ON qiita.mask_definition IS
    'Bounds the scheme to derivations the code can actually reproduce. Adding a '
    'value means a new migration plus the matching constant in '
    'qiita_control_plane.repositories.mask_definition.';


-- ---------------------------------------------------------------------------
-- mint_mask_definition — dual-hash lookup + in-place re-key
-- ---------------------------------------------------------------------------
-- Two parameters are added, both defaulted, so the pre-existing 5-argument call
-- still resolves during the deploy window. Postgres overloads on signature, so
-- adding defaulted parameters creates a SECOND function rather than replacing
-- the first, and a 5-argument call against both would fail "function is not
-- unique" — hence the DROP.
--
-- p_legacy_params_hash: the same config hashed with the legacy adapter_set_hash.
-- When the current hash misses and the legacy hash hits, that row IS this
-- config; it is re-keyed in place rather than minted afresh. mask_idx does not
-- move, so qiita.mask_sample, qiita.work_ticket.mask_idx, and the data plane's
-- read_mask rows keep pointing at the same mask and nothing re-masks.
--
-- The re-key is the lazy half of the migration; it only converts a config
-- something mints again. `qiita-admin backfill mask-adapter-hash` converts the
-- rest, which is what makes the contract phase reachable.
DROP FUNCTION qiita.mint_mask_definition(bytea, text, text, jsonb, bigint);

CREATE FUNCTION qiita.mint_mask_definition(
    p_params_hash bytea,
    p_filter_workflow text,
    p_filter_version text,
    p_params jsonb,
    p_principal_idx bigint,
    p_legacy_params_hash bytea DEFAULT NULL,
    p_adapter_hash_scheme text DEFAULT NULL
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

    IF p_legacy_params_hash IS NOT NULL AND octet_length(p_legacy_params_hash) <> 32 THEN
        RAISE EXCEPTION 'legacy_params_hash must be 32 bytes (SHA-256), got %',
            octet_length(p_legacy_params_hash)
            USING ERRCODE = '22023';
    END IF;

    LOOP
        -- Fast path: the config already exists under the current hash.
        SELECT * INTO v_row
            FROM qiita.mask_definition
            WHERE params_hash = p_params_hash;
        IF FOUND THEN
            RETURN v_row;
        END IF;

        -- Legacy hit: the same config, keyed on the byte-derived adapter hash.
        -- Re-key in place. The UPDATE is guarded on the legacy hash still being
        -- present so two concurrent minters cannot both convert it; the loser
        -- finds nothing, loops, and takes the fast path above on the winner's
        -- row.
        --
        -- The unique_violation arm covers a different interleaving: a session
        -- inserting p_params_hash between this iteration's SELECT and this
        -- UPDATE. Without it the UPDATE raises 23505 to the caller, failing a
        -- mint whose whole contract is to resolve idempotently. Swallowing it
        -- loops back to the fast-path SELECT, which now finds that row. The
        -- BEGIN/EXCEPTION opens a subtransaction per iteration; it is entered
        -- only on the legacy path, which the contract phase removes.
        IF p_legacy_params_hash IS NOT NULL THEN
            BEGIN
                UPDATE qiita.mask_definition
                    SET params_hash = p_params_hash,
                        params = p_params,
                        adapter_hash_scheme = p_adapter_hash_scheme
                    WHERE params_hash = p_legacy_params_hash
                    RETURNING * INTO v_row;
                IF FOUND THEN
                    RETURN v_row;
                END IF;
            EXCEPTION WHEN unique_violation THEN
                CONTINUE;
            END;
        END IF;

        -- Not present under either hash: try to insert. A concurrent inserter
        -- may win the race, in which case ON CONFLICT makes this a no-op and we
        -- loop to re-select.
        INSERT INTO qiita.mask_definition
            (params_hash, filter_workflow, filter_version, params, created_by_idx,
             adapter_hash_scheme)
            VALUES (p_params_hash, p_filter_workflow, p_filter_version,
                    p_params, p_principal_idx, p_adapter_hash_scheme)
            ON CONFLICT (params_hash) DO NOTHING
            RETURNING * INTO v_row;
        IF FOUND THEN
            RETURN v_row;
        END IF;
        -- Lost the race; loop back and SELECT the winner's row.
    END LOOP;
END;
$$;

COMMENT ON FUNCTION qiita.mint_mask_definition(bytea, text, text, jsonb, bigint, bytea, text) IS
    'Idempotent mint of a qiita.mask_definition row: returns the existing row '
    'when p_params_hash already exists; otherwise, when p_legacy_params_hash is '
    'supplied and matches, re-keys that row onto p_params_hash in place '
    '(mask_idx unchanged); otherwise inserts. Raises SQLSTATE 22023 when either '
    'hash is not 32 bytes; propagates ForeignKeyViolation (unknown '
    'p_principal_idx) from the INSERT.';


-- migrate:down
--
-- Schema-only. Rows the mint or the backfill already re-keyed keep their
-- sequence-derived params_hash: the old byte digest is not recomputable without
-- the adapter Parquet, so down cannot restore it. A CP rolled back past this
-- migration re-derives the byte identity, misses those rows, and mints duplicate
-- masks.

DROP FUNCTION qiita.mint_mask_definition(bytea, text, text, jsonb, bigint, bytea, text);

CREATE FUNCTION qiita.mint_mask_definition(
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
        SELECT * INTO v_row
            FROM qiita.mask_definition
            WHERE params_hash = p_params_hash;
        IF FOUND THEN
            RETURN v_row;
        END IF;

        INSERT INTO qiita.mask_definition
            (params_hash, filter_workflow, filter_version, params, created_by_idx)
            VALUES (p_params_hash, p_filter_workflow, p_filter_version,
                    p_params, p_principal_idx)
            ON CONFLICT (params_hash) DO NOTHING
            RETURNING * INTO v_row;
        IF FOUND THEN
            RETURN v_row;
        END IF;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION qiita.mint_mask_definition(bytea, text, text, jsonb, bigint) IS
    'Idempotent mint of a qiita.mask_definition row: returns the existing row '
    'when p_params_hash already exists, otherwise inserts and returns the new '
    'row. Raises SQLSTATE 22023 when p_params_hash is not 32 bytes; propagates '
    'ForeignKeyViolation (unknown p_principal_idx) from the INSERT.';

ALTER TABLE qiita.mask_definition
    DROP CONSTRAINT mask_definition_adapter_hash_scheme_known,
    DROP COLUMN adapter_hash_scheme;
