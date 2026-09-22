-- Mask lifecycle at TWO granularities, because the two failures they describe are
-- different failures.
--
-- A mask's params_hash covers the resolved thresholds, not the code that applied
-- them: filter_version is the workflow YAML version, not the miint build. So a
-- config whose scoring turned out to be wrong re-resolves to the SAME mask_idx on
-- the next run and masks new data with the same defect. `qiita.mask_definition`
-- had no lifecycle column to say so.
--
--   * `mask_definition.status` answers "is this CONFIGURATION void?" — a bad
--     reference, a bad threshold. It is what stops NEW bad data, by refusing the
--     mint (see qiita.mint_mask_definition below).
--
--   * `mask_sample.state = 'invalidated'` answers "is this RUN of a sound config
--     bad?" — the unit that actually succeeded or failed. One measured incident
--     had 26 prep-samples under one mask of which 7 classified wrongly, the 7
--     being exactly those whose job OOM-escalated and so ran with a larger Arrow
--     batch. Deprecating the mask would have voided 19 sound results to flag 7.
--
-- Invalidation is a `state` VALUE rather than a column beside `state` so that
-- every existing consumer refuses without being edited: the mask_sample gate
-- contract (canonical statement at repositories/block.py::fetch_mask_sample_state)
-- is that a consumer proceeds ONLY on 'completed', so a third value is refused by
-- construction. A parallel boolean would have to be read at each site, and a site
-- that forgot fails OPEN — which for this table means serving host reads.
--
-- Both value sets are TEXT + CHECK, not Postgres ENUMs, matching reference.status
-- and the existing mask_sample.state. Their Python twins (MaskDefinitionStatus,
-- MaskSampleState in qiita_common.models) are therefore out of scope for the
-- enum-parity test, per the TEXT/CHECK carve-out in CLAUDE.md.
--
-- Both markers are nullable columns cleared in place, not history rows. That is a
-- narrower record than qiita.reference_exclusion keeps (20260721000005), which
-- soft-deletes so who blocked/unblocked and why stays queryable: restoring a mask
-- or a run here discards who withdrew it. The trade is deliberate — the question
-- these answer is "may this be used NOW", asked on the hot path by the mint and by
-- every masked-read consumer, and a current-state column answers it with a column
-- read rather than a latest-row-per-key query. A withdrawal that needs to survive
-- its own reversal belongs in an audit surface, not in the gate consumers poll.
--
-- No backfill: DEFAULT 'active' covers every existing mask_definition row, and no
-- mask_sample row changes state.

-- migrate:up

ALTER TABLE qiita.mask_definition
    ADD COLUMN status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'deprecated')),
    ADD COLUMN deprecated_at TIMESTAMPTZ,
    ADD COLUMN deprecated_by_idx BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    ADD COLUMN deprecation_reason TEXT,
    -- The mask that replaces this one, when there is one. Nullable: a config can be
    -- void with nothing to point at yet. ON DELETE SET NULL rather than RESTRICT so
    -- deleting the replacement does not pin the deprecated row's whole lifecycle.
    ADD COLUMN superseded_by BIGINT REFERENCES qiita.mask_definition(mask_idx) ON DELETE SET NULL,

    -- Biconditional: the three provenance fields are set exactly when deprecated.
    -- A reason is mandatory: it is what a reader of a deprecated mask behind
    -- published data has to go on.
    ADD CONSTRAINT mask_definition_deprecation_fields CHECK (
        (status = 'deprecated')
            = (deprecated_at IS NOT NULL
               AND deprecated_by_idx IS NOT NULL
               AND deprecation_reason IS NOT NULL)
    ),
    ADD CONSTRAINT mask_definition_deprecation_reason_nonblank CHECK (
        deprecation_reason IS NULL OR btrim(deprecation_reason) <> ''
    ),
    ADD CONSTRAINT mask_definition_superseded_only_when_deprecated CHECK (
        superseded_by IS NULL OR status = 'deprecated'
    ),
    ADD CONSTRAINT mask_definition_supersede_not_self CHECK (
        superseded_by IS NULL OR superseded_by <> mask_idx
    );

COMMENT ON COLUMN qiita.mask_definition.status IS
    'Lifecycle of the filtering CONFIG: ''active'' | ''deprecated'' (TEXT + CHECK, '
    'not a Postgres ENUM). Python twin: qiita_common.models.MaskDefinitionStatus. '
    '''deprecated'' means the configuration itself is void, so qiita.mint_mask_definition '
    'refuses to return the row and no new data can be masked under it. It does NOT '
    'judge any individual run — that is mask_sample.state.';

COMMENT ON COLUMN qiita.mask_definition.superseded_by IS
    'The mask_definition that replaces this one, when a re-mint under corrected '
    'code produced a new identity. Settable only on a deprecated row.';

ALTER TABLE qiita.mask_sample
    DROP CONSTRAINT mask_sample_state_check,
    ADD CONSTRAINT mask_sample_state_check
        CHECK (state IN ('pending', 'completed', 'invalidated')),
    ADD COLUMN invalidated_at TIMESTAMPTZ,
    ADD COLUMN invalidated_by_idx BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    ADD COLUMN invalidation_reason TEXT,
    ADD CONSTRAINT mask_sample_invalidation_fields CHECK (
        (state = 'invalidated')
            = (invalidated_at IS NOT NULL
               AND invalidated_by_idx IS NOT NULL
               AND invalidation_reason IS NOT NULL)
    ),
    ADD CONSTRAINT mask_sample_invalidation_reason_nonblank CHECK (
        invalidation_reason IS NULL OR btrim(invalidation_reason) <> ''
    );

-- Supersede the applied 20260724000000 COMMENT (itself a supersession of
-- 20260701000003's), keeping its both-paths content -- reconcile and
-- finalize-mask-sample are what write the two pre-existing states -- and adding
-- the third.
COMMENT ON TABLE qiita.mask_sample IS
    'Per-(mask_idx, prep_sample) gate for read masking. Written first-class by '
    'BOTH masking paths: the block path materializes ''pending'' at plan time '
    'and flips ''completed'' at reconcile; the per-sample mask-model workflows '
    '(read-mask, fastq-to-parquet) write ''completed'' at their '
    'finalize-mask-sample terminal step. A completed run later judged '
    'untrustworthy is set ''invalidated'', recorded with who/when/why. Any '
    'consumer that must not read an absent, partial or withdrawn pass-set reads '
    'ONLY ''completed'' -- absence of a row means "not masked-complete", NEVER '
    '"pass". Stated as a contract, not a roster (an enumerated consumer list '
    'would only go stale); see fetch_mask_sample_state. Python twin: '
    'qiita_common.models.MaskSampleState.';

COMMENT ON COLUMN qiita.mask_sample.state IS
    'Gate state: ''pending'' | ''completed'' | ''invalidated'' (TEXT + CHECK, not a '
    'Postgres ENUM). ''invalidated'' is terminal for this (mask, sample) pair: the '
    'masking ran and finished, and its output is not to be consumed. Re-running '
    'under a corrected config mints a different mask_idx, hence a different row.';

-- ---------------------------------------------------------------------------
-- mint_mask_definition: refuse to hand back a deprecated config
-- ---------------------------------------------------------------------------
-- Enforced inside the function rather than at its callers (runner/_mask.py, block_planner.py, routes/read_masked.py)
-- because the function IS the mint: a caller that reached the dedup and got a row
-- back has, by construction, not been told the config is void.
--
-- Restated whole because CREATE OR REPLACE FUNCTION has no partial form. The body
-- is 20260804000000's plus two deprecation checks and, on the legacy path, a
-- SELECT that reads the row those checks need before the re-key. The signature is
-- that migration's 7-argument one, so this REPLACES in place rather than adding an
-- overload (test_mint_mask_definition_has_exactly_one_overload pins that).
CREATE OR REPLACE FUNCTION qiita.mint_mask_definition(
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
            -- A deprecated config is void: refuse rather than hand it back, so no
            -- new data is masked under it. Reached only here and on the legacy
            -- re-key below — a freshly INSERTed row is 'active' by default.
            IF v_row.status = 'deprecated' THEN
                RAISE EXCEPTION
                    'mask_definition % is deprecated and cannot be minted against: %',
                    v_row.mask_idx, v_row.deprecation_reason
                    USING ERRCODE = '23514';
            END IF;
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
        --
        -- The deprecation check reads the row BEFORE the re-key: raising after it
        -- would rely on the abort to undo the UPDATE, and the enclosing
        -- EXCEPTION block makes that reasoning harder than it needs to be.
        IF p_legacy_params_hash IS NOT NULL THEN
            SELECT * INTO v_row
                FROM qiita.mask_definition
                WHERE params_hash = p_legacy_params_hash;
            IF FOUND AND v_row.status = 'deprecated' THEN
                RAISE EXCEPTION
                    'mask_definition % is deprecated and cannot be minted against: %',
                    v_row.mask_idx, v_row.deprecation_reason
                    USING ERRCODE = '23514';
            END IF;
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
    'hash is not 32 bytes; raises SQLSTATE 23514 when the matched row is '
    'status=''deprecated'' (a void config must not mask new data); propagates '
    'ForeignKeyViolation (unknown p_principal_idx) from the INSERT.';

-- migrate:down

-- Restore the pre-lifecycle function FIRST: plpgsql resolves column references at
-- run time, so leaving the status-checking body behind after the column is dropped
-- would break every mint rather than fail here. Logic identical to
-- 20260804000000's; its inline comments are not carried, so read that migration
-- for the unique_violation/CONTINUE interleaving they explain.
CREATE OR REPLACE FUNCTION qiita.mint_mask_definition(
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
        SELECT * INTO v_row
            FROM qiita.mask_definition
            WHERE params_hash = p_params_hash;
        IF FOUND THEN
            RETURN v_row;
        END IF;

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
    END LOOP;
END;
$$;

-- Order matters: the biconditional CHECKs are dropped BEFORE the state/status
-- rollback, because a row reverted to 'active'/'completed' still carries its
-- provenance columns until they are dropped, which is exactly what those CHECKs
-- forbid.
ALTER TABLE qiita.mask_definition
    DROP CONSTRAINT IF EXISTS mask_definition_supersede_not_self,
    DROP CONSTRAINT IF EXISTS mask_definition_superseded_only_when_deprecated,
    DROP CONSTRAINT IF EXISTS mask_definition_deprecation_reason_nonblank,
    DROP CONSTRAINT IF EXISTS mask_definition_deprecation_fields;

ALTER TABLE qiita.mask_sample
    DROP CONSTRAINT IF EXISTS mask_sample_invalidation_reason_nonblank,
    DROP CONSTRAINT IF EXISTS mask_sample_invalidation_fields;

-- A row this migration marked goes back to what it was before there was anywhere
-- to say otherwise. The judgement is lost, not translated: nothing downstream of
-- this migration can express it.
UPDATE qiita.mask_sample SET state = 'completed' WHERE state = 'invalidated';
UPDATE qiita.mask_definition SET status = 'active' WHERE status = 'deprecated';

ALTER TABLE qiita.mask_sample
    DROP COLUMN IF EXISTS invalidation_reason,
    DROP COLUMN IF EXISTS invalidated_by_idx,
    DROP COLUMN IF EXISTS invalidated_at,
    DROP CONSTRAINT IF EXISTS mask_sample_state_check,
    ADD CONSTRAINT mask_sample_state_check CHECK (state IN ('pending', 'completed'));

ALTER TABLE qiita.mask_definition
    DROP COLUMN IF EXISTS superseded_by,
    DROP COLUMN IF EXISTS deprecation_reason,
    DROP COLUMN IF EXISTS deprecated_by_idx,
    DROP COLUMN IF EXISTS deprecated_at,
    DROP COLUMN IF EXISTS status;

-- Restore the comments the up-block superseded. A COMMENT outlives the column
-- it describes, so leaving these attached would document a lifecycle the
-- reverted schema cannot express -- the function comment asserting an ERRCODE
-- the reverted body never raises, the table and column comments naming a state
-- the re-tightened CHECK forbids. The function and table comments go back to
-- their predecessors' text (20260804000000, 20260724000000); the column comment
-- is new here, so it clears.
COMMENT ON FUNCTION qiita.mint_mask_definition(bytea, text, text, jsonb, bigint, bytea, text) IS
    'Idempotent mint of a qiita.mask_definition row: returns the existing row '
    'when p_params_hash already exists; otherwise, when p_legacy_params_hash is '
    'supplied and matches, re-keys that row onto p_params_hash in place '
    '(mask_idx unchanged); otherwise inserts. Raises SQLSTATE 22023 when either '
    'hash is not 32 bytes; propagates ForeignKeyViolation (unknown '
    'p_principal_idx) from the INSERT.';

COMMENT ON TABLE qiita.mask_sample IS
    'Per-(mask_idx, prep_sample) completion gate for read masking. Written '
    'first-class by BOTH masking paths: the block path materializes ''pending'' at '
    'plan time and flips ''completed'' at reconcile; the per-sample mask-model '
    'workflows (read-mask, fastq-to-parquet) write ''completed'' at their '
    'finalize-mask-sample terminal step. Any consumer that must not read an absent '
    'or partial pass-set reads ONLY ''completed'' -- absence of a row means '
    '"not masked-complete", NEVER "pass". Stated as a contract, not a roster (an '
    'enumerated consumer list would only go stale); see fetch_mask_sample_state.';

COMMENT ON COLUMN qiita.mask_sample.state IS NULL;
