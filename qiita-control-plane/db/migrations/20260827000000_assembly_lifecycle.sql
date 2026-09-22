-- Assembly lifecycle at TWO granularities, the assembly twin of the mask
-- lifecycle (20260812010000). The two markers answer two different questions,
-- and the mask migration's header states why that split exists; this one states
-- only what is different on the assembly side.
--
-- qiita.processing is the assembly run identity: the canonical-params hash over
-- {workflow, version, mask_idx, assembler} that both qiita.assembly_membership
-- and the DuckLake assembly tables are stamped with. The hash covers the mask
-- that selected the reads, not the judgement that the mask was sound, so a run
-- built from a pass-set later found unsound re-resolves to the SAME
-- processing_idx and assembles new samples from the same defect.
--
--   * `processing.status` answers "is this run CONFIGURATION void?" — the mask
--     it assembled, the assembler it named. It is what stops NEW bad data, by
--     refusing the mint (see qiita.mint_processing below).
--
--   * `assembly_sample.state = 'invalidated'` answers "is this RUN of a sound
--     config bad?" — the (run, sample) pair that actually succeeded or failed.
--
-- Invalidation is a `state` VALUE rather than a column beside `state` for the
-- reason the mask migration gives: the gate contract (canonical statement at
-- repositories/assembly.py::fetch_assembly_sample_state) is that a consumer
-- needing contigs proceeds ONLY on 'completed', so a fourth value is refused by
-- construction rather than by a check each consumer had to remember.
--
-- What differs from mask_sample: this gate already carries a second terminal
-- value, 'no_data'. It is NOT withdrawable — a run that assembled no contig has
-- no output to withdraw — so the PATCH route skips such a row rather than
-- writing over it, the same way it skips 'pending'.
--
-- Both value sets stay TEXT + CHECK, not Postgres ENUMs, matching
-- mask_definition.status / mask_sample.state and the existing
-- assembly_sample.state. Their Python twins (ProcessingStatus,
-- AssemblySampleState in qiita_common.models) are therefore out of scope for
-- the enum-parity test, per the TEXT/CHECK carve-out in CLAUDE.md.
--
-- No backfill: DEFAULT 'active' covers every existing qiita.processing row, and
-- no assembly_sample row changes state.

-- migrate:up

ALTER TABLE qiita.processing
    ADD COLUMN status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'deprecated')),
    ADD COLUMN deprecated_at TIMESTAMPTZ,
    ADD COLUMN deprecated_by_idx BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    ADD COLUMN deprecation_reason TEXT,
    -- The run that replaces this one, when there is one. Nullable: a config can
    -- be void with nothing to point at yet. ON DELETE SET NULL rather than
    -- RESTRICT so deleting the replacement does not pin the deprecated row's
    -- whole lifecycle.
    ADD COLUMN superseded_by BIGINT REFERENCES qiita.processing(processing_idx) ON DELETE SET NULL,

    -- Biconditional: the three provenance fields are set exactly when
    -- deprecated. A reason is mandatory: it is what a reader of a deprecated run
    -- behind published contigs has to go on.
    ADD CONSTRAINT processing_deprecation_fields CHECK (
        (status = 'deprecated')
            = (deprecated_at IS NOT NULL
               AND deprecated_by_idx IS NOT NULL
               AND deprecation_reason IS NOT NULL)
    ),
    ADD CONSTRAINT processing_deprecation_reason_nonblank CHECK (
        deprecation_reason IS NULL OR btrim(deprecation_reason) <> ''
    ),
    ADD CONSTRAINT processing_superseded_only_when_deprecated CHECK (
        superseded_by IS NULL OR status = 'deprecated'
    ),
    ADD CONSTRAINT processing_supersede_not_self CHECK (
        superseded_by IS NULL OR superseded_by <> processing_idx
    );

COMMENT ON COLUMN qiita.processing.status IS
    'Lifecycle of the run CONFIG: ''active'' | ''deprecated'' (TEXT + CHECK, not '
    'a Postgres ENUM). Python twin: qiita_common.models.ProcessingStatus. '
    '''deprecated'' means the configuration itself is void, so '
    'qiita.mint_processing refuses to return the row and no new sample can be '
    'assembled under it. It does NOT judge any individual run — that is '
    'assembly_sample.state.';

-- Supersede the applied 20260707000000 COMMENT, whose "Mint via
-- qiita.mint_processing (idempotent upsert on params_hash)" is now conditional.
COMMENT ON TABLE qiita.processing IS
    'CP-minted processing-run identity. One row per distinct assembly run — the '
    'workflow + version + result-affecting params (mask_idx, assembler); '
    'deduplicated on params_hash (SHA-256 of the canonical params JSON) so the '
    'same params resolve to the same processing_idx fleet-wide. Mint via '
    'qiita.mint_processing (idempotent upsert on params_hash, EXCEPT that a '
    'status=''deprecated'' row is refused rather than returned); the canonical '
    'params are built CP-side in the runner _build_processing_params.';

COMMENT ON COLUMN qiita.processing.superseded_by IS
    'The qiita.processing row that replaces this one, when a re-mint under a '
    'corrected mask or assembler produced a new identity. Settable only on a '
    'deprecated row.';

ALTER TABLE qiita.assembly_sample
    DROP CONSTRAINT assembly_sample_state_check,
    ADD CONSTRAINT assembly_sample_state_check
        CHECK (state IN ('pending', 'completed', 'no_data', 'invalidated')),
    ADD COLUMN invalidated_at TIMESTAMPTZ,
    ADD COLUMN invalidated_by_idx BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    ADD COLUMN invalidation_reason TEXT,
    ADD CONSTRAINT assembly_sample_invalidation_fields CHECK (
        (state = 'invalidated')
            = (invalidated_at IS NOT NULL
               AND invalidated_by_idx IS NOT NULL
               AND invalidation_reason IS NOT NULL)
    ),
    ADD CONSTRAINT assembly_sample_invalidation_reason_nonblank CHECK (
        invalidation_reason IS NULL OR btrim(invalidation_reason) <> ''
    );

-- Supersede the applied 20260819000001 COMMENT, keeping its content — which
-- writer produces each of the three original states, and that completion is this
-- column's value rather than the presence of rows — and adding the fourth.
COMMENT ON TABLE qiita.assembly_sample IS
    'Per-(processing_idx, prep_sample) completion gate for long-read assembly. '
    'Materialized ''pending'' when the runner mints the run identity, then '
    'written ''completed'' by the terminal finalize-assembly-sample action or '
    '''no_data'' by the runner when assembly_hash found no contig of any kind. A '
    'completed run later judged untrustworthy is set ''invalidated'', recorded '
    'with who/when/why. Completion is this column''s value: the presence of '
    'qiita.assembly_membership or DuckLake rows does not imply it, because the '
    'assembly tail writes those across several workflow entries. What each state '
    'and a missing row do and do not tell a consumer is the read contract at '
    'repositories/assembly.py::fetch_assembly_sample_state. Twin of '
    'qiita.mask_sample / qiita.alignment_sample.';

COMMENT ON COLUMN qiita.assembly_sample.state IS
    'Gate state: ''pending'' | ''completed'' | ''no_data'' | ''invalidated'' '
    '(TEXT + CHECK, not a Postgres ENUM). Python twin: '
    'qiita_common.models.AssemblySampleState. ''invalidated'' says the assembly '
    'ran, produced contigs, and its output is not to be consumed; the contigs '
    'stay in DuckLake and the run stays answerable, so provenance for anything '
    'already published survives the withdrawal.';

-- ---------------------------------------------------------------------------
-- mint_processing: refuse to hand back a deprecated config
-- ---------------------------------------------------------------------------
-- Enforced inside the function rather than at its caller (runner/_processing.py)
-- because the function IS the mint: a caller that reached the dedup and got a row
-- back has, by construction, not been told the config is void.
--
-- Restated whole because CREATE OR REPLACE FUNCTION has no partial form. The
-- body is 20260707000000's plus the deprecation check on the fast path; a
-- freshly INSERTed row is 'active' by default, so that is the only arm that can
-- return a deprecated row.
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
            IF v_row.status = 'deprecated' THEN
                RAISE EXCEPTION
                    'processing % is deprecated and cannot be minted against: %',
                    v_row.processing_idx, v_row.deprecation_reason
                    USING ERRCODE = '23514';
            END IF;
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
    'Raises SQLSTATE 22023 when p_params_hash is not 32 bytes; raises SQLSTATE '
    '23514 when the matched row is status=''deprecated'' (a void config must not '
    'assemble new data).';

-- migrate:down

-- Restore the pre-lifecycle function FIRST: plpgsql resolves column references
-- at run time, so leaving the status-checking body behind after the column is
-- dropped would break every mint rather than fail here. Logic identical to
-- 20260707000000's.
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
    END LOOP;
END;
$$;

-- Order matters: the biconditional CHECKs are dropped BEFORE the state/status
-- rollback, because a row reverted to 'active'/'completed' still carries its
-- provenance columns until they are dropped, which is exactly what those CHECKs
-- forbid.
ALTER TABLE qiita.processing
    DROP CONSTRAINT IF EXISTS processing_supersede_not_self,
    DROP CONSTRAINT IF EXISTS processing_superseded_only_when_deprecated,
    DROP CONSTRAINT IF EXISTS processing_deprecation_reason_nonblank,
    DROP CONSTRAINT IF EXISTS processing_deprecation_fields;

ALTER TABLE qiita.assembly_sample
    DROP CONSTRAINT IF EXISTS assembly_sample_invalidation_reason_nonblank,
    DROP CONSTRAINT IF EXISTS assembly_sample_invalidation_fields;

-- A row this migration marked goes back to what it was before there was anywhere
-- to say otherwise. The judgement is lost, not translated: nothing downstream of
-- this migration can express it. 'completed' is the only state an invalidated
-- row can have come from — the PATCH route refuses to withdraw a 'pending' or
-- 'no_data' row — so the reversal is exact rather than a guess.
UPDATE qiita.assembly_sample SET state = 'completed' WHERE state = 'invalidated';
UPDATE qiita.processing SET status = 'active' WHERE status = 'deprecated';

ALTER TABLE qiita.assembly_sample
    DROP COLUMN IF EXISTS invalidation_reason,
    DROP COLUMN IF EXISTS invalidated_by_idx,
    DROP COLUMN IF EXISTS invalidated_at,
    DROP CONSTRAINT IF EXISTS assembly_sample_state_check,
    ADD CONSTRAINT assembly_sample_state_check
        CHECK (state IN ('pending', 'completed', 'no_data'));

ALTER TABLE qiita.processing
    DROP COLUMN IF EXISTS superseded_by,
    DROP COLUMN IF EXISTS deprecation_reason,
    DROP COLUMN IF EXISTS deprecated_by_idx,
    DROP COLUMN IF EXISTS deprecated_at,
    DROP COLUMN IF EXISTS status;

-- Restore the comments the up-block superseded. A COMMENT outlives the column it
-- describes, so leaving these attached would document a lifecycle the reverted
-- schema cannot express — the function comment asserting an ERRCODE the reverted
-- body never raises, the table comment naming a state the re-tightened CHECK
-- forbids. The function and table comments go back to their predecessors' text
-- (20260707000000, 20260819000001); the two column comments are new here, so
-- they clear with their columns.
COMMENT ON FUNCTION qiita.mint_processing(bytea, text, text, jsonb) IS
    'Idempotent mint of a qiita.processing row: returns the existing row when '
    'p_params_hash already exists, otherwise inserts and returns the new row. '
    'Raises SQLSTATE 22023 when p_params_hash is not 32 bytes.';

COMMENT ON TABLE qiita.assembly_sample IS
    'Per-(processing_idx, prep_sample) completion gate for long-read assembly. '
    'Materialized ''pending'' when the runner mints the run identity, then '
    'written ''completed'' by the terminal finalize-assembly-sample action or '
    '''no_data'' by the runner when assembly_hash found no contig of any kind. '
    'Completion is this column''s value: the presence of '
    'qiita.assembly_membership or DuckLake rows does not imply it, because the '
    'assembly tail writes those across several workflow entries. ''completed'' '
    'and ''no_data'' are terminal for the run; what ''pending'' and a missing '
    'row do and do not tell a consumer is the read contract at '
    'repositories/assembly.py::fetch_assembly_sample_state. Twin of '
    'qiita.mask_sample / qiita.alignment_sample.';

COMMENT ON TABLE qiita.processing IS
    'CP-minted processing-run identity. One row per distinct assembly run — the '
    'workflow + version + result-affecting params (mask_idx, assembler); '
    'deduplicated on params_hash (SHA-256 of the canonical params JSON) so the '
    'same params resolve to the same processing_idx fleet-wide. Mint via '
    'qiita.mint_processing (idempotent upsert on params_hash); the canonical '
    'params are built CP-side in the runner _build_processing_params.';

-- Only assembly_sample.state needs clearing: DROP COLUMN takes its own comment
-- with it, so processing.status / processing.superseded_by are already gone.
COMMENT ON COLUMN qiita.assembly_sample.state IS NULL;
