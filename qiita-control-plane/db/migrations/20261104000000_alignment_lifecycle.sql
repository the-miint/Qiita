-- Alignment lifecycle at the two granularities the mask and assembly lifecycles
-- already have (20260812010000_mask_lifecycle.sql carries why there are two), plus
-- the block state that lets an operator abandon an align plan without deleting the
-- alignment.
--
--   * `alignment_definition.status = 'deprecated'` voids a CONFIG.
--     qiita.mint_alignment_definition refuses to hand the row back, so no new plan or
--     de novo run is minted under it. A block alignment's identity carries no workflow
--     version, so re-planning the same reference + aligner + mask resolves to the
--     same row: a deprecated block alignment is retired, and re-aligning means a
--     different config.
--
--   * `alignment_sample.state = 'invalidated'` withdraws one prep_sample's RUN of a
--     sound config. Every consumer reads only 'completed', so the third value is
--     refused without editing them.
--
--   * `block.state = 'abandoned'` marks a block an operator gave up on
--     (POST /alignment-definition/{idx}/abandon-plan). The pending gates it
--     covered are cleared so the prep_samples can be re-planned under the same
--     alignment_idx; the block row and its ticket stay as the record of the
--     attempt. A covering-block check reads 'abandoned' as not owing anything.
--
-- All three value sets are TEXT + CHECK, not Postgres ENUMs; their Python twins are
-- out of scope for the enum-parity test (CLAUDE.md, TEXT/CHECK carve-out).
--
-- No backfill: DEFAULT 'active' covers every existing alignment_definition row, and
-- no alignment_sample or block row changes state.

-- migrate:up

ALTER TABLE qiita.alignment_definition
    ADD COLUMN status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'deprecated')),
    ADD COLUMN deprecated_at TIMESTAMPTZ,
    ADD COLUMN deprecated_by_idx BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    ADD COLUMN deprecation_reason TEXT,
    ADD COLUMN superseded_by BIGINT
        REFERENCES qiita.alignment_definition(alignment_idx) ON DELETE SET NULL,
    ADD CONSTRAINT alignment_definition_deprecation_fields CHECK (
        (status = 'active'
            AND num_nonnulls(deprecated_at, deprecated_by_idx, deprecation_reason) = 0)
        OR (status = 'deprecated'
            AND num_nonnulls(deprecated_at, deprecated_by_idx, deprecation_reason) = 3)
    ),
    ADD CONSTRAINT alignment_definition_deprecation_reason_nonblank CHECK (
        deprecation_reason IS NULL OR btrim(deprecation_reason) <> ''
    ),
    ADD CONSTRAINT alignment_definition_superseded_only_when_deprecated CHECK (
        superseded_by IS NULL OR status = 'deprecated'
    ),
    ADD CONSTRAINT alignment_definition_supersede_not_self CHECK (
        superseded_by IS NULL OR superseded_by <> alignment_idx
    );

COMMENT ON COLUMN qiita.alignment_definition.status IS
    'Lifecycle of the alignment CONFIG: ''active'' | ''deprecated'' (TEXT + CHECK, '
    'not a Postgres ENUM). Python twin: qiita_common.models.AlignmentDefinitionStatus. '
    '''deprecated'' means the configuration is void: qiita.mint_alignment_definition '
    'refuses to return the row, and every consumer refuses to read it. It does not '
    'judge any one prep_sample''s run -- that is alignment_sample.state.';

COMMENT ON COLUMN qiita.alignment_definition.superseded_by IS
    'The alignment_definition that replaces this one, when there is one. Settable '
    'only on a deprecated row.';

ALTER TABLE qiita.alignment_sample
    DROP CONSTRAINT alignment_sample_state_check,
    ADD CONSTRAINT alignment_sample_state_check
        CHECK (state IN ('pending', 'completed', 'invalidated')),
    ADD COLUMN invalidated_at TIMESTAMPTZ,
    ADD COLUMN invalidated_by_idx BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    ADD COLUMN invalidation_reason TEXT,
    ADD CONSTRAINT alignment_sample_invalidation_fields CHECK (
        (state <> 'invalidated'
            AND num_nonnulls(invalidated_at, invalidated_by_idx, invalidation_reason) = 0)
        OR (state = 'invalidated'
            AND num_nonnulls(invalidated_at, invalidated_by_idx, invalidation_reason) = 3)
    ),
    ADD CONSTRAINT alignment_sample_invalidation_reason_nonblank CHECK (
        invalidation_reason IS NULL OR btrim(invalidation_reason) <> ''
    );

-- Supersedes 20260823000000's comment, keeping its two-writer content and adding
-- the third state and the abandon path.
COMMENT ON TABLE qiita.alignment_sample IS
    'Per-(alignment_idx, prep_sample) completion gate for alignment, written by two '
    'paths that never share an identity: bulk-block sharded alignment materializes '
    '''pending'' at plan time and flips ''completed'' at reconcile once every '
    'covering block finished, and per-prep_sample de novo alignment materializes it when '
    'the runner mints the identity and flips it at the workflow''s terminal step. '
    'Which path a row came from is readable from the alignment_definition''s params: '
    'a block identity hashes a reference_idx and a shard set, a de novo one hashes an '
    'assembly subject. A completed run later judged untrustworthy is set '
    '''invalidated'', with who/when/why. A ''pending'' row whose covering blocks were '
    'abandoned is deleted by the abandon-plan route so the prep_sample can be re-planned. '
    'Consumers read ONLY ''completed'' prep_samples -- alignment rows are NOT 1:1 with '
    'reads, so presence of rows must never be read as "done". Python twin: '
    'qiita_common.models.AlignmentSampleState. Twin of qiita.mask_sample.';

COMMENT ON COLUMN qiita.alignment_sample.state IS
    'Gate state: ''pending'' | ''completed'' | ''invalidated'' (TEXT + CHECK, not a '
    'Postgres ENUM). ''invalidated'' is terminal for this (alignment, prep_sample) pair: '
    'the run finished and its output is not to be consumed.';

ALTER TABLE qiita.block
    DROP CONSTRAINT block_state_check,
    ADD CONSTRAINT block_state_check
        CHECK (state IN ('pending', 'processing', 'completed', 'failed', 'abandoned'));

COMMENT ON COLUMN qiita.block.state IS
    '''pending'' | ''processing'' | ''completed'' | ''failed'' | ''abandoned'' (TEXT + '
    'CHECK). ''abandoned'' is set only by the align abandon-plan route: the block''s '
    'rows were purged and the pending gates it covered were cleared, so it no longer '
    'counts toward any prep_sample''s completion and its ticket may not be redriven.';

-- ---------------------------------------------------------------------------
-- mint_alignment_definition: refuse to hand back a deprecated config
-- ---------------------------------------------------------------------------
-- Enforced inside the function because the function IS the mint, the same
-- placement as qiita.mint_mask_definition (20260812010000). Restated whole: the
-- body is 20260712000000's plus the deprecation check, with the same signature so
-- this replaces in place rather than adding an overload.
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
        SELECT * INTO v_row
            FROM qiita.alignment_definition
            WHERE params_hash = p_params_hash;
        IF FOUND THEN
            -- A freshly INSERTed row is 'active' by default, so only this path
            -- can meet a deprecated one.
            IF v_row.status = 'deprecated' THEN
                RAISE EXCEPTION
                    'alignment_definition % is deprecated and cannot be minted against: %',
                    v_row.alignment_idx, v_row.deprecation_reason
                    USING ERRCODE = '23514';
            END IF;
            RETURN v_row;
        END IF;

        INSERT INTO qiita.alignment_definition (params_hash, params, created_by_idx)
            VALUES (p_params_hash, p_params, p_principal_idx)
            ON CONFLICT (params_hash) DO NOTHING
            RETURNING * INTO v_row;
        IF FOUND THEN
            RETURN v_row;
        END IF;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION qiita.mint_alignment_definition(bytea, jsonb, bigint) IS
    'Idempotent mint of a qiita.alignment_definition row: returns the existing '
    'row when p_params_hash already exists, otherwise inserts and returns the '
    'new row. Raises SQLSTATE 23514 when the existing row is deprecated, and '
    'SQLSTATE 22023 when p_params_hash is not 32 bytes; propagates '
    'ForeignKeyViolation (unknown p_principal_idx) from the INSERT.';

-- ---------------------------------------------------------------------------
-- qc_verdict: an alignment row counts only under an active alignment
-- ---------------------------------------------------------------------------
-- Restated from 20261020000000 with `ad.status = 'active'` beside the gate check,
-- matching the mask arm.
CREATE OR REPLACE VIEW qiita.qc_verdict AS
SELECT DISTINCT ON (c.prep_sample_idx)
       c.prep_sample_idx, c.severity, c."check"
  FROM qiita.prep_sample_qc_check c
  LEFT JOIN qiita.mask_definition md ON md.mask_idx = c.mask_idx
  LEFT JOIN qiita.mask_sample ms
    ON ms.mask_idx = c.mask_idx AND ms.prep_sample_idx = c.prep_sample_idx
  LEFT JOIN qiita.alignment_definition ad ON ad.alignment_idx = c.alignment_idx
  LEFT JOIN qiita.alignment_sample als
    ON als.alignment_idx = c.alignment_idx AND als.prep_sample_idx = c.prep_sample_idx
 WHERE c.state = 'current'
   AND (c.mask_idx IS NULL OR (md.status = 'active' AND ms.state = 'completed'))
   AND (c.alignment_idx IS NULL OR (ad.status = 'active' AND als.state = 'completed'))
 ORDER BY c.prep_sample_idx,
          CASE c.severity WHEN 'fail' THEN 3 WHEN 'warning' THEN 2 ELSE 1 END DESC,
          c."check", c.idx;

COMMENT ON VIEW qiita.qc_verdict IS
    'Worst severity per prep_sample among the rows that count: current, for a mask row '
    'an active mask with a completed mask_sample, and for an alignment row an active '
    'alignment with a completed alignment_sample. Python twin: qiita_common.models.QcVerdict.';


-- migrate:down

CREATE OR REPLACE VIEW qiita.qc_verdict AS
SELECT DISTINCT ON (c.prep_sample_idx)
       c.prep_sample_idx, c.severity, c."check"
  FROM qiita.prep_sample_qc_check c
  LEFT JOIN qiita.mask_definition md ON md.mask_idx = c.mask_idx
  LEFT JOIN qiita.mask_sample ms
    ON ms.mask_idx = c.mask_idx AND ms.prep_sample_idx = c.prep_sample_idx
  LEFT JOIN qiita.alignment_sample als
    ON als.alignment_idx = c.alignment_idx AND als.prep_sample_idx = c.prep_sample_idx
 WHERE c.state = 'current'
   AND (c.mask_idx IS NULL OR (md.status = 'active' AND ms.state = 'completed'))
   AND (c.alignment_idx IS NULL OR als.state = 'completed')
 ORDER BY c.prep_sample_idx,
          CASE c.severity WHEN 'fail' THEN 3 WHEN 'warning' THEN 2 ELSE 1 END DESC,
          c."check", c.idx;

COMMENT ON VIEW qiita.qc_verdict IS
    'Worst severity per prep_sample among the rows that count: current, for a mask row '
    'an active mask with a completed mask_sample, and for an alignment row a completed '
    'alignment_sample. Python twin: qiita_common.models.QcVerdict.';

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
        SELECT * INTO v_row
            FROM qiita.alignment_definition
            WHERE params_hash = p_params_hash;
        IF FOUND THEN
            RETURN v_row;
        END IF;

        INSERT INTO qiita.alignment_definition (params_hash, params, created_by_idx)
            VALUES (p_params_hash, p_params, p_principal_idx)
            ON CONFLICT (params_hash) DO NOTHING
            RETURNING * INTO v_row;
        IF FOUND THEN
            RETURN v_row;
        END IF;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION qiita.mint_alignment_definition(bytea, jsonb, bigint) IS
    'Idempotent mint of a qiita.alignment_definition row: returns the existing '
    'row when p_params_hash already exists, otherwise inserts and returns the '
    'new row. Raises SQLSTATE 22023 when p_params_hash is not 32 bytes; '
    'propagates ForeignKeyViolation (unknown p_principal_idx) from the INSERT.';

-- An abandoned block has no faithful earlier value. As 'failed' it would count as an
-- incomplete cover for its prep_samples' re-planned gates, which would never
-- finalize, and its ticket would fail-stop the re-plan's fan-out. So the rollback
-- stops instead of rewriting it.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM qiita.block WHERE state = 'abandoned') THEN
        RAISE EXCEPTION 'qiita.block has abandoned blocks; the schema before this '
            'migration cannot represent them, so it is not rolled back';
    END IF;
END
$$;

-- An invalidated run goes back to 'completed' and a deprecated config to 'active':
-- the judgment is lost, not translated. The CHECKs that tie provenance to state go
-- first, since a reverted row still carries its provenance until the columns drop.
ALTER TABLE qiita.block
    DROP CONSTRAINT block_state_check,
    ADD CONSTRAINT block_state_check
        CHECK (state IN ('pending', 'processing', 'completed', 'failed'));
COMMENT ON COLUMN qiita.block.state IS NULL;

ALTER TABLE qiita.alignment_definition
    DROP CONSTRAINT IF EXISTS alignment_definition_supersede_not_self,
    DROP CONSTRAINT IF EXISTS alignment_definition_superseded_only_when_deprecated,
    DROP CONSTRAINT IF EXISTS alignment_definition_deprecation_reason_nonblank,
    DROP CONSTRAINT IF EXISTS alignment_definition_deprecation_fields;

ALTER TABLE qiita.alignment_sample
    DROP CONSTRAINT IF EXISTS alignment_sample_invalidation_reason_nonblank,
    DROP CONSTRAINT IF EXISTS alignment_sample_invalidation_fields;

UPDATE qiita.alignment_sample SET state = 'completed' WHERE state = 'invalidated';

ALTER TABLE qiita.alignment_sample
    DROP COLUMN IF EXISTS invalidation_reason,
    DROP COLUMN IF EXISTS invalidated_by_idx,
    DROP COLUMN IF EXISTS invalidated_at,
    DROP CONSTRAINT IF EXISTS alignment_sample_state_check,
    ADD CONSTRAINT alignment_sample_state_check CHECK (state IN ('pending', 'completed'));

ALTER TABLE qiita.alignment_definition
    DROP COLUMN IF EXISTS superseded_by,
    DROP COLUMN IF EXISTS deprecation_reason,
    DROP COLUMN IF EXISTS deprecated_by_idx,
    DROP COLUMN IF EXISTS deprecated_at,
    DROP COLUMN IF EXISTS status;

COMMENT ON TABLE qiita.alignment_sample IS
    'Per-(alignment_idx, prep_sample) completion gate for alignment, written by two '
    'paths that never share an identity: bulk-block sharded alignment materializes '
    '''pending'' at plan time and flips ''completed'' at reconcile once every '
    'covering block finished, and per-sample de novo alignment materializes it when '
    'the runner mints the identity and flips it at the workflow''s terminal step. '
    'Which path a row came from is readable from the alignment_definition''s params: '
    'a block identity hashes a reference_idx and a shard set, a de novo one hashes an '
    'assembly subject. Consumers read ONLY ''completed'' samples — alignment rows are '
    'NOT 1:1 with reads, so presence of rows must never be read as "done". Twin of '
    'qiita.mask_sample.';

COMMENT ON COLUMN qiita.alignment_sample.state IS NULL;
