-- Output equivalences between two definitions of one kind -- two masks, two
-- alignments, or two processings -- recorded by a system_admin: one verdict on whether
-- the two definitions' outputs are equivalent. `same` and `similar` put the two in one
-- group (qiita.output_equivalence_verdict_joins below is the one SQL statement of
-- that set); `different` and `not_benchmarked` keep them apart; `pending` is
-- undecided. A decision names exactly two definitions and the change between their
-- stored params; it never generalizes to other definitions. Groups are computed on
-- read from these rows and the definitions' params
-- (qiita_control_plane.definition_similarity holds the rules), so nothing here
-- stores a group.
--
--   * `qiita.output_equivalence` is one row per unordered two definitions, ordered
--     low < high by definition id. A decision survives a hand DELETE of either
--     definition: the typed FK goes to NULL (ON DELETE SET NULL), the
--     `*_definition_ref` columns keep the ids, and a trigger stamps `detached_at`. A
--     detached output equivalence links nothing and can no longer be decided or
--     revoked. A live joining one is the exception: a BEFORE DELETE trigger on each
--     definition table refuses the delete while one names the row
--     (refuse_definition_delete_under_joining_equivalence says why). Revoke it first.
--
--   * `qiita.output_equivalence_event` is its history: opened, each decision, each
--     revoke, with who and why. A revoke returns the output equivalence to `pending`
--     and clears the decision columns, so the event row is where the earlier decision
--     remains.
--
-- `definition_kind`, `verdict` and `event` are TEXT + CHECK, not Postgres ENUMs; their
-- Python twins (qiita_common.models.DefinitionKind, OutputEquivalenceVerdict,
-- OutputEquivalenceEventType) are out of scope for the enum-parity test (CLAUDE.md,
-- TEXT/CHECK carve-out).

-- migrate:up

-- The verdicts that join the group: the SQL twin of
-- qiita_common.models.JOINING_VERDICTS, pinned to it by test_output_equivalence.py.
-- Every SQL rule that reads a join calls this.
CREATE FUNCTION qiita.output_equivalence_verdict_joins(p_verdict TEXT) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE AS $$
    SELECT p_verdict IN ('same', 'similar')
$$;

COMMENT ON FUNCTION qiita.output_equivalence_verdict_joins(TEXT) IS
    'True for the output_equivalence verdicts that put the two definitions in one '
    'group (''same'', ''similar''). Python twin: '
    'qiita_common.models.JOINING_VERDICTS.';

CREATE TABLE qiita.output_equivalence (
    output_equivalence_idx BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    definition_kind     TEXT   NOT NULL
        CHECK (definition_kind IN ('mask', 'alignment', 'processing')),

    -- The kind's two columns are set on a live row; the other kinds' are NULL.
    low_mask_idx        BIGINT REFERENCES qiita.mask_definition(mask_idx) ON DELETE SET NULL,
    high_mask_idx       BIGINT REFERENCES qiita.mask_definition(mask_idx) ON DELETE SET NULL,
    low_alignment_idx   BIGINT
        REFERENCES qiita.alignment_definition(alignment_idx) ON DELETE SET NULL,
    high_alignment_idx  BIGINT
        REFERENCES qiita.alignment_definition(alignment_idx) ON DELETE SET NULL,
    low_processing_idx  BIGINT REFERENCES qiita.processing(processing_idx) ON DELETE SET NULL,
    high_processing_idx BIGINT REFERENCES qiita.processing(processing_idx) ON DELETE SET NULL,

    -- The two definition ids, kept when a typed FK above is nulled by a delete.
    low_definition_ref  BIGINT NOT NULL,
    high_definition_ref BIGINT NOT NULL,
    detached_at         TIMESTAMPTZ,

    -- {JSON Pointer param path: [low value, high value]} over the two stored params,
    -- computed when the output equivalence is opened, and recomputed by an audited edit of either
    -- definition's settings (qiita_control_plane.definition_settings).
    change              JSONB   NOT NULL CHECK (jsonb_typeof(change) = 'object'),
    -- True when every changed path is an upstream reference (a mask_idx, or a de novo
    -- alignment's processing_idx); definition_similarity says what that allows.
    derived_only        BOOLEAN NOT NULL,

    verdict             TEXT    NOT NULL DEFAULT 'pending'
        CHECK (verdict IN ('pending', 'same', 'similar', 'different', 'not_benchmarked')),
    decided_by_idx      BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    decided_at          TIMESTAMPTZ,
    decision_reason     TEXT,

    opened_by_idx       BIGINT  NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    opened_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT output_equivalence_ordered CHECK (low_definition_ref < high_definition_ref),

    -- Per kind: the other kinds' columns are NULL; each of the kind's columns is NULL
    -- or equal to its ref; and the row is detached exactly when one of them is NULL.
    CONSTRAINT output_equivalence_typed_refs CHECK (
        CASE definition_kind
            WHEN 'mask' THEN
                num_nonnulls(low_alignment_idx, high_alignment_idx,
                             low_processing_idx, high_processing_idx) = 0
                AND (low_mask_idx IS NULL OR low_mask_idx = low_definition_ref)
                AND (high_mask_idx IS NULL OR high_mask_idx = high_definition_ref)
                AND (detached_at IS NULL) = (num_nonnulls(low_mask_idx, high_mask_idx) = 2)
            WHEN 'alignment' THEN
                num_nonnulls(low_mask_idx, high_mask_idx,
                             low_processing_idx, high_processing_idx) = 0
                AND (low_alignment_idx IS NULL OR low_alignment_idx = low_definition_ref)
                AND (high_alignment_idx IS NULL OR high_alignment_idx = high_definition_ref)
                AND (detached_at IS NULL)
                    = (num_nonnulls(low_alignment_idx, high_alignment_idx) = 2)
            WHEN 'processing' THEN
                num_nonnulls(low_mask_idx, high_mask_idx,
                             low_alignment_idx, high_alignment_idx) = 0
                AND (low_processing_idx IS NULL OR low_processing_idx = low_definition_ref)
                AND (high_processing_idx IS NULL OR high_processing_idx = high_definition_ref)
                AND (detached_at IS NULL)
                    = (num_nonnulls(low_processing_idx, high_processing_idx) = 2)
            ELSE false
        END
    ),

    CONSTRAINT output_equivalence_decision_fields CHECK (
        (verdict = 'pending'
            AND num_nonnulls(decided_by_idx, decided_at, decision_reason) = 0)
        OR (verdict <> 'pending'
            AND num_nonnulls(decided_by_idx, decided_at, decision_reason) = 3)
    ),
    CONSTRAINT output_equivalence_decision_reason_nonblank CHECK (
        decision_reason IS NULL OR btrim(decision_reason) <> ''
    ),
    CONSTRAINT output_equivalence_derived_only_not_joining CHECK (
        NOT derived_only OR NOT qiita.output_equivalence_verdict_joins(verdict)
    )
);

-- One row per unordered two definitions and kind, detached rows included: a deleted
-- definition's id is never minted again (GENERATED ALWAYS AS IDENTITY).
CREATE UNIQUE INDEX output_equivalence_mask_refs
    ON qiita.output_equivalence (low_definition_ref, high_definition_ref)
    WHERE definition_kind = 'mask';
CREATE UNIQUE INDEX output_equivalence_alignment_refs
    ON qiita.output_equivalence (low_definition_ref, high_definition_ref)
    WHERE definition_kind = 'alignment';
CREATE UNIQUE INDEX output_equivalence_processing_refs
    ON qiita.output_equivalence (low_definition_ref, high_definition_ref)
    WHERE definition_kind = 'processing';

COMMENT ON TABLE qiita.output_equivalence IS
    'A system_admin''s verdict on whether the outputs of two definitions of one kind '
    '(mask, alignment, processing) are equivalent: ''same'' and ''similar'' put them '
    'in one group, ''different'' and ''not_benchmarked'' keep them apart, '
    '''pending'' is undecided. Groups are computed on read by '
    'qiita_control_plane.definition_similarity; nothing stores them. A row survives '
    'a hand DELETE of either definition as a detached row.';
COMMENT ON COLUMN qiita.output_equivalence.definition_kind IS
    '''mask'' | ''alignment'' | ''processing'' (TEXT + CHECK). Python twin: '
    'qiita_common.models.DefinitionKind.';
COMMENT ON COLUMN qiita.output_equivalence.low_definition_ref IS
    'The lower of the two definition ids. Equal to the kind''s low_*_idx on a live '
    'row; kept when that FK is nulled by a delete.';
COMMENT ON COLUMN qiita.output_equivalence.high_definition_ref IS
    'The higher of the two definition ids. Equal to the kind''s high_*_idx on a live '
    'row; kept when that FK is nulled by a delete.';
COMMENT ON COLUMN qiita.output_equivalence.detached_at IS
    'Set by qiita.detach_output_equivalence when an ON DELETE SET NULL nulls one of the '
    'kind''s typed FKs. A detached output equivalence links nothing and is not decided or revoked.';
COMMENT ON COLUMN qiita.output_equivalence.change IS
    'JSON object {param path as a JSON Pointer (RFC 6901): [low value, high value]} '
    'of the paths whose '
    'values differ between the two definitions'' stored params. Absent and null are '
    'the same value; lists compare whole.';
COMMENT ON COLUMN qiita.output_equivalence.derived_only IS
    'True when every path in change is an upstream reference '
    '(qiita_control_plane.definition_similarity says what that allows).';
COMMENT ON COLUMN qiita.output_equivalence.verdict IS
    '''pending'' | ''same'' | ''similar'' | ''different'' | ''not_benchmarked'' '
    '(TEXT + CHECK). Python twin: '
    'qiita_common.models.OutputEquivalenceVerdict. decided_by_idx, decided_at and '
    'decision_reason are set exactly when it is not ''pending''.';

-- ---------------------------------------------------------------------------
-- detach_output_equivalence
-- ---------------------------------------------------------------------------
-- Makes ON DELETE SET NULL legal on these FKs: without it the FK action would null a
-- typed column on a row whose detached_at is NULL, and output_equivalence_typed_refs
-- would reject the UPDATE, failing the DELETE. BEFORE UPDATE so the stamp lands in
-- the same statement as the FK action. Fires on any non-null -> null transition of a
-- typed column; the FK action is the only writer that produces one. A second delete
-- of its other definition keeps the first stamp.
CREATE FUNCTION qiita.detach_output_equivalence()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.detached_at IS NULL AND (
        (OLD.low_mask_idx IS NOT NULL AND NEW.low_mask_idx IS NULL)
        OR (OLD.high_mask_idx IS NOT NULL AND NEW.high_mask_idx IS NULL)
        OR (OLD.low_alignment_idx IS NOT NULL AND NEW.low_alignment_idx IS NULL)
        OR (OLD.high_alignment_idx IS NOT NULL AND NEW.high_alignment_idx IS NULL)
        OR (OLD.low_processing_idx IS NOT NULL AND NEW.low_processing_idx IS NULL)
        OR (OLD.high_processing_idx IS NOT NULL AND NEW.high_processing_idx IS NULL)
    ) THEN
        NEW.detached_at := now();
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION qiita.detach_output_equivalence() IS
    'Stamps output_equivalence.detached_at when an ON DELETE SET NULL nulls one of its '
    'typed definition FKs. Required for that FK action to satisfy '
    'output_equivalence_typed_refs -- without it deleting a definition named by an '
    'output equivalence fails.';

CREATE TRIGGER output_equivalence_detach
    BEFORE UPDATE ON qiita.output_equivalence
    FOR EACH ROW EXECUTE FUNCTION qiita.detach_output_equivalence();

-- ---------------------------------------------------------------------------
-- joining_output_equivalences_naming
-- ---------------------------------------------------------------------------
CREATE FUNCTION qiita.joining_output_equivalences_naming(p_kind TEXT, p_ref BIGINT)
RETURNS TEXT LANGUAGE sql STABLE AS $$
    SELECT string_agg(output_equivalence_idx::text, ', ' ORDER BY output_equivalence_idx)
      FROM qiita.output_equivalence
     WHERE definition_kind = p_kind
       AND qiita.output_equivalence_verdict_joins(verdict)
       AND detached_at IS NULL
       AND p_ref IN (low_definition_ref, high_definition_ref)
$$;

COMMENT ON FUNCTION qiita.joining_output_equivalences_naming(TEXT, BIGINT) IS
    'The idxs, comma-separated and ascending, of the live (not detached) joining '
    'output equivalences of kind p_kind that name definition p_ref, or NULL when none.';

-- ---------------------------------------------------------------------------
-- refuse_definition_delete_under_joining_equivalence
-- ---------------------------------------------------------------------------
-- A revoke refuses while another joining output equivalence relies on the one revoked
-- (qiita_control_plane.definition_similarity); detaching a joining one by deleting one
-- of its definitions would skip that check. So a definition named by a live (not
-- detached) joining output equivalence cannot be deleted: SQLSTATE 55006
-- (object_in_use), the message names their idxs. One that does not join (pending, or
-- decided different or not_benchmarked) still detaches.
-- joining_output_equivalences_naming is the lookup, shared with the settings lock
-- (20261116000004_definition_settings_lock).
--
-- TG_ARGV[0] is the definition kind, TG_ARGV[1] the deleted row's id column.
CREATE FUNCTION qiita.refuse_definition_delete_under_joining_equivalence()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    ref    BIGINT := (to_jsonb(OLD) ->> TG_ARGV[1])::bigint;
    joined TEXT   := qiita.joining_output_equivalences_naming(TG_ARGV[0], ref);
BEGIN
    IF joined IS NOT NULL THEN
        RAISE EXCEPTION
            '% % is named by joining output equivalence(s) %; revoke first',
            TG_TABLE_NAME, ref, joined
            USING ERRCODE = '55006';
    END IF;
    RETURN OLD;
END;
$$;

COMMENT ON FUNCTION qiita.refuse_definition_delete_under_joining_equivalence() IS
    'BEFORE DELETE on mask_definition, alignment_definition and processing: refuses '
    '(SQLSTATE 55006) while a not-detached output_equivalence with a joining verdict '
    '(qiita.output_equivalence_verdict_joins) names the row. Revoke it first; one that '
    'does not join detaches.';

CREATE TRIGGER mask_definition_joining_equivalence_delete
    BEFORE DELETE ON qiita.mask_definition
    FOR EACH ROW EXECUTE FUNCTION
        qiita.refuse_definition_delete_under_joining_equivalence('mask', 'mask_idx');
CREATE TRIGGER alignment_definition_joining_equivalence_delete
    BEFORE DELETE ON qiita.alignment_definition
    FOR EACH ROW EXECUTE FUNCTION
        qiita.refuse_definition_delete_under_joining_equivalence('alignment', 'alignment_idx');
CREATE TRIGGER processing_joining_equivalence_delete
    BEFORE DELETE ON qiita.processing
    FOR EACH ROW EXECUTE FUNCTION
        qiita.refuse_definition_delete_under_joining_equivalence('processing', 'processing_idx');

-- ---------------------------------------------------------------------------
-- output_equivalence_event
-- ---------------------------------------------------------------------------
CREATE TABLE qiita.output_equivalence_event (
    output_equivalence_event_idx BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    -- RESTRICT: an output equivalence keeps its history, so one with events (every one
    -- has its 'opened' event) cannot be deleted.
    output_equivalence_idx       BIGINT NOT NULL
        REFERENCES qiita.output_equivalence(output_equivalence_idx) ON DELETE RESTRICT,
    event                     TEXT   NOT NULL CHECK (event IN ('opened', 'decided', 'revoked')),
    -- The output equivalence's verdict after the event.
    verdict                   TEXT   NOT NULL
        CHECK (verdict IN ('pending', 'same', 'similar', 'different', 'not_benchmarked')),
    actor_principal_idx       BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    reason                    TEXT,
    occurred_at               TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT output_equivalence_event_shape CHECK (
        (event = 'opened' AND verdict = 'pending' AND reason IS NULL)
        OR (event = 'decided' AND verdict <> 'pending' AND reason IS NOT NULL)
        OR (event = 'revoked' AND verdict = 'pending' AND reason IS NOT NULL)
    ),
    CONSTRAINT output_equivalence_event_reason_nonblank CHECK (
        reason IS NULL OR btrim(reason) <> ''
    )
);

CREATE INDEX output_equivalence_event_equivalence
    ON qiita.output_equivalence_event (output_equivalence_idx, output_equivalence_event_idx);

COMMENT ON TABLE qiita.output_equivalence_event IS
    'History of a output_equivalence: opened, each decision and each revoke, with the '
    'actor and reason. Append-only: UPDATE and DELETE are refused by triggers, and '
    'the FK to the output equivalence is ON DELETE RESTRICT, so it cannot be deleted '
    'either. A revoke nulls its decision columns; these rows are what keeps who '
    'decided what.';
COMMENT ON COLUMN qiita.output_equivalence_event.event IS
    '''opened'' | ''decided'' | ''revoked'' (TEXT + CHECK). Python twin: '
    'qiita_common.models.OutputEquivalenceEventType.';
COMMENT ON COLUMN qiita.output_equivalence_event.verdict IS
    'The output equivalence''s verdict after this event (TEXT + CHECK). Python twin: '
    'qiita_common.models.OutputEquivalenceVerdict.';

CREATE FUNCTION qiita.tg_output_equivalence_event_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'qiita.output_equivalence_event is append-only (% refused)', TG_OP;
END;
$$;

CREATE TRIGGER output_equivalence_event_no_update
    BEFORE UPDATE ON qiita.output_equivalence_event
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_output_equivalence_event_immutable();
CREATE TRIGGER output_equivalence_event_no_delete
    BEFORE DELETE ON qiita.output_equivalence_event
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_output_equivalence_event_immutable();


-- migrate:down

DROP TRIGGER IF EXISTS output_equivalence_event_no_delete ON qiita.output_equivalence_event;
DROP TRIGGER IF EXISTS output_equivalence_event_no_update ON qiita.output_equivalence_event;
DROP FUNCTION IF EXISTS qiita.tg_output_equivalence_event_immutable();
DROP TABLE IF EXISTS qiita.output_equivalence_event;
DROP TRIGGER IF EXISTS processing_joining_equivalence_delete ON qiita.processing;
DROP TRIGGER IF EXISTS alignment_definition_joining_equivalence_delete ON qiita.alignment_definition;
DROP TRIGGER IF EXISTS mask_definition_joining_equivalence_delete ON qiita.mask_definition;
DROP FUNCTION IF EXISTS qiita.refuse_definition_delete_under_joining_equivalence();
DROP FUNCTION IF EXISTS qiita.joining_output_equivalences_naming(TEXT, BIGINT);
DROP TRIGGER IF EXISTS output_equivalence_detach ON qiita.output_equivalence;
DROP FUNCTION IF EXISTS qiita.detach_output_equivalence();
DROP TABLE IF EXISTS qiita.output_equivalence;
DROP FUNCTION IF EXISTS qiita.output_equivalence_verdict_joins(TEXT);
