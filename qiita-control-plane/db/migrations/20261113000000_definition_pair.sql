-- Pair decisions between two definitions of one kind -- two masks, two alignments,
-- or two processings -- recorded by a system_admin: `similar` puts the two in one
-- group, `not_similar` keeps them apart. A decision names exactly two definitions and
-- the change between their stored params; it never generalizes to other pairs.
-- Groups are computed on read from these rows and the definitions' params
-- (qiita_control_plane.definition_similarity holds the rules), so nothing here
-- stores a group.
--
--   * `qiita.definition_pair` is one row per unordered pair, ordered low < high by
--     definition id. A decision survives a hand DELETE of either definition: the
--     typed FK goes to NULL (ON DELETE SET NULL), the `*_definition_ref` columns keep
--     the ids, and a trigger stamps `detached_at`. A detached pair links nothing and
--     can no longer be decided or revoked.
--
--   * `qiita.definition_pair_event` is the pair's history: opened, each decision,
--     each revoke, with who and why. A revoke returns the pair to `pending` and clears
--     the decision columns, so the event row is where the earlier decision remains.
--
-- `definition_kind`, `verdict` and `event` are TEXT + CHECK, not Postgres ENUMs; their
-- Python twins (qiita_common.models.DefinitionKind, DefinitionPairVerdict,
-- DefinitionPairEventType) are out of scope for the enum-parity test (CLAUDE.md,
-- TEXT/CHECK carve-out).

-- migrate:up

CREATE TABLE qiita.definition_pair (
    definition_pair_idx BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
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

    -- {dotted.param.path: [low value, high value]} over the two stored params,
    -- computed when the pair is opened. Params never change after a mint, so it is
    -- not recomputed.
    change              JSONB   NOT NULL CHECK (jsonb_typeof(change) = 'object'),
    -- True when every changed path is an upstream reference (a mask_idx, or a de novo
    -- alignment's processing_idx). Such a pair groups by derivation already; it can
    -- be decided only `not_similar`.
    derived_only        BOOLEAN NOT NULL,

    verdict             TEXT    NOT NULL DEFAULT 'pending'
        CHECK (verdict IN ('pending', 'similar', 'not_similar')),
    decided_by_idx      BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    decided_at          TIMESTAMPTZ,
    decision_reason     TEXT,

    opened_by_idx       BIGINT  NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    opened_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT definition_pair_ordered CHECK (low_definition_ref < high_definition_ref),

    -- Per kind: the other kinds' columns are NULL; each of the kind's columns is NULL
    -- or equal to its ref; and the row is detached exactly when one of them is NULL.
    CONSTRAINT definition_pair_typed_refs CHECK (
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

    CONSTRAINT definition_pair_decision_fields CHECK (
        (verdict = 'pending'
            AND num_nonnulls(decided_by_idx, decided_at, decision_reason) = 0)
        OR (verdict <> 'pending'
            AND num_nonnulls(decided_by_idx, decided_at, decision_reason) = 3)
    ),
    CONSTRAINT definition_pair_decision_reason_nonblank CHECK (
        decision_reason IS NULL OR btrim(decision_reason) <> ''
    ),
    CONSTRAINT definition_pair_derived_only_not_similar CHECK (
        NOT derived_only OR verdict <> 'similar'
    )
);

-- One row per unordered pair and kind, detached rows included: a deleted
-- definition's id is never minted again (GENERATED ALWAYS AS IDENTITY).
CREATE UNIQUE INDEX definition_pair_mask_refs
    ON qiita.definition_pair (low_definition_ref, high_definition_ref)
    WHERE definition_kind = 'mask';
CREATE UNIQUE INDEX definition_pair_alignment_refs
    ON qiita.definition_pair (low_definition_ref, high_definition_ref)
    WHERE definition_kind = 'alignment';
CREATE UNIQUE INDEX definition_pair_processing_refs
    ON qiita.definition_pair (low_definition_ref, high_definition_ref)
    WHERE definition_kind = 'processing';

COMMENT ON TABLE qiita.definition_pair IS
    'A system_admin''s decision on two definitions of one kind (mask, alignment, '
    'processing): ''similar'' puts them in one group, ''not_similar'' keeps them '
    'apart, ''pending'' is undecided. Groups are computed on read by '
    'qiita_control_plane.definition_similarity; nothing stores them. A row survives '
    'a hand DELETE of either definition as a detached row.';
COMMENT ON COLUMN qiita.definition_pair.definition_kind IS
    '''mask'' | ''alignment'' | ''processing'' (TEXT + CHECK). Python twin: '
    'qiita_common.models.DefinitionKind.';
COMMENT ON COLUMN qiita.definition_pair.low_definition_ref IS
    'The lower definition id of the pair. Equal to the kind''s low_*_idx on a live '
    'row; kept when that FK is nulled by a delete.';
COMMENT ON COLUMN qiita.definition_pair.high_definition_ref IS
    'The higher definition id of the pair. Equal to the kind''s high_*_idx on a live '
    'row; kept when that FK is nulled by a delete.';
COMMENT ON COLUMN qiita.definition_pair.detached_at IS
    'Set by qiita.detach_definition_pair when an ON DELETE SET NULL nulls one of the '
    'kind''s typed FKs. A detached pair links nothing and is not decided or revoked.';
COMMENT ON COLUMN qiita.definition_pair.change IS
    'JSON object {dotted.param.path: [low value, high value]} of the paths whose '
    'values differ between the two definitions'' stored params. Absent and null are '
    'the same value; lists compare whole.';
COMMENT ON COLUMN qiita.definition_pair.derived_only IS
    'True when every path in change is an upstream reference. Decidable '
    '''not_similar'' only.';
COMMENT ON COLUMN qiita.definition_pair.verdict IS
    '''pending'' | ''similar'' | ''not_similar'' (TEXT + CHECK). Python twin: '
    'qiita_common.models.DefinitionPairVerdict. decided_by_idx, decided_at and '
    'decision_reason are set exactly when it is not ''pending''.';

-- ---------------------------------------------------------------------------
-- detach_definition_pair
-- ---------------------------------------------------------------------------
-- Makes ON DELETE SET NULL legal on these FKs: without it the FK action would null a
-- typed column on a row whose detached_at is NULL, and definition_pair_typed_refs
-- would reject the UPDATE, failing the DELETE. BEFORE UPDATE so the stamp lands in
-- the same statement as the FK action. Fires on any non-null -> null transition of a
-- typed column; the FK action is the only writer that produces one. A second delete
-- of the pair's other definition keeps the first stamp.
CREATE FUNCTION qiita.detach_definition_pair()
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

COMMENT ON FUNCTION qiita.detach_definition_pair() IS
    'Stamps definition_pair.detached_at when an ON DELETE SET NULL nulls one of its '
    'typed definition FKs. Required for that FK action to satisfy '
    'definition_pair_typed_refs -- without it deleting a definition named by a pair '
    'fails.';

CREATE TRIGGER definition_pair_detach
    BEFORE UPDATE ON qiita.definition_pair
    FOR EACH ROW EXECUTE FUNCTION qiita.detach_definition_pair();

-- ---------------------------------------------------------------------------
-- definition_pair_event
-- ---------------------------------------------------------------------------
CREATE TABLE qiita.definition_pair_event (
    definition_pair_event_idx BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    definition_pair_idx       BIGINT NOT NULL
        REFERENCES qiita.definition_pair(definition_pair_idx) ON DELETE CASCADE,
    event                     TEXT   NOT NULL CHECK (event IN ('opened', 'decided', 'revoked')),
    -- The pair's verdict after the event.
    verdict                   TEXT   NOT NULL
        CHECK (verdict IN ('pending', 'similar', 'not_similar')),
    actor_principal_idx       BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    reason                    TEXT,
    occurred_at               TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT definition_pair_event_shape CHECK (
        (event = 'opened' AND verdict = 'pending' AND reason IS NULL)
        OR (event = 'decided' AND verdict <> 'pending' AND reason IS NOT NULL)
        OR (event = 'revoked' AND verdict = 'pending' AND reason IS NOT NULL)
    ),
    CONSTRAINT definition_pair_event_reason_nonblank CHECK (
        reason IS NULL OR btrim(reason) <> ''
    )
);

CREATE INDEX definition_pair_event_pair
    ON qiita.definition_pair_event (definition_pair_idx, definition_pair_event_idx);

COMMENT ON TABLE qiita.definition_pair_event IS
    'History of a definition_pair: opened, each decision and each revoke, with the '
    'actor and reason. Rows are never updated; they go only with their pair.';
COMMENT ON COLUMN qiita.definition_pair_event.event IS
    '''opened'' | ''decided'' | ''revoked'' (TEXT + CHECK). Python twin: '
    'qiita_common.models.DefinitionPairEventType.';
COMMENT ON COLUMN qiita.definition_pair_event.verdict IS
    'The pair''s verdict after this event (TEXT + CHECK). Python twin: '
    'qiita_common.models.DefinitionPairVerdict.';

CREATE FUNCTION qiita.tg_definition_pair_event_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'qiita.definition_pair_event rows are not updated';
END;
$$;

CREATE TRIGGER definition_pair_event_no_update
    BEFORE UPDATE ON qiita.definition_pair_event
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_definition_pair_event_immutable();


-- migrate:down

DROP TRIGGER IF EXISTS definition_pair_event_no_update ON qiita.definition_pair_event;
DROP FUNCTION IF EXISTS qiita.tg_definition_pair_event_immutable();
DROP TABLE IF EXISTS qiita.definition_pair_event;
DROP TRIGGER IF EXISTS definition_pair_detach ON qiita.definition_pair;
DROP FUNCTION IF EXISTS qiita.detach_definition_pair();
DROP TABLE IF EXISTS qiita.definition_pair;
