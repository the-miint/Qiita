-- migrate:up

-- A mask, alignment or processing is minted from its settings, and a pair decision,
-- the derivation grouping and every published params_hash read those settings. After
-- the mint they are locked: a BEFORE UPDATE trigger on each table refuses a change
-- to them unless the same transaction recorded it in
-- `qiita.definition_settings_edit`, which only `qiita.edit_definition_settings`
-- writes for a system_admin, with old and new values and a reason.
--
-- Locked (the settings):
--   mask_definition:      params, params_hash, filter_workflow, filter_version,
--                         adapter_hash_scheme
--   alignment_definition: params, params_hash
--   processing:           params, params_hash, workflow, version
-- Editable: the id, created_by_idx / created_at, and the lifecycle columns status,
-- deprecated_at, deprecated_by_idx, deprecation_reason, superseded_by.
--
-- One transition passes without an admin edit: the adapter-hash re-key of a mask
-- (`adapter_hash_scheme` NULL -> 'sequence_hash_v1', with params and params_hash
-- rewritten and filter_workflow / filter_version unchanged). The mint's legacy
-- path and `qiita-admin backfill mask-adapter-hash` make it; it rewrites how the
-- adapter set is hashed, not what the mask does. The trigger records it in the
-- audit table with no actor.
--
-- Any other change to a definition that a live `similar` pair names is refused
-- (SQLSTATE 55006), as its delete is (`refuse_definition_delete_under_similar_pair`): the
-- groups that pair joins were decided over the old settings. Revoke it first.
--
-- Residual: the database role the services use can INSERT an audit row itself and
-- then UPDATE. The row must name a system_admin and carry the exact old and new
-- values, so the edit is still recorded; that it went through the route is not.

CREATE TABLE qiita.definition_settings_edit (
    definition_settings_edit_idx BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    definition_kind     TEXT   NOT NULL
        CHECK (definition_kind IN ('mask', 'alignment', 'processing')),
    -- The edited definition's id. No FK: the record outlives a delete of the row.
    definition_ref      BIGINT NOT NULL,
    edit_kind           TEXT   NOT NULL CHECK (edit_kind IN ('admin_edit', 'adapter_hash_rekey')),
    actor_principal_idx BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    -- {locked column: value} before and after, as to_jsonb of the row serializes them.
    old_settings        JSONB  NOT NULL CHECK (jsonb_typeof(old_settings) = 'object'),
    new_settings        JSONB  NOT NULL CHECK (jsonb_typeof(new_settings) = 'object'),
    reason              TEXT   NOT NULL CHECK (btrim(reason) <> ''),
    -- The transaction that made the edit; the lock trigger matches on it.
    txid                BIGINT NOT NULL DEFAULT txid_current(),
    occurred_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT definition_settings_edit_actor
        CHECK ((edit_kind = 'admin_edit') = (actor_principal_idx IS NOT NULL)),
    CONSTRAINT definition_settings_edit_changes CHECK (old_settings <> new_settings)
);

CREATE INDEX definition_settings_edit_definition
    ON qiita.definition_settings_edit (definition_kind, definition_ref, txid);

COMMENT ON TABLE qiita.definition_settings_edit IS
    'Every change to a mask, alignment or processing''s locked settings: who, when, '
    'the old and new values and why. Append-only (UPDATE and DELETE refused). Written '
    'by qiita.edit_definition_settings (admin_edit) and by the lock trigger for the '
    'mask adapter-hash re-key (adapter_hash_rekey, no actor).';
COMMENT ON COLUMN qiita.definition_settings_edit.definition_kind IS
    '''mask'' | ''alignment'' | ''processing'' (TEXT + CHECK). Python twin: '
    'qiita_common.models.DefinitionKind.';

CREATE FUNCTION qiita.tg_definition_settings_edit_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'qiita.definition_settings_edit is append-only (% refused)', TG_OP;
END;
$$;

CREATE TRIGGER definition_settings_edit_no_update
    BEFORE UPDATE ON qiita.definition_settings_edit
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_definition_settings_edit_immutable();
CREATE TRIGGER definition_settings_edit_no_delete
    BEFORE DELETE ON qiita.definition_settings_edit
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_definition_settings_edit_immutable();

-- An admin edit names a system_admin.
CREATE FUNCTION qiita.tg_definition_settings_edit_actor() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.edit_kind = 'admin_edit' AND NOT EXISTS (
        SELECT 1 FROM qiita.principal
         WHERE idx = NEW.actor_principal_idx AND system_role = 'system_admin'
    ) THEN
        RAISE EXCEPTION 'principal % is not a system_admin; only a system_admin edits a'
            ' definition''s settings', NEW.actor_principal_idx
            USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER definition_settings_edit_actor
    BEFORE INSERT ON qiita.definition_settings_edit
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_definition_settings_edit_actor();

-- ---------------------------------------------------------------------------
-- The locked columns, and their values as an object
-- ---------------------------------------------------------------------------
CREATE FUNCTION qiita.definition_settings_columns(p_kind TEXT) RETURNS TEXT[]
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE p_kind
        WHEN 'mask' THEN ARRAY['params', 'params_hash', 'filter_workflow',
                               'filter_version', 'adapter_hash_scheme']
        WHEN 'alignment' THEN ARRAY['params', 'params_hash']
        WHEN 'processing' THEN ARRAY['params', 'params_hash', 'workflow', 'version']
    END
$$;

CREATE FUNCTION qiita.definition_settings_of(p_kind TEXT, p_row JSONB) RETURNS JSONB
LANGUAGE sql IMMUTABLE AS $$
    SELECT jsonb_object_agg(c, p_row -> c)
      FROM unnest(qiita.definition_settings_columns(p_kind)) AS c
$$;

-- ---------------------------------------------------------------------------
-- edit_definition_settings: the one path for an admin edit
-- ---------------------------------------------------------------------------
-- `p_patch` names locked columns only, with their new values as to_jsonb writes
-- them (params_hash as '\x<hex>'). The caller computes params_hash from params
-- (`qiita_common.hashing.canonical_params_hash`); `qiita-admin definition
-- edit-settings` does. Records the edit, then makes it, in the caller's
-- transaction. Returns the audit row's id.
CREATE FUNCTION qiita.edit_definition_settings(
    p_kind TEXT, p_ref BIGINT, p_patch JSONB, p_actor BIGINT, p_reason TEXT
) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_table  TEXT;
    v_id_col TEXT;
    v_cols   TEXT[] := qiita.definition_settings_columns(p_kind);
    v_old    JSONB;
    v_new    JSONB;
    v_edit   BIGINT;
BEGIN
    CASE p_kind
        WHEN 'mask' THEN v_table := 'qiita.mask_definition'; v_id_col := 'mask_idx';
        WHEN 'alignment' THEN v_table := 'qiita.alignment_definition';
                              v_id_col := 'alignment_idx';
        WHEN 'processing' THEN v_table := 'qiita.processing'; v_id_col := 'processing_idx';
        ELSE RAISE EXCEPTION 'unknown definition kind %', p_kind USING ERRCODE = '22023';
    END CASE;
    IF jsonb_typeof(p_patch) IS DISTINCT FROM 'object' OR EXISTS (
        SELECT 1 FROM jsonb_object_keys(p_patch) AS k WHERE k <> ALL (v_cols)
    ) THEN
        RAISE EXCEPTION 'a settings edit of a % names only %', p_kind, v_cols
            USING ERRCODE = '22023';
    END IF;
    EXECUTE format('SELECT to_jsonb(t) FROM %s t WHERE %I = $1 FOR UPDATE', v_table, v_id_col)
        INTO v_old USING p_ref;
    IF v_old IS NULL THEN
        RAISE EXCEPTION '% % not found', p_kind, p_ref USING ERRCODE = 'P0002';
    END IF;
    -- Through the row type, so the new values serialize as the trigger will read them.
    EXECUTE format('SELECT to_jsonb(jsonb_populate_record(NULL::%s, $1))', v_table)
        INTO v_new USING v_old || p_patch;
    INSERT INTO qiita.definition_settings_edit
        (definition_kind, definition_ref, edit_kind, actor_principal_idx,
         old_settings, new_settings, reason)
    VALUES (p_kind, p_ref, 'admin_edit', p_actor, qiita.definition_settings_of(p_kind, v_old),
            qiita.definition_settings_of(p_kind, v_new), p_reason)
    RETURNING definition_settings_edit_idx INTO v_edit;
    EXECUTE format(
        'UPDATE %s SET (%s) = (SELECT %s FROM jsonb_populate_record(NULL::%s, $1)) WHERE %I = $2',
        v_table,
        (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_cols) AS c),
        (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_cols) AS c),
        v_table, v_id_col
    ) USING v_new, p_ref;
    RETURN v_edit;
END;
$$;

COMMENT ON FUNCTION qiita.edit_definition_settings(TEXT, BIGINT, JSONB, BIGINT, TEXT) IS
    'The admin edit of a definition''s locked settings: records it in '
    'qiita.definition_settings_edit (actor must be a system_admin, reason required), '
    'then updates the row. The route PATCH /admin/definition-settings/{kind}/{idx} '
    'calls it after computing params_hash and checking the pairs.';

-- ---------------------------------------------------------------------------
-- The lock
-- ---------------------------------------------------------------------------
-- TG_ARGV[0] is the kind, TG_ARGV[1] the id column.
CREATE FUNCTION qiita.lock_definition_settings() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    kind  TEXT   := TG_ARGV[0];
    ref   BIGINT := (to_jsonb(OLD) ->> TG_ARGV[1])::bigint;
    old_s JSONB  := qiita.definition_settings_of(TG_ARGV[0], to_jsonb(OLD));
    new_s JSONB  := qiita.definition_settings_of(TG_ARGV[0], to_jsonb(NEW));
    pairs TEXT;
BEGIN
    IF old_s = new_s THEN
        RETURN NEW;
    END IF;
    IF kind = 'mask'
       AND old_s ->> 'adapter_hash_scheme' IS NULL
       AND new_s ->> 'adapter_hash_scheme' = 'sequence_hash_v1'
       AND old_s -> 'filter_workflow' = new_s -> 'filter_workflow'
       AND old_s -> 'filter_version' = new_s -> 'filter_version' THEN
        INSERT INTO qiita.definition_settings_edit
            (definition_kind, definition_ref, edit_kind, old_settings, new_settings, reason)
        VALUES (kind, ref, 'adapter_hash_rekey', old_s, new_s,
                'adapter_set_hash re-keyed onto sequence_hash_v1');
        RETURN NEW;
    END IF;
    SELECT string_agg(definition_pair_idx::text, ', ' ORDER BY definition_pair_idx)
      INTO pairs
      FROM qiita.definition_pair
     WHERE definition_kind = kind
       AND verdict = 'similar'
       AND detached_at IS NULL
       AND ref IN (low_definition_ref, high_definition_ref);
    IF pairs IS NOT NULL THEN
        RAISE EXCEPTION
            '% % is named by similar definition pair(s) %; revoke first',
            TG_TABLE_NAME, ref, pairs
            USING ERRCODE = '55006';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM qiita.definition_settings_edit
         WHERE definition_kind = kind AND definition_ref = ref
           AND txid = txid_current() AND edit_kind = 'admin_edit'
           AND old_settings = old_s AND new_settings = new_s
    ) THEN
        RAISE EXCEPTION
            'the settings of % % are locked after its mint; a system_admin edits them'
            ' with `qiita-admin definition edit-settings`, which records the edit',
            TG_TABLE_NAME, ref
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION qiita.lock_definition_settings() IS
    'BEFORE UPDATE on mask_definition, alignment_definition and processing: refuses a '
    'change to the locked settings (qiita.definition_settings_columns) unless this '
    'transaction recorded the same change in qiita.definition_settings_edit '
    '(SQLSTATE 55000), and refuses any change while a live similar pair names the row '
    '(55006). Passes the mask adapter-hash re-key, recording it.';

CREATE TRIGGER mask_definition_lock_settings
    BEFORE UPDATE ON qiita.mask_definition
    FOR EACH ROW EXECUTE FUNCTION qiita.lock_definition_settings('mask', 'mask_idx');
CREATE TRIGGER alignment_definition_lock_settings
    BEFORE UPDATE ON qiita.alignment_definition
    FOR EACH ROW EXECUTE FUNCTION qiita.lock_definition_settings('alignment', 'alignment_idx');
CREATE TRIGGER processing_lock_settings
    BEFORE UPDATE ON qiita.processing
    FOR EACH ROW EXECUTE FUNCTION qiita.lock_definition_settings('processing', 'processing_idx');


-- migrate:down

DROP TRIGGER IF EXISTS processing_lock_settings ON qiita.processing;
DROP TRIGGER IF EXISTS alignment_definition_lock_settings ON qiita.alignment_definition;
DROP TRIGGER IF EXISTS mask_definition_lock_settings ON qiita.mask_definition;
DROP FUNCTION IF EXISTS qiita.lock_definition_settings();
DROP FUNCTION IF EXISTS qiita.edit_definition_settings(TEXT, BIGINT, JSONB, BIGINT, TEXT);
DROP FUNCTION IF EXISTS qiita.definition_settings_of(TEXT, JSONB);
DROP FUNCTION IF EXISTS qiita.definition_settings_columns(TEXT);
DROP TABLE IF EXISTS qiita.definition_settings_edit;
DROP FUNCTION IF EXISTS qiita.tg_definition_settings_edit_actor();
DROP FUNCTION IF EXISTS qiita.tg_definition_settings_edit_immutable();
