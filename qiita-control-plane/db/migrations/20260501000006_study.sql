-- migrate:up

-- =============================================================================
-- TIER ENUM
-- =============================================================================

-- A single unified tier enum used both for user-to-study access levels
-- and for data-visibility requirements. A user's effective tier on a
-- study is determined by their study_access row (or 'public' if they
-- have no row). A value's required tier is determined by a resolution
-- chain that differs by value type (two kinds on each side):
-- globally-linked biosample metadata values terminate at
-- biosample_global_field.default_tier (2 steps); purely study-local
-- biosample metadata values fall through to study.default_tier (3 steps);
-- globally-linked prep-sample metadata values terminate at
-- prep_sample_global_field.default_tier (2 steps); purely study-local
-- prep-sample metadata values fall through to study.default_tier (3 steps).
-- The read-access check is a straightforward numeric comparison on the
-- ordering below: a user can read a value iff their effective tier on
-- the value's reachable study is >= the value's required tier. System
-- admins bypass this check entirely via the global system-admin override,
-- so they can read every value regardless of tier.
--
-- Not every value is valid in every column:
--   * study_access.access_tier cannot be 'public' (a user with no study
--     relationship simply has no row in study_access; 'public' is the
--     implicit effective tier for such users).
--   * All other columns that reference 'tier' accept any value.
--
-- Mirrored by qiita_common.models.Tier. The two value sets are kept in
-- lockstep by tests — change both in the same PR.
CREATE TYPE qiita.tier AS ENUM (
    'public',
    'viewer',
    'member',
    'admin'
);


-- =============================================================================
-- STUDIES
-- =============================================================================

CREATE TABLE qiita.study (
    idx                          BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    owner_idx                    BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    principal_investigator_idx   BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    title                        VARCHAR(500) NOT NULL,
    alias                        VARCHAR(255),
    description                  TEXT,
    abstract                     TEXT,
    funding                      VARCHAR(500),
    ebi_study_accession          VARCHAR(50),
    notes                        TEXT,
    extra_metadata               JSONB,
    parent_study_idx             BIGINT REFERENCES qiita.study(idx) ON DELETE SET NULL,
    default_tier                 qiita.tier NOT NULL DEFAULT 'member',
    created_by_idx               BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger. The timestamp
    -- is used as the ETag for optimistic-concurrency control on PATCH.
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Generated tsvector backing full-text search.
    -- The column is recomputed automatically whenever any of its source columns
    -- changes; the GIN index below makes the search a fast inverted-index lookup
    -- rather than a sequential scan. Title and alias get the highest weight ('A'),
    -- abstract and description the next ('B'), and notes/funding the lowest ('C')
    -- so that matches in the most distinctive fields rank higher in the results.
    search_vector                TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')),       'A') ||
        setweight(to_tsvector('english', coalesce(alias, '')),       'A') ||
        setweight(to_tsvector('english', coalesce(abstract, '')),    'B') ||
        setweight(to_tsvector('english', coalesce(description, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(notes, '')),       'C') ||
        setweight(to_tsvector('english', coalesce(funding, '')),     'C')
    ) STORED,

    -- Note that this by itself cannot prevent cycles
    CONSTRAINT study_no_self_parent CHECK (parent_study_idx IS NULL OR parent_study_idx <> idx)
);

CREATE INDEX study_owner_idx ON qiita.study (owner_idx);
CREATE INDEX study_pi_idx ON qiita.study (principal_investigator_idx);
CREATE INDEX study_parent_idx ON qiita.study (parent_study_idx);
CREATE INDEX study_ebi_accession_idx ON qiita.study (ebi_study_accession) WHERE ebi_study_accession IS NOT NULL;
CREATE INDEX study_search_vector_idx ON qiita.study USING GIN (search_vector);

CREATE TRIGGER study_set_updated_at
    BEFORE UPDATE ON qiita.study
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- =============================================================================
-- STUDY ACCESS (per-user permissions)
-- =============================================================================

CREATE TABLE qiita.study_access (
    idx            BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    study_idx      BIGINT NOT NULL REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    principal_idx  BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    access_tier    qiita.tier NOT NULL,
    granted_by_idx BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    granted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- NOTE on the absence of retirement columns: study_access rows are
    -- hard-deleted when access is revoked, not soft-retired. Unlike
    -- biosamples, prep samples, etc (where a retired row remains
    -- visible to the entity's owner and to admins and carries audit
    -- metadata), access grants have no lingering meaning once revoked:
    -- the only question the table answers is "does this principal
    -- currently have access to this study, and at what tier?", and a
    -- revoked grant is simply the absence of that answer. There is no
    -- "retired access" state the system needs to query. Regranting
    -- access after revocation creates a fresh row; the historical fact
    -- of the previous grant is not preserved in this table.

    -- Critical: enforces that "the access tier" for a (study, principal) pair is unambiguous.
    -- Without this, PUT-as-upsert semantics would not be safe.
    CONSTRAINT study_access_unique_per_principal UNIQUE (study_idx, principal_idx),

    -- 'public' is never a valid access_tier on a study_access row. A
    -- principal with no study relationship simply has no row here;
    -- their effective tier on the study is 'public' by virtue of the
    -- absence of a row, not by virtue of a row containing 'public' so
    -- writing 'public' here would be meaningless.
    CONSTRAINT study_access_no_public_tier CHECK (access_tier <> 'public')
);

CREATE INDEX study_access_principal_idx ON qiita.study_access (principal_idx);

COMMENT ON TABLE qiita.study_access IS
    'Per-(study, principal) access grants. A principal with no row here has '
    'effective tier ''public'' on the study by absence; presence of a row '
    'means a non-public grant at access_tier. The study_access_no_public_tier '
    'CHECK enforces the data side: ''public'' is not a valid access_tier '
    'because writing the by-absence default explicitly would be meaningless.';


-- =============================================================================
-- STUDY TAGS (controlled shared namespace of tags applied to studies)
-- =============================================================================
--
-- study_tag is a registry of tag definitions that can be applied to studies.
-- The namespace is global (one shared table across all studies) and
-- controlled (tags must be registered in this table before they can be
-- associated with a study; they cannot be free-form strings on the
-- association side). Names are canonicalized to lowercase ASCII letters,
-- digits, hyphens, and underscores, up to 100 characters, making name
-- comparison a simple string equality check (no case-folding or
-- normalization layer needed at query time). Study tags persist after all
-- their associations are removed; the registry is not automatically pruned.

CREATE TABLE qiita.study_tag (
    idx            BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Tag name, canonicalized to lowercase ASCII. The CHECK constraint
    -- enforces the character set; the UNIQUE constraint enforces that
    -- each canonical name appears at most once.
    name           VARCHAR(100) NOT NULL UNIQUE
                   CHECK (name ~ '^[a-z0-9_-]+$' AND length(name) >= 1),

    -- Optional human-readable description of what this tag means.
    description    TEXT,

    created_by_idx BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger. The
    -- API uses this timestamp as the ETag for optimistic-concurrency
    -- control on PATCH.
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE qiita.study_tag IS
    'Controlled global namespace of tags that can be applied to studies. '
    'Names are lowercase ASCII (letters, digits, hyphens, underscores), '
    'max 100 chars.';

CREATE TRIGGER study_tag_set_updated_at
    BEFORE UPDATE ON qiita.study_tag
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


CREATE TABLE qiita.study_tag_to_study (
    study_tag_idx  BIGINT NOT NULL REFERENCES qiita.study_tag(idx) ON DELETE RESTRICT,
    study_idx      BIGINT NOT NULL REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    created_by_idx BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (study_tag_idx, study_idx)
);

COMMENT ON TABLE qiita.study_tag_to_study IS
    'Association between a study_tag and a study. No retirement columns: '
    'associations are hard-deleted, not retired, because there is no '
    'audit value in preserving defunct tag/study links.';

-- Supports "find all studies with this tag" search.
CREATE INDEX study_tag_to_study_study_idx
    ON qiita.study_tag_to_study (study_idx);


-- migrate:down

DROP TABLE IF EXISTS qiita.study_tag_to_study;
DROP TABLE IF EXISTS qiita.study_tag;
DROP TABLE IF EXISTS qiita.study_access;
DROP TABLE IF EXISTS qiita.study;
DROP TYPE IF EXISTS qiita.tier;
