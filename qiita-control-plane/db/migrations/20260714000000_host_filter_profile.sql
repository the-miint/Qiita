-- migrate:up
-- =============================================================================
-- HOST FILTER PROFILE
-- =============================================================================
-- The config layer that resolves a host ORGANISM to a host reference BUILD.
--
-- The two facts are deliberately kept apart. Which organism a sample came from
-- is a property of the sample and lives in its metadata (the `host_taxon_id`
-- biosample global field, terminology-typed against NCBI Taxonomy). Which
-- reference build we deplete against is NOT a property of the sample — it is a
-- submission-time choice that changes every time we rebuild the host DB, so
-- storing it on the sample row would freeze a sample against a build that is no
-- longer the one we would pick today. This table is the join between them:
-- (organism, platform) -> build.
--
-- Platform participates in the key because the STAGES are chosen per platform,
-- not per organism: the same host is depleted differently depending on how it
-- was sequenced. So the same host needs a row per platform.
--
-- Empty at creation. The live rows are seeded out-of-band by the operator (see
-- DEPLOY_CHECKLIST.md) because the reference_idx values they point at exist
-- only on the deploy — a migration INSERT would have nothing to reference in a
-- fresh test database.
CREATE TABLE qiita.host_filter_profile (
    idx                    BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    -- The host organism this profile depletes. An NCBI Taxonomy term (e.g. 9606).
    host_term_idx          BIGINT NOT NULL REFERENCES qiita.terminology_term(idx) ON DELETE RESTRICT,
    platform               qiita.platform NOT NULL,
    -- Stage 1, required: the host DB carrying a rype .ryxdi index. A profile row
    -- MEANS "filter this host", so there is always something to filter against —
    -- which is why this is NOT NULL and carries no "requires" CHECK.
    --
    -- NOT NULL encodes "every profile we run today starts with rype", which is
    -- true of the platforms we actually filter (illumina, pacbio_smrt). It is an
    -- open question whether rype is usable on a high-indel platform such as ONT;
    -- if it is not, an ONT profile would have to be minimap2-only, and THIS is
    -- the constraint that would need relaxing (rype nullable, plus a CHECK that
    -- at least one stage is present). Deliberately not pre-emptively relaxed —
    -- no ONT profile exists to design against.
    rype_reference_idx     BIGINT NOT NULL REFERENCES qiita.reference(reference_idx) ON DELETE RESTRICT,
    -- Stage 2, optional: a minimap2 .mmi. NULL means this profile has no second
    -- stage and stops after rype.
    --
    -- Deliberately NOT constrained against platform. Which stages a given
    -- (host, platform) wants is an ASSAY decision, and the schema should not
    -- freeze today's answer: encoding the current pairing as a CHECK would make
    -- revisiting it a migration. It is NOT a claim that any aligner does or does
    -- not work on any read type — that is a question for the assay owner and for
    -- measurement, not for this column comment.
    minimap2_reference_idx BIGINT REFERENCES qiita.reference(reference_idx) ON DELETE RESTRICT,
    created_by_idx         BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One active profile per (host, platform). A host-DB rebuild UPDATEs the
    -- reference idx on the existing row rather than inserting a second one, so
    -- the resolver's lookup is unambiguous by construction and never has to
    -- pick a winner among competing rows.
    CONSTRAINT host_filter_profile_host_platform_unique UNIQUE (host_term_idx, platform)
);

COMMENT ON TABLE qiita.host_filter_profile IS
    'Maps a host taxon + sequencing platform to the reference build(s) used for '
    'host-read depletion. The organism is biosample metadata (host_taxon_id); this '
    'table is the config layer resolving it to a reference at submission time, so '
    'a host-DB rebuild repoints existing samples without rewriting them. Stage 1 '
    '(rype) is required; stage 2 (minimap2) is optional and NULL means the profile '
    'stops after stage 1. Seeded out-of-band by the operator: the reference_idx '
    'values it points at exist only on a live deploy.';

-- migrate:down
DROP TABLE qiita.host_filter_profile;
