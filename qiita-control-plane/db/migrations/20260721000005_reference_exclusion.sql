-- migrate:up

-- =============================================================================
-- REFERENCE EXCLUSION (global blocklist of bad genome_idx / feature_idx)
-- =============================================================================
-- A curated blocklist of reference entities that must be excluded from
-- downstream consumption (alignment / taxonomy feature tables) without deleting
-- the underlying rows or rebuilding aligner indexes. Each row blocks EXACTLY ONE
-- of genome_idx / feature_idx (the num_nonnulls CHECK enforces it).
--
-- GLOBAL, not reference-scoped: there is deliberately no reference_idx column.
-- A block applies wherever the entity appears, so future references that load a
-- blocked genome inherit the block automatically. Enforcement is a query-time
-- anti-join in the data plane over the RESOLVED feature set (a genome block
-- expands to all its features via feature_genome), materialized there by the
-- control plane; see qiita_control_plane.repositories.reference_exclusion.
--
-- `reason` is plain TEXT (no Postgres ENUM twin, mirroring reference.status /
-- reference.kind) — free-form operator-supplied provenance, out of enum-parity
-- scope; bounded 1..2000 chars by CHECK (matched by the Pydantic model).

-- The blocklist is a DURABLE CURATORIAL RECORD, by two deliberate choices:
--
-- 1. genome_idx / feature_idx are PLAIN BIGINT, NOT foreign keys, so a block
--    SURVIVES deletion of its target entity. The only path that hard-deletes a
--    genome/feature is delete_reference_cascade's orphan-GC, which fires when the
--    LAST reference holding an entity is deleted. A curator who blocked something
--    expects it to STAY blocked, so rather than cascade the block away (or block
--    the delete with an interlock) the row persists; feature_idx is
--    content-hash-stable and genome_idx is (source, source_id)-stable, so a later
--    re-ingest of the same entity reuses the same idx and the surviving block
--    re-attaches automatically. The add route existence-checks the target itself
--    (there is no FK to 404 an unknown idx). excluded_by_idx / unblocked_by_idx
--    DO reference qiita.principal (ON DELETE RESTRICT): principals are never
--    hard-deleted (only disabled / retired), so the audit trail can't dangle.
--
-- 2. Unblocking is a SOFT delete (unblocked_at / unblocked_by_idx), never a row
--    DELETE, so who blocked/unblocked and why stays queryable. An ACTIVE block is
--    unblocked_at IS NULL; enforcement + the query endpoint consider only active
--    rows. Re-blocking an unblocked entity inserts a NEW active row, so each
--    block/unblock cycle is one durable record.
CREATE TABLE qiita.reference_exclusion (
    reference_exclusion_idx BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    genome_idx       BIGINT,
    feature_idx      BIGINT,
    reason           TEXT        NOT NULL,
    excluded_by_idx  BIGINT      NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    excluded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Soft-delete (unblock): both set together or both NULL. NULL unblocked_at =
    -- the block is active.
    unblocked_at     TIMESTAMPTZ,
    unblocked_by_idx BIGINT      REFERENCES qiita.principal(idx) ON DELETE RESTRICT,

    -- Exactly one target per row.
    CONSTRAINT reference_exclusion_exactly_one_target
        CHECK (num_nonnulls(genome_idx, feature_idx) = 1),
    CONSTRAINT reference_exclusion_reason_len
        CHECK (char_length(reason) BETWEEN 1 AND 2000),
    CONSTRAINT reference_exclusion_unblock_consistent
        CHECK ((unblocked_at IS NULL) = (unblocked_by_idx IS NULL))
);

-- One ACTIVE block per entity (partial uniques scoped to unblocked_at IS NULL so
-- ON CONFLICT DO NOTHING makes a re-block idempotent, while historical
-- soft-deleted rows can accumulate). Also serve the resolution lookups by target.
CREATE UNIQUE INDEX reference_exclusion_genome_active_uniq
    ON qiita.reference_exclusion (genome_idx)
    WHERE genome_idx IS NOT NULL AND unblocked_at IS NULL;
CREATE UNIQUE INDEX reference_exclusion_feature_active_uniq
    ON qiita.reference_exclusion (feature_idx)
    WHERE feature_idx IS NOT NULL AND unblocked_at IS NULL;

-- The `--` block above isn't reachable from \d+; pin the two load-bearing
-- invariants where an operator inspecting the live schema will see them.
COMMENT ON TABLE qiita.reference_exclusion IS
    'Curated GLOBAL blocklist of bad genome_idx/feature_idx, masked from consumption '
    '(the alignment/taxonomy _visible anti-join views) without deleting rows or '
    'rebuilding indexes. Targets are NOT foreign keys, so a block survives entity '
    'deletion and re-attaches on re-ingest (idx is content/natural-key stable). '
    'Unblock is a soft delete (unblocked_at); active block = unblocked_at IS NULL. '
    'See qiita_control_plane.repositories.reference_exclusion.';
COMMENT ON CONSTRAINT reference_exclusion_exactly_one_target ON qiita.reference_exclusion IS
    'Each block targets exactly one of genome_idx / feature_idx; a genome block '
    'expands to all its features via feature_genome at resolve time.';

-- migrate:down

DROP TABLE IF EXISTS qiita.reference_exclusion;
