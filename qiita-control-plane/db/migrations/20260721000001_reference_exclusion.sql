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
-- Blocking is reversible (delete the row) — that is the whole point versus the
-- hard DELETE /reference/{idx} cascade. Unlike read_mask there is NO completion
-- gate: absence of a row safely means "not blocked" (a curated list is complete
-- by construction, with no partial/in-flight state).
--
-- `reason` is plain TEXT (no Postgres ENUM twin, mirroring reference.status /
-- reference.kind) — free-form operator-supplied provenance, out of enum-parity
-- scope.

-- ON DELETE CASCADE on both target FKs is a DELIBERATE, accepted tradeoff. The
-- only path that hard-deletes a genome/feature is delete_reference_cascade's
-- orphan-GC (actions/reference.py), which fires when the LAST reference holding
-- an entity is deleted — i.e. the entity no longer exists in any reference, so a
-- block on it is moot. Rather than pin those rows against GC (an operator
-- interlock: "unblock before you can delete this reference"), we let the block
-- die with the entity. The "future references inherit the block" guarantee still
-- holds for every realistic case, because a re-loaded genome that still has a
-- surviving feature/genome row reuses the same idx; only the delete-last-
-- reference-then-re-ingest edge re-mints a fresh idx that must be re-blocked.
CREATE TABLE qiita.reference_exclusion (
    genome_idx      BIGINT REFERENCES qiita.genome(genome_idx)   ON DELETE CASCADE,
    feature_idx     BIGINT REFERENCES qiita.feature(feature_idx) ON DELETE CASCADE,
    reason          TEXT        NOT NULL,
    excluded_by_idx BIGINT      NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    excluded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Exactly one target per row.
    CONSTRAINT reference_exclusion_exactly_one_target
        CHECK (num_nonnulls(genome_idx, feature_idx) = 1)
);

-- One block per entity (partial uniques so ON CONFLICT DO NOTHING makes re-blocks
-- idempotent). Also serve the resolution lookups by target.
CREATE UNIQUE INDEX reference_exclusion_genome_uniq
    ON qiita.reference_exclusion (genome_idx) WHERE genome_idx IS NOT NULL;
CREATE UNIQUE INDEX reference_exclusion_feature_uniq
    ON qiita.reference_exclusion (feature_idx) WHERE feature_idx IS NOT NULL;

-- migrate:down

DROP TABLE IF EXISTS qiita.reference_exclusion;
