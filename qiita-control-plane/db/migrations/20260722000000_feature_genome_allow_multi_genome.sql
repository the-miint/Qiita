-- migrate:up

-- Allow a feature to belong to MULTIPLE genomes. feature_idx is content-hash-
-- global (identical bytes share one feature_idx), so two organisms that carry an
-- identical mobile element (e.g. a shared plasmid) resolve to the SAME feature_idx
-- under different genome_idx. The original standalone UNIQUE(feature_idx) modelled
-- a feature as belonging to at most one genome, so the second genome's association
-- collided on load and was silently dropped (ON CONFLICT DO NOTHING) — a lossful
-- load. The composite PRIMARY KEY (feature_idx, genome_idx) already models the
-- many-to-many correctly (and its leading feature_idx keeps feature->genome
-- lookups indexed), so dropping the standalone UNIQUE is all that is needed.
--
-- NOTE (operator): references loaded before this migration stay lossful — the
-- dropped second-genome associations are NOT backfilled. RE-LOAD affected
-- references to recover shared-feature associations.

ALTER TABLE qiita.feature_genome DROP CONSTRAINT feature_genome_feature_idx_key;

-- migrate:down

-- Restores the standalone UNIQUE. This FAILS if any many-to-many row already
-- exists (a feature_idx under two genomes) — an inherent property of reverting an
-- expand: you cannot re-impose a uniqueness the data now violates. Purge the
-- duplicate associations first if a rollback is genuinely required.
ALTER TABLE qiita.feature_genome ADD CONSTRAINT feature_genome_feature_idx_key UNIQUE (feature_idx);
