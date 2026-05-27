-- migrate:up

-- =============================================================================
-- BUMP IDENTITY START TO 25000 FOR STUDY AND PREP_SAMPLE
-- =============================================================================
--
-- A legacy import will insert historic qiita.study and qiita.prep_sample
-- rows carrying their original integer identifiers (all below 25000). To
-- avoid PK collisions between newly-minted rows on this deployment and
-- the incoming legacy block, fast-forward the per-column identity
-- sequences for qiita.study.idx and qiita.prep_sample.idx to 25000.
--
-- Existing rows already minted in [1, 25000) keep their idx untouched;
-- only the *next* row inserted via the normal GENERATED-ALWAYS path
-- receives 25000. RESTART moves a sequence forward only -- this migration
-- is therefore idempotent against a deployment whose sequences already
-- sit above 25000 (no rows are touched, no values are reused), but it
-- cannot be undone safely once new rows have been minted at >= 25000.
--
-- The legacy import itself must use OVERRIDING SYSTEM VALUE on its
-- INSERTs because both columns are GENERATED ALWAYS AS IDENTITY. This
-- migration only bumps the sequence high-water mark; loading the legacy
-- rows is a separate step.
--
-- Scope is deliberately tight to study + prep_sample. Adjacent tables
-- (biosample, sequenced_sample, sequencing_run, prep_protocol, the
-- per-row *_metadata / *_field_exception tables, ...) are intentionally
-- left on their existing starting points: the legacy import is expected
-- to receive freshly-minted identifiers for those rows rather than
-- preserving the originals.

ALTER TABLE qiita.study       ALTER COLUMN idx RESTART WITH 25000;
ALTER TABLE qiita.prep_sample ALTER COLUMN idx RESTART WITH 25000;


-- migrate:down

-- Intentionally no-op: RESTART cannot be unwound without risking PK
-- collisions with rows minted after the bump (a rewind to 1 would
-- collide with both the pre-bump test rows and any new rows minted at
-- >= 25000). The forward step is itself idempotent, so re-running the
-- migration after a no-op down is safe.
SELECT 1;
