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

-- Pin the reserved-range invariant in the catalog so it is visible from
-- `\d+ qiita.study` / `\d+ qiita.prep_sample` and from any tooling that
-- introspects column comments. The legacy importer (and any other code
-- path that writes explicit idx values via OVERRIDING SYSTEM VALUE) must
-- read this threshold from here rather than re-hardcoding 25000.

COMMENT ON COLUMN qiita.study.idx IS
    'Reserved-range identity: [1, 25000) is reserved for the one-time '
    'legacy-Qiita import (inserted with OVERRIDING SYSTEM VALUE); new '
    'rows mint at 25000 and above. Any code path that writes explicit '
    'idx values must stay below 25000 and must not collide with rows '
    'already present in the reserved band.';

COMMENT ON COLUMN qiita.prep_sample.idx IS
    'Reserved-range identity: [1, 25000) is reserved for the one-time '
    'legacy-Qiita import (inserted with OVERRIDING SYSTEM VALUE); new '
    'rows mint at 25000 and above. Any code path that writes explicit '
    'idx values must stay below 25000 and must not collide with rows '
    'already present in the reserved band.';


-- migrate:down

-- Drop the column-level documentation that the up step added.
COMMENT ON COLUMN qiita.prep_sample.idx IS NULL;
COMMENT ON COLUMN qiita.study.idx IS NULL;

-- The RESTART WITH 25000 itself is intentionally NOT unwound: rewinding
-- to 1 would collide with both the pre-bump test rows and any new rows
-- minted at >= 25000 after the up step ran. Re-running the up step is
-- safe -- RESTART moves a sequence forward only, so a sequence already
-- sitting at or above 25000 stays put.
