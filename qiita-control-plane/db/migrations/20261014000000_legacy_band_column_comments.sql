-- migrate:up

-- Reword the reserved-range comments on study.idx, prep_sample.idx and
-- sequenced_pool.idx. They called [1, 25000) the band for "the one-time
-- legacy-Qiita import", but a system_admin now creates a study in it through
-- POST /api/v1/study with an explicit study_idx, a few legacy studies at a time.
-- The range and the rules for writing into it are unchanged, and
-- LEGACY_IDX_RESERVED_CEILING (qiita_common.models.study) must still equal the
-- start recorded here. Comments only: no row, sequence or constraint changes.

COMMENT ON COLUMN qiita.study.idx IS
    'Reserved-range identity: [1, 25000) is reserved for studies carried over '
    'from legacy Qiita, which keep their legacy-Qiita study id (inserted with '
    'OVERRIDING SYSTEM VALUE); new rows mint at 25000 and above. Any code path '
    'that writes explicit idx values must stay below 25000 and must not collide '
    'with rows already present in the reserved band.';

COMMENT ON COLUMN qiita.prep_sample.idx IS
    'Reserved-range identity: [1, 25000) is reserved for rows carried over from '
    'legacy Qiita, which keep their original id (inserted with OVERRIDING SYSTEM '
    'VALUE); new rows mint at 25000 and above. Any code path that writes '
    'explicit idx values must stay below 25000 and must not collide with rows '
    'already present in the reserved band.';

COMMENT ON COLUMN qiita.sequenced_pool.idx IS
    'Reserved-range identity: [1, 25000) is reserved for legacy-Qiita prep ids '
    '(inserted with OVERRIDING SYSTEM VALUE); new rows mint at 25000 and above. '
    'Any code path that writes explicit idx values must stay below 25000 and '
    'must not collide with rows already present in the reserved band.';


-- migrate:down

-- Restore the comments the reserved-range bump migrations wrote.

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

COMMENT ON COLUMN qiita.sequenced_pool.idx IS
    'Reserved-range identity: [1, 25000) is reserved for the one-time '
    'legacy-Qiita import (inserted with OVERRIDING SYSTEM VALUE); new '
    'rows mint at 25000 and above. Any code path that writes explicit '
    'idx values must stay below 25000 and must not collide with rows '
    'already present in the reserved band.';
