-- Per-prep_sample QC check results and the verdict view over them.

-- migrate:up

-- The derived-row FKs cascade so these rows never block a delete; without its ticket a
-- row has no provenance.
CREATE TABLE qiita.prep_sample_qc_check (
    idx                 BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    prep_sample_idx     BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE CASCADE,
    mask_idx            BIGINT REFERENCES qiita.mask_definition(mask_idx) ON DELETE CASCADE,
    alignment_idx       BIGINT REFERENCES qiita.alignment_definition(alignment_idx)
                            ON DELETE CASCADE,
    "check"             TEXT NOT NULL,
    severity            TEXT NOT NULL,
    reason              TEXT,
    measured_value      DOUBLE PRECISION,
    threshold           DOUBLE PRECISION,
    work_ticket_idx     BIGINT NOT NULL
                            REFERENCES qiita.work_ticket(work_ticket_idx) ON DELETE CASCADE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    state               TEXT NOT NULL DEFAULT 'current',
    invalidated_at      TIMESTAMPTZ,
    invalidated_by_idx  BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    invalidation_reason TEXT,

    CONSTRAINT prep_sample_qc_check_check_values CHECK ("check" IN (
        'data_type', 'composition', 'host_fraction', 'read_retention',
        'amplicon_region', 'orientation', 'source_tracker', 'classifier'
    )),
    CONSTRAINT prep_sample_qc_check_severity_values CHECK (
        severity IN ('info', 'warning', 'fail')
    ),
    CONSTRAINT prep_sample_qc_check_state_values CHECK (state IN ('current', 'invalidated')),
    CONSTRAINT prep_sample_qc_check_reason_format CHECK (reason ~ '^[a-z][a-z0-9_]*$'),
    CONSTRAINT prep_sample_qc_check_one_producer CHECK (num_nonnulls(mask_idx, alignment_idx) <= 1),
    CONSTRAINT prep_sample_qc_check_invalidation_fields CHECK (
        (state = 'current'
            AND num_nonnulls(invalidated_at, invalidated_by_idx, invalidation_reason) = 0)
        OR (state = 'invalidated'
            AND num_nonnulls(invalidated_at, invalidated_by_idx, invalidation_reason) = 3)
    ),
    CONSTRAINT prep_sample_qc_check_invalidation_reason_nonblank CHECK (
        invalidation_reason IS NULL OR btrim(invalidation_reason) <> ''
    )
);

-- NULLS NOT DISTINCT so a NULL producer still collides within (prep_sample, "check").
CREATE UNIQUE INDEX prep_sample_qc_check_key
    ON qiita.prep_sample_qc_check (prep_sample_idx, "check", mask_idx, alignment_idx)
    NULLS NOT DISTINCT;

COMMENT ON TABLE qiita.prep_sample_qc_check IS
    'One QC check result per (prep_sample, check, producer). Python twins: '
    'qiita_common.models.QcCheck, QcSeverity, QcCheckState; reason codes live in '
    'qiita_common/data/qc_reasons.yml.';

CREATE VIEW qiita.qc_verdict AS
SELECT DISTINCT ON (c.prep_sample_idx)
       c.prep_sample_idx, c.severity, c."check"
  FROM qiita.prep_sample_qc_check c
  LEFT JOIN qiita.mask_definition md ON md.mask_idx = c.mask_idx
  LEFT JOIN qiita.mask_sample ms
    ON ms.mask_idx = c.mask_idx AND ms.prep_sample_idx = c.prep_sample_idx
  LEFT JOIN qiita.alignment_sample als
    ON als.alignment_idx = c.alignment_idx AND als.prep_sample_idx = c.prep_sample_idx
 WHERE c.state = 'current'
   AND (c.mask_idx IS NULL OR (md.status = 'active' AND ms.state = 'completed'))
   AND (c.alignment_idx IS NULL OR als.state = 'completed')
 ORDER BY c.prep_sample_idx,
          CASE c.severity WHEN 'fail' THEN 3 WHEN 'warning' THEN 2 ELSE 1 END DESC,
          c."check", c.idx;

COMMENT ON VIEW qiita.qc_verdict IS
    'Worst severity per prep_sample among the rows that count: current, for a mask row '
    'an active mask with a completed mask_sample, and for an alignment row a completed '
    'alignment_sample. Python twin: qiita_common.models.QcVerdict.';

-- migrate:down

DROP VIEW qiita.qc_verdict;
DROP TABLE qiita.prep_sample_qc_check;
