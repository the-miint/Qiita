-- migrate:up

-- Name the third writer of the qiita.mask_sample gate. A prep_sample whose ingest
-- stored no reads has nothing to mask, so the runner (on a masking ticket it ends
-- NO_DATA for want of reads) and the block planner record it 'completed' with an
-- empty pass-set, and minting its sequence_range later removes that row. Supersedes
-- the table COMMENT of the mask lifecycle migration, which named only the two
-- masking paths. Comment only: no row, constraint or index changes.

COMMENT ON TABLE qiita.mask_sample IS
    'Per-(mask_idx, prep_sample) gate for read masking. Written first-class by '
    'BOTH masking paths: the block path materializes ''pending'' at plan time '
    'and flips ''completed'' at reconcile; the per-sample mask-model workflows '
    '(read-mask, fastq-to-parquet) write ''completed'' at their '
    'finalize-mask-sample terminal step. A prep_sample that ingest found empty is '
    'written ''completed'' with an empty pass-set, by the runner when it ends a '
    'masking ticket NO_DATA and by the block planner; minting its sequence_range '
    'later removes that row. A completed run later judged '
    'untrustworthy is set ''invalidated'', recorded with who/when/why. Any '
    'consumer that must not read an absent, partial or withdrawn pass-set reads '
    'ONLY ''completed'' -- absence of a row means "not masked-complete", NEVER '
    '"pass". Stated as a contract, not a roster (an enumerated consumer list '
    'would only go stale); see fetch_mask_sample_state. Python twin: '
    'qiita_common.models.MaskSampleState.';

-- migrate:down

COMMENT ON TABLE qiita.mask_sample IS
    'Per-(mask_idx, prep_sample) gate for read masking. Written first-class by '
    'BOTH masking paths: the block path materializes ''pending'' at plan time '
    'and flips ''completed'' at reconcile; the per-sample mask-model workflows '
    '(read-mask, fastq-to-parquet) write ''completed'' at their '
    'finalize-mask-sample terminal step. A completed run later judged '
    'untrustworthy is set ''invalidated'', recorded with who/when/why. Any '
    'consumer that must not read an absent, partial or withdrawn pass-set reads '
    'ONLY ''completed'' -- absence of a row means "not masked-complete", NEVER '
    '"pass". Stated as a contract, not a roster (an enumerated consumer list '
    'would only go stale); see fetch_mask_sample_state. Python twin: '
    'qiita_common.models.MaskSampleState.';
