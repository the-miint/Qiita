-- migrate:up

-- Comment only; no schema change.
--
-- The gate now has a second writer. `align-denovo` aligns ONE prep_sample against its
-- own assembly, so there is no block cover-map to wait on and its terminal
-- `finalize-alignment-sample` flips the row directly. The old comment described the
-- block path as the only one, which would read a per-sample row as an anomaly.

COMMENT ON TABLE qiita.alignment_sample IS
    'Per-(alignment_idx, prep_sample) completion gate for alignment, written by two '
    'paths that never share an identity: bulk-block sharded alignment materializes '
    '''pending'' at plan time and flips ''completed'' at reconcile once every '
    'covering block finished, and per-sample de novo alignment materializes it when '
    'the runner mints the identity and flips it at the workflow''s terminal step. '
    'Which path a row came from is readable from the alignment_definition''s params: '
    'a block identity hashes a reference_idx and a shard set, a de novo one hashes an '
    'assembly subject. Consumers read ONLY ''completed'' samples — alignment rows are '
    'NOT 1:1 with reads, so presence of rows must never be read as "done". Twin of '
    'qiita.mask_sample.';


-- migrate:down

COMMENT ON TABLE qiita.alignment_sample IS
    'Per-(alignment_idx, prep_sample) completion gate for bulk-block sharded '
    'alignment. Materialized ''pending'' at plan time, flipped ''completed'' at '
    'reconcile once every covering block finished. Consumers read ONLY '
    '''completed'' samples — alignment rows are NOT 1:1 with reads, so presence '
    'of rows must never be read as "done". Twin of qiita.mask_sample.';
