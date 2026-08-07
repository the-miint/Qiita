-- migrate:up transaction:false
-- CREATE INDEX CONCURRENTLY cannot run inside a transaction block, and dbmate
-- sends every statement of a migration file to libpq in a single Exec that
-- behaves as one implicit transaction even under transaction:false — so this
-- up block must be EXACTLY ONE statement (the down block likewise). Same
-- reason as qiita_work_ticket_email_owed_idx.
--
-- qiita.alignment_sample's PRIMARY KEY is (alignment_idx, prep_sample_idx),
-- which serves every existing consumer: they all lead with a known
-- alignment_idx (list_incomplete_alignment_samples, the block reconcile, the
-- feature-table resolver). The pool-alignment discovery read is the first to
-- ask the opposite question — "which alignments touch THESE samples?" — with
-- no alignment_idx predicate at all, and a composite btree cannot be used on
-- its non-leading column, so that query sequential-scans the whole table.
--
-- That matters here specifically because the table grows without bound (one
-- row per (alignment config, sample), across every reference, aligner and
-- rerun) and the route is open to any authenticated user, so it is meant to be
-- hit often. prep_sample_idx leads; alignment_idx follows so
-- list_alignments_over_prep_samples can be served index-only.
CREATE INDEX CONCURRENTLY qiita_alignment_sample_prep_sample_idx
    ON qiita.alignment_sample (prep_sample_idx, alignment_idx);

-- migrate:down transaction:false
DROP INDEX CONCURRENTLY IF EXISTS qiita.qiita_alignment_sample_prep_sample_idx;
