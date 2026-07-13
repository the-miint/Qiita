-- SynDNA spike-in read count, and a corrected monotonic invariant.
--
-- Adds a FOURTH per-sequenced_sample read count. A SynDNA spike-in is added in
-- the lab: it is not a molecule from the sample, so it is NOT biological. It gets
-- its own bucket, disjoint from `biological`, so the read accounting BALANCES — a
-- spike-in read leaves `biological` and has to be counted somewhere; a bare "reads
-- that weren't QC failures" total would both hide it and corrupt `biological`.
-- (A MASKING metric. The cell-count model consumes per-insert coverage depth, not
-- this read count — see the COMMENT below.)
--
-- Like its three siblings this is a both-mates total (the `*_r1r2` convention:
-- count(*) + count(second-mate)). PacBio HiFi — the only protocol that carries
-- SynDNA today — is SINGLE-END, so on those rows the column simply equals the
-- read count. The suffix names the counting convention, not a claim that R2
-- exists, and `sequenced_sample` is one table shared by Illumina and PacBio: the
-- same column must serve an Illumina absquant run later.
--
-- The bucketing predicate also changes, in the same PR, from a blacklist
-- (`reason NOT LIKE 'qc_%'`) to a WHITELIST (`pass` + `host_*`). The blacklist
-- was fail-OPEN: every reason added since would have been counted as biological
-- by default, which is exactly how `spikein_syndna` and `twist_no_adaptor` would
-- have silently inflated it.
--
-- The monotonic CHECK from 20260622000000 is DROPPED and RE-ADDED here (never
-- edit an applied migration). `biological` and `spikein` are disjoint reason sets,
-- so their sum is bounded by `raw`; `quality_filtered` (reason='pass') remains a
-- subset of `biological`. Reads that are neither — `qc_*` and `twist_no_adaptor` —
-- count toward `raw` only.
--
-- Additive and backfill-free: the new column reads NULL on every existing row
-- until its sample is (re)processed. coalesce() in the CHECK keeps a
-- partially-populated row passing, as before.

-- migrate:up
ALTER TABLE qiita.sequenced_sample
  ADD COLUMN spikein_read_count_r1r2 BIGINT,
  ADD CONSTRAINT sequenced_sample_spikein_read_count_nonneg CHECK (
    spikein_read_count_r1r2 IS NULL OR spikein_read_count_r1r2 >= 0
  );

ALTER TABLE qiita.sequenced_sample
  DROP CONSTRAINT sequenced_sample_read_counts_monotonic;

ALTER TABLE qiita.sequenced_sample
  ADD CONSTRAINT sequenced_sample_read_counts_monotonic CHECK (
    quality_filtered_read_count_r1r2 <= biological_read_count_r1r2
    AND biological_read_count_r1r2 + coalesce(spikein_read_count_r1r2, 0)
          <= raw_read_count_r1r2
  );

COMMENT ON COLUMN qiita.sequenced_sample.spikein_read_count_r1r2 IS
  'Total SynDNA spike-in reads (R1+R2), disjoint from biological_read_count_r1r2 '
  '(a spike-in is synthetic, not a molecule from the sample). A MASKING metric: it '
  'exists so the per-stage read accounting balances, since spike-in reads are '
  'removed from biological and must be accounted somewhere. It is NOT the input to '
  'the cell-count model, which needs per-insert COVERAGE DEPTH (aligned bases / '
  'insert length), a different quantity that cannot be derived from a read count. '
  'PacBio HiFi is single-end, so on those rows this equals the read count; the '
  '_r1r2 suffix names the both-mates counting convention shared with its siblings. '
  'NULL until processed.';

-- migrate:down
ALTER TABLE qiita.sequenced_sample
  DROP CONSTRAINT sequenced_sample_read_counts_monotonic;

ALTER TABLE qiita.sequenced_sample
  ADD CONSTRAINT sequenced_sample_read_counts_monotonic CHECK (
    quality_filtered_read_count_r1r2 <= biological_read_count_r1r2
    AND biological_read_count_r1r2 <= raw_read_count_r1r2
  );

ALTER TABLE qiita.sequenced_sample
  DROP CONSTRAINT sequenced_sample_spikein_read_count_nonneg,
  DROP COLUMN spikein_read_count_r1r2;
