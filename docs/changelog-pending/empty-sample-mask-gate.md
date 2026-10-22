### Fixed

- **A sample whose reads came back empty at ingest no longer blocks its pool's masked-read
  export.** A BAM or FASTQ with no reads, or an empty well on an Illumina plate, left the
  sample with no masked-complete record under the pool's mask. Its `read-mask` ticket
  failed with "no stored reads … must be ingested", and `qiita-admin masked-read-export`
  refused the whole pool. Such a sample is now recorded as masked with an empty pass-set,
  zero read counts, and a zero per SynDNA insert, so the export writes an empty file for
  it. `read-mask` and `fastq-to-parquet` end its ticket `no_data`, and the block plan
  (`submit-block-mask-pool`) records it without tiling it. A sample counts as empty only
  when an ingest record shows it was read: its `bam-to-parquet` or `fastq-to-parquet`
  ticket ended `no_data`, or a finished `bcl-convert` ticket lists it. Without that record
  the `read-mask` ticket still fails, now with a message naming the three ingests.

### Changed

- **The block-mask plan response reports empty samples.** `POST …/block-mask-plan`
  returns two new counts: `samples_completed_empty` (samples ingest found empty, recorded
  as masked) and `samples_skipped_invalidated` (empty samples whose withdrawn mask record
  was left alone). `samples_skipped_no_reads` now counts only samples with no reads and
  no ingest record.
- **A `read-mask` ticket that fails for want of reads now carries its `mask_idx`**, so
  `qiita mask samples` lists the sample instead of omitting it.
