### Fixed

- **A prep_sample whose reads came back empty at ingest no longer blocks its pool's
  masked-read export (#761).** A BAM or FASTQ with no reads, or an
  empty well on an Illumina plate, left the prep_sample with no record that it was
  masked under the pool's mask. Its `read-mask` ticket failed with "no stored reads …
  must be ingested", and `qiita-admin masked-read-export` refused the whole pool. Such a
  prep_sample is now recorded as masked with no reads to serve, zero read counts, and a
  zero per SynDNA insert, so the export writes an empty file for it.
  - `read-mask` and `fastq-to-parquet` end its ticket `no_data`.
  - The block plan (`submit-block-mask-pool`) records it without tiling it.
  - It counts as empty only when an ingest record shows it was read: its
    `bam-to-parquet` or `fastq-to-parquet` ticket ended `no_data`, or a finished
    `bcl-convert` ticket lists it. Without that record the `read-mask` ticket still
    fails.
  - If reads are later stored for the prep_sample, the record is removed, so it has to
    be masked again before it can be exported.

### Changed

- **The block-mask plan response reports empty prep_samples
  (#761).** `POST …/block-mask-plan` returns two new counts:
  - `samples_completed_empty`: prep_samples that ingest found empty, recorded as
    masked.
  - `samples_skipped_invalidated`: empty prep_samples whose masking was withdrawn
    earlier and is left so.

  `samples_skipped_no_reads` now counts only prep_samples that have no reads and no
  ingest record. The plan answers 409 when a prep_sample gains reads while it runs;
  planning again picks them up.
- **A `read-mask` ticket that fails for want of reads now carries its `mask_idx`
  (#761)**, so `qiita mask samples` lists the prep_sample
  instead of leaving it out.
