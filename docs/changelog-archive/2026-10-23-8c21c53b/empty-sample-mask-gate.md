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
  - It counts as empty only when its newest ingest shows it was read and held
    nothing: a `bam-to-parquet` or `fastq-to-parquet` ticket that ended `no_data`, or
    a finished `bcl-convert` ticket that lists it. When a newer ingest failed, was
    cancelled or has not finished, the older one does not count. Without that record
    the `read-mask` ticket still fails, or ends `no_data` unrecorded for an
    expected-empty control.
  - If reads are later stored for the prep_sample, the record is removed, so it has to
    be masked again before it can be exported. Reads stored while the record is being
    written wait for it, so the removal does not miss it. A newer ingest that fails or
    has not finished leaves the record in place.

### Changed

- **The block-mask plan response reports empty prep_samples
  (#761).** `POST …/block-mask-plan` returns two new counts:
  - `samples_completed_empty`: prep_samples that ingest found empty, recorded as
    masked.
  - `samples_skipped_invalidated`: empty prep_samples whose masking was withdrawn
    earlier and is left so.

  `samples_skipped_no_reads` now counts only prep_samples that have no reads and that
  no ingest shows empty: they have none, or their newest did not end that way. The plan answers 409 when a prep_sample
  gains reads or has a newer ingest submitted while it runs. Planning again tiles new
  reads; a prep_sample whose newer ingest has not finished counts in
  `samples_skipped_no_reads` until it does.
- **A `read-mask` ticket that fails for want of reads now carries its `mask_idx`
  (#761)**, so `qiita mask samples` lists the prep_sample
  instead of leaving it out.
