### Changed

- **A pool with some permanently failed runs now lands the rest (#731).** When a `download-ena-study`
  ticket's only remaining failures are permanent (a format error, a run with no sequence data,
  a 404, zero reads), `ingest_ena_reads` stores every other run, and the new
  `persist-ena-run-outcomes` step records each failed run's reason and failure type on its
  `sequenced_sample`. The batch item reads `completed_partial`, and each failed run shows in
  `ena_runs` as `download_failed` with its `failure_reason` and `failure_type`.
  `qiita submit-ena-import --watch` exits 1 on a `completed_partial` item. The ticket is
  `completed`, so the submitter gets the work-ticket digest unless they opted out of processing
  emails. A pool with a transient failure still retries first, and one where no run was stored
  still fails.
- **`download-ena-study` retries a failed connection and an HTTP status 0 (#731).** Both were
  failed as permanent bad input before; they now retry like other transport errors.
