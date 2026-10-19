### Fixed

- **An ENA outage no longer fails an ENA read download on its first attempt (#715).**
  `ingest_ena_reads` now retries a download that fails because ENA's metadata API, which
  looks up the run's files, answers with an HTTP 5xx, 408 or 429. On every miint build,
  such a failure used to end the ticket with failure_type `permanent` at once.

### Changed

- **`ingest_ena_reads` is ready for a miint build that raises for a run it could not
  read (#715).** From duckdb-miint#312 on, miint raises instead of skipping the run with a
  warning. With such a build, a 5xx, 408 or 429 from ENA's file server and a run that
  broke mid-download are retried, as they were when miint skipped the run, and a run
  whose file is missing (404) fails at once instead of using up its retries. With
  duckdb-miint#310, ENA's "Not all results may have been written" answer is retried too.
