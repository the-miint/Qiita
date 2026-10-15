### Fixed

- **An ENA outage no longer fails an ENA read download on its first attempt.**
  `ingest_ena_reads` now retries a fetch that fails with an HTTP 5xx or 429 from ENA or its
  file server, with ENA's "Not all results may have been written" answer, or with miint's
  "every run failed" error for a run that broke mid-download. None of these carries a
  network word, so each was classified as bad input and failed the ticket at once. A 404
  still fails at once. From duckdb-miint#312 on, miint raises these errors for a scan of
  one run instead of skipping the run with a warning; the step handles either build.
