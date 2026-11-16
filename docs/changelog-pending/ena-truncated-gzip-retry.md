### Fixed

- **An ENA run whose gzip ends before its end marker is retried (#889).** `ingest_ena_reads`
  used to re-raise that error unclassified, which failed the whole step. It is now a transient
  fetch error, so the run retries. miint reports the same text whether the transfer was cut
  short or the response was not the file at all (a redirect to an empty directory, say), so a
  run whose file ENA does not serve now retries up to its retry budget (3 by default) and then
  fails `retries_exhausted` (permanent), where before it failed the step at once
  (duckdb-miint#460).
