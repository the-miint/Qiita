### Fixed

- **An ENA run whose gzip ends before its end marker is retried (#NNN).** `ingest_ena_reads`
  used to fail the run at once as bad input; it now classifies it as a transient fetch error,
  since a cut transfer reports the same text. A run whose file ENA does not actually serve
  (a redirect to an empty directory, say) reports it too, so it now retries up to its retry
  budget (3 by default) and then fails `retries_exhausted` (permanent), where before it failed
  at once. miint cannot tell the two apart (duckdb-miint#460).
