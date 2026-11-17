### Changed

- **A pool's stored ENA runs land once transient retries run out (#NNN).** `ingest_ena_reads`
  used to fail the whole ticket when a run kept failing transiently, so the pool's good runs
  were not loaded. On the ticket's last attempt it now lands the stored runs and records the
  failed ones as `retriable`, where a permanent failure is recorded `permanent`, and the item
  reads `completed_partial`. A pool where no run was stored still fails `retries_exhausted`. A
  re-import tickets the `retriable` runs again, restricted to them; `permanent` ones are still
  left. A re-import's ticket whose runs all fail again ends FAILED, not
  `completed_partial`. A run that fails every time but is classed retriable (a gzip that ends
  early because ENA serves no file, or a file whose bytes do not match the `fastq_md5` ENA
  publishes for it; check the run on the ENA Portal) is downloaded again on each re-import.
