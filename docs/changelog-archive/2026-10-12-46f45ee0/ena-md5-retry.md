### Fixed

- **`ingest_ena_reads` retries an ENA md5 mismatch and a run miint skipped instead of failing permanently (#661).**
  miint documents a transfer truncated on a clean gzip member boundary as indistinguishable
  from a genuinely bad digest and advises bounded retries, so the ticket's `max_retries`
  is the backstop. A skipped run (a failed open or a mid-download failure) is retried the
  same way. A genuinely bad digest, a corrupt body or a permanent open failure such as a
  404 now fails only after the retries are used up. A run with zero reads and no warning
  still fails permanently. When several runs of a pool fail, a permanent outcome decides
  the step's kind and the reason gives the first three failed runs in full (a permanent one
  first) and lists up to 20 more by accession.
  A ticket that uses up `max_retries` on any retriable kind (`external_fetch_transient`,
  OOM, timeout) now ends `permanent` with a reason starting `retries_exhausted` (the kind
  is identified by that prefix, not a stored column) followed by the last attempt's kind
  and reason, so the notify digest emails it with its reason; before, it ended `retriable`
  and was withheld. The digest's held line now reads "held after an infrastructure
  failure": it counts only retriable tickets outside the step retry loop, which a redrive
  still heals.
