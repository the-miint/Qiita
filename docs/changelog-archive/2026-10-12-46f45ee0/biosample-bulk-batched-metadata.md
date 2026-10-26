### Changed

- **The bulk biosample import writes a batch's metadata in a few statements (#662).**
  It previously wrote each value in its own savepoint and INSERT (~1.4 ms per
  value); a 75-column sheet now imports at ~9 ms per row instead of ~106 ms. A
  batch the database rejects is written again row by row, so the error and the
  row it names are unchanged. The per-request cap rises from 15,000 to 25,000
  metadata values (~0.85 MiB, inside the gateway's default request-size limit). A
  transient DB error (deadlock, or a lock wait past the command timeout) during the
  batched write now answers the same retryable 503 the single import and phase 1 do,
  rather than a 500.
