### Fixed

- **An ENA outage at resolve no longer fails the import item at once (#739).** A transient
  HTTP status (5xx, 408 or 429) from the ENA metadata reads is now retried with capped
  backoff for up to six hours per call; the item then fails naming the outage. A
  connection failure (refused, reset, timed out) retries the same way; other statuses and
  errors still fail on the first attempt.
