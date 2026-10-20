### Changed

- **A transiently-failed follow-on is retried on an interval, not only at the next
  control-plane start (#707).** A new in-process sweeper (`follow_on_sweeper`) runs the
  same `reconcile_follow_ons` the startup hook does, once per
  `FOLLOW_ON_SWEEP_INTERVAL_SECONDS` (default 60), so a follow-on whose submission hit a
  5xx or a database error is resubmitted within an interval rather than waiting for a CP
  restart. It takes no advisory lock: the submit path is create-at-most-once, so an
  overlapping reconcile is harmless.
