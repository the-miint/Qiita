### Added

- **`DISPATCH_MAX_INFLIGHT_CEILING` sets the largest dispatch cap an operator can set
  at runtime (#PR).** Before, `DISPATCH_MAX_INFLIGHT` was both the cap a restart
  reverts to and the size of the Postgres pool, so `qiita-admin dispatch-cap --set`
  could not go above 12 without booting at the higher cap. The ceiling is one of 8,
  12, 16 or 24 and at least `DISPATCH_MAX_INFLIGHT`; any other value fails the boot.
  When set, it sizes the pool (48 connections at 24) and is the `max_settable` that
  `dispatch-cap` reports. Unset, the pool and `max_settable` are as before.
