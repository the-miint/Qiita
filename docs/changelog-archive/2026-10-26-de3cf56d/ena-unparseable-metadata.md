### Fixed

- **An ENA sample attribute value a numeric field cannot hold no longer fails the run (#717).** A
  latitude or longitude written with its hemisphere after it (`39N`, `12.5 W`) is signed
  and stored as a number. Any other mapped value its global field's type cannot hold
  (`39 deg`) now stores `not provided` on the global field, keeps the submitted text in a
  study-local `<field> (as submitted)` field, and adds a harmonization warning. Before,
  the run failed to register, so a study whose biosamples all shared one such value failed
  outright.
- **An ENA import item's failure reason names each run failure once (#717).** When every
  run fails, the reason groups runs by cause (`50 runs (ERR0000001, ERR0000002,
  ERR0000003, …): <reason>`) instead of repeating the same sentence per run.
  Each run's own reason is still on its `ena_runs` entry. (A study with no public runs
  fails before per-run registration and still lists each run's status individually.)
- **An ENA import item that fails on an exception with no message says what it was
  (#717).** The failure reason was empty; it is now the exception's type, such as
  `TimeoutError (no message)`.
