### Fixed

- **An ENA read download survives dropped connections (#815).** The compute orchestrator's
  miint connections now retry a failed HTTP request up to 8 times (about a minute per
  request) instead of DuckDB's default of 3, so a burst of connections dropped by ENA's
  file server no longer fails the run.
