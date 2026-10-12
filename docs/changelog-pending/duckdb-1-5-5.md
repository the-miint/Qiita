### Changed

- **DuckDB 1.5.4 → 1.5.5 across every component, and every DuckDB pin is now exact
  (#651).** The team miint mirror now builds against DuckDB 1.5.5 (its 1.5.4 builds stopped
  updating on Sep 11), so the data-plane crate (`=1.10505.0`), the four Python components
  (`duckdb==1.5.5`) and their locks, the CI libduckdb default, the CLI the lake scripts
  require, and the long-read-assembly `assemble`/`checkm` images move together. The Python
  pins were floors and the crate a caret range, so a fresh resolve could land on DuckDB
  1.5.6, for which the mirror has no miint build. `test_duckdb_version_sync` now also holds
  the deploy CLI version, and every tracked `pyproject.toml` (any dependency table) and
  `uv.lock`, to the crate. The lake scripts now refuse a DuckDB CLI of any other version.
  The long-read-assembly `checkm` image, which this bump rebuilds, now pins CheckM, pplacer,
  hmmer and prodigal, so the rebuild cannot re-resolve them. The DuckLake extension
  DuckDB 1.5.5 installs moves `d318a545` → `d8a1881e`: bug fixes, and a catalog created
  under 1.5.4 keeps its schema and version.

### Fixed

- **A DoGet whose query fails partway through now ends in an error, not a clean end of
  stream (#651).** The data plane read its streaming result with the `duckdb` crate's
  Arrow iterator, which could not report a failed chunk fetch, so a client received a
  truncated table that looked complete. It now fetches with the crate's fallible `step`
  (public as of `duckdb` 1.10505.0), and the failure reaches the client as the stream's
  final item; a panic in the producer now does the same. The zero-row schema probe each
  DoGet ran first is gone too: the streaming result now reports its own schema before the
  first fetch.
