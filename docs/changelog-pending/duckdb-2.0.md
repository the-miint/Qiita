### Changed

- **DuckDB 2.0.0 everywhere, with the DuckLake catalog held at format 1.0 (#duckdb-2.0).**
  These move from DuckDB 1.5.5 to 2.0.0, with the duckdb-miint build the team mirror
  publishes for it:
  - the data plane (`duckdb` crate 1.20000.1, with arrow, arrow-flight and parquet 60);
  - the control plane and the compute orchestrator;
  - the integration tests;
  - the `long-read-assembly` assemble and checkm images;
  - CI's libduckdb;
  - the DuckDB CLI that `make lake-shell` and `scripts/lake-gc.sh` require.

  DuckDB 2.0's ducklake extension creates a new catalog at format 1.1. So every DuckLake
  attach — the data plane's, the lake scripts' and the integration tests' — now names
  `DUCKLAKE_VERSION '1.0'`. The existing catalog stays 1.0, a 1.1 catalog is refused
  rather than used, and a DuckDB 1.5.5 client can still read what 2.0 writes there, so
  rolling back keeps working.
- **Host filtering counts somewhat more read pairs as host (#duckdb-2.0).** miint's 2.0 build
  reverse-complements mate 2 when paired `align_minimap2` runs under the `sr` preset
  `host_filter` uses (duckdb-miint#341). Forward–reverse pairs now align as pairs, and
  some pairs that did not align before now do.
- **A miint COPY writer accepts DuckDB's own COPY options and ignores them (#duckdb-2.0).** For
  example, `ROW_GROUP_SIZE_BYTES` on `FORMAT BIOM` used to be refused as an unknown
  option and now does nothing (duckdb-miint#373). A name neither DuckDB nor miint knows
  is still refused.
