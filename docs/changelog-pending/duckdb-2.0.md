### Changed

- **DuckDB 2.0.0 everywhere, with the DuckLake catalog held at format 1.0 (#801).**
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
- **Host filtering maps read pairs as native minimap2 does (#801).** miint's 2.0
  build reverse-complements mate 2 before mapping a pair under the `sr` preset
  `host_filter` uses (duckdb-miint#341). A pair can now map as one fragment when neither
  mate maps alone, so `host_filter` can count a pair as host that it did not count before.
- **A dropped ENA download connection is retried at random waits, under a per-run limit
  (#801).** DuckDB 2.0 draws each wait between retries at random, so a request's 8 retries
  wait up to about 70 s in all (about 35 s on average) rather than about a minute. It also
  limits one ENA run's retries to about 50 dropped connections, with no setting; a run
  that reaches the limit is retried as a transient fetch failure.
- **A registered file whose rows are all deleted now leaves the lake (#801).**
  DuckLake on DuckDB 2.0 drops such a file from its table, so `scripts/lake-gc.sh
  --reclaim` removes it once the snapshots that hold it expire; on 1.5.5 it stayed in
  the table, and on disk. A `register_files` replay that arrives after every row of one
  of its files was deleted is now refused (`FailedPrecondition`) instead of answered
  with the earlier registration, while that file is still at its lake path. Once lake
  GC has removed it, a replay cannot tell itself from a first call (#800).
- **A control-plane restart interrupts an in-flight `mint-features` map write
  instead of waiting for it (#801).** A restart cancels the steps that have not
  finished by its drain timeout, and the next startup re-attaches them. A
  `mint-features` or `mint-annotation-features` step cancelled while it wrote its
  feature or annotation map held the shutdown until the write finished; the write is
  now interrupted.
- **Sorted at-rest writes no longer set a row count per row group (#801).** The read
  and reference-chunk writers asked for a whole number of vectors sized to their rows'
  width, because DuckDB 1.5.5 ignored the byte cap on a sorted COPY. DuckDB 2.0 applies
  it, so their row groups are now cut by the 64 MB byte cap alone, as every other
  writer's are. On ~300 MB of 15 KB rows, write time and file size were unchanged at
  both the transfer and the at-rest compression level.
- **A miint COPY format can accept a DuckDB COPY option and ignore it (#801).**
  `ROW_GROUP_SIZE_BYTES` on `FORMAT BIOM` or `FORMAT NEWICK` used to be refused as an
  unknown option, and now does nothing (duckdb-miint#373). A name neither DuckDB nor
  miint knows is still refused.
