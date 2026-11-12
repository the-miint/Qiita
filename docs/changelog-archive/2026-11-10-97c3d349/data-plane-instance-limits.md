### Added

- **A per-instance memory limit for the data planes on a host (#791).**
  `QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX` in `/etc/qiita/data-plane.env`, optional, becomes
  `MemoryMax=` in a drop-in the deploy writes for each `qiita-data-plane@<port>` unit,
  together with `QIITA_DATA_PLANE_LOCAL_INSTANCES`, the number of local instances. With
  `DUCKDB_MEMORY_LIMIT` unset, each instance then sizes its pool from its own limit rather
  than the whole host's memory, and with `DUCKDB_THREADS` unset it divides the host's CPU
  budget by the local instances as well as by `DUCKLAKE_POOL_SIZE`. The deploy rejects a value that is not a positive integer
  with an optional `K`/`M`/`G`/`T` suffix, or whose total across the local instances
  exceeds the host's `MemTotal`, before installing anything, and stops if
  `data-plane.env` sets `QIITA_DATA_PLANE_LOCAL_INSTANCES` itself or sets a
  `DUCKDB_MEMORY_LIMIT` that is not written in `MiB` or `GiB`, or that, times
  `DUCKLAKE_POOL_SIZE`, exceeds the instance's limit. `preflight` reports the
  value and `verify-deploy` compares each unit's `MemoryMax` with it (`dp-memory-max`
  rows, skipped when the key is unset).

- **verify-deploy checks the catalog connection budget across every data-plane host
  (#791).** Each data-plane instance names its catalog sessions
  `qiita-data-plane:<port>:pool=<pool size>` through libpq's `application_name`, and the
  new `catalog-connections` row reads the names back from the catalog server's
  `pg_stat_activity`: it fails when the pools of every connected instance, plus the other
  client sessions open now, exceed `max_connections` less `superuser_reserved_connections`
  and `reserved_connections`, and otherwise prints the numbers and one
  `<host>:<port> pool=<n>` line per instance. The data plane's catalog role needs
  `pg_read_all_stats`; without it the row fails and names the grant. The deploy's
  wait for the data plane to go idle counts only that role's catalog sessions, so
  another role's open transaction does not hold the restart.

### Changed

- **The data plane refuses a `DUCKLAKE_CATALOG_CONNSTR` that sets `application_name`, or
  that is a `postgres://` URI (#791).** It sets its own name on every catalog connection;
  `preflight`'s `connstr/ducklake` row fails such a connection string before the restart.

- **The data plane derives its spill cap from the spill disk when the spill directory is
  set (#791).** With `DUCKDB_TEMP_DIRECTORY` set and `DUCKDB_MAX_TEMP_DIRECTORY_SIZE`
  unset, each connection's cap is the free space of the filesystem holding the directory
  at boot, divided by the local instances and `DUCKLAKE_POOL_SIZE`; below 64 MiB the data
  plane refuses to boot. Without `DUCKDB_TEMP_DIRECTORY` the cap stays 4GiB. The boot log
  line says where the cap came from.
