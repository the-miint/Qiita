### Added

- **A per-instance memory limit for the data planes on a host (#TBD).**
  `QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX` in `/etc/qiita/data-plane.env`, optional, becomes
  `MemoryMax=` in a drop-in the deploy writes for each `qiita-data-plane@<port>` unit,
  together with `QIITA_DATA_PLANE_LOCAL_INSTANCES`, the number of local instances. With
  `DUCKDB_MEMORY_LIMIT` unset, each instance then sizes its pool from its own limit rather
  than the whole host's memory. The deploy rejects a value that is not a positive integer
  with an optional `K`/`M`/`G`/`T` suffix, or whose total across the local instances
  exceeds the host's `MemTotal`, before installing anything, and stops if
  `data-plane.env` sets `QIITA_DATA_PLANE_LOCAL_INSTANCES` itself or sets a
  `DUCKDB_MEMORY_LIMIT` that, times `DUCKLAKE_POOL_SIZE`, exceeds the instance's limit. `preflight` reports the
  value and `verify-deploy` compares each unit's `MemoryMax` with it (`dp-memory-max`
  rows, skipped when the key is unset).

### Changed

- **The data plane derives its spill cap from the spill disk when the spill directory is
  set (#TBD).** With `DUCKDB_TEMP_DIRECTORY` set and `DUCKDB_MAX_TEMP_DIRECTORY_SIZE`
  unset, each connection's cap is the free space of the filesystem holding the directory
  at boot, divided by the local instances and `DUCKLAKE_POOL_SIZE`; below 64 MiB the data
  plane refuses to boot. Without `DUCKDB_TEMP_DIRECTORY` the cap stays 4GiB. The boot log
  line says where the cap came from.
