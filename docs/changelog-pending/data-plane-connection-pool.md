### Added

- **The data plane holds a bounded pool of DuckLake connections, and refuses work past it
  (#713).** Every DoGet and DoAction borrows one attached connection from a pool whose size
  is the number of them one data plane serves at once (`DUCKLAKE_POOL_SIZE`, default 8); a
  DoPut writes Parquet without touching the catalog and is not bounded by it. A request that arrives with every connection in use is
  refused with gRPC `RESOURCE_EXHAUSTED` and a message naming the bound, instead of being
  queued. Each pooled connection carries its own `threads`, `memory_limit`, `temp_directory`
  and `max_temp_directory_size` (`DUCKDB_*`), derived at boot by detecting the process's own
  memory ceiling — cgroup v2, then cgroup v1, then `/proc/meminfo`, then a fallback — and
  dividing half of it, and the CPU allowance, by the pool size. Boot fails, naming both ways
  out, if that leaves under 256 MiB per connection. The boot log names the detected ceiling,
  the source that answered, and every value in use.

### Changed

- **A request no longer opens its own DuckDB (#713).** All fourteen lake entry points in the
  data plane opened a fresh in-memory DuckDB and attached DuckLake per request, at DuckDB's
  defaults — which size themselves from the process's cgroup, so each concurrent
  request authorised itself against the whole host. They now take a borrowed connection.
  Reusing the attach is also most of a small request's cost: a fresh connect + attach + query
  measured 84–107 ms and about 16 catalog transactions, against 3.0–3.4 ms and about 3.7 on a
  reused attach.
- **A busy data plane is classified retriable, not bad input (#713).** A refusal is filed as
  `data_plane_transient` in two places: the control plane's data-plane fetch classifier, where
  it is held for an operator redrive, and the orchestrator's native-job dispatcher, where the
  runner re-dispatches the step. Both match on the refusal's message via
  `qiita_common.data_plane_errors`, because pyarrow surfaces that gRPC status as
  `ArrowInvalid` — a `ValueError`, not a `FlightError` subclass — so a dispatcher that
  classified by exception type recorded an overload as malformed input. A DoAction raised
  from the control plane's action library is still permanent.
