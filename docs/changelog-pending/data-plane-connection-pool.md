### Added

- **The data plane holds a bounded pool of DuckLake connections, and refuses work past it
  (#NNN).** Every DoGet, DoPut and DoAction that touches the lake borrows one attached
  connection from a pool whose size is the number of requests one data plane serves at once
  (`DUCKLAKE_POOL_SIZE`, default 8). A request that arrives with every connection in use is
  refused with gRPC `RESOURCE_EXHAUSTED` and a message naming the bound, instead of being
  queued. Each pooled connection carries its own `threads`, `memory_limit`, `temp_directory`
  and `max_temp_directory_size` (`DUCKDB_*`), derived at boot by detecting the process's own
  memory ceiling — cgroup v2, then cgroup v1, then `/proc/meminfo`, then a fallback — and
  dividing half of it, and the CPU allowance, by the pool size. The boot log names the
  detected ceiling, the source that answered, and every value in use.

### Changed

- **A request no longer opens its own DuckDB (#NNN).** All fourteen lake entry points in the
  data plane opened a fresh in-memory DuckDB and attached DuckLake per request, at DuckDB's
  defaults — which size themselves from the process's cgroup, so each concurrent
  request authorised itself against the whole host. They now take a borrowed connection.
  Reusing the attach is also most of a small request's cost: a fresh connect + attach + query
  measured 84–107 ms and about 16 catalog transactions, against 3.0–3.4 ms and about 3.7 on a
  reused attach.
- **A busy data plane is classified retriable, not bad input (#NNN).** The control plane's
  data-plane fetch classifier (`runner/_upload.py`) files a `RESOURCE_EXHAUSTED` refusal as
  `DATA_PLANE_TRANSIENT`, so an operator redrive self-heals it. It matches by message because
  pyarrow surfaces that status as `ArrowInvalid` — a `ValueError`, not a `FlightError`
  subclass.
