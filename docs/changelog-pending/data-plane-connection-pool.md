### Added

- **The data plane holds a bounded pool of DuckLake connections, and refuses work past it
  (#713).** Every DoGet and DoAction borrows one attached connection from a pool whose size
  is the number of DoGets and DoActions one data plane serves at once (`DUCKLAKE_POOL_SIZE`,
  default 8). A request that arrives with every connection in use is refused with gRPC
  `RESOURCE_EXHAUSTED` and a message naming the bound, instead of being queued. Each pooled connection carries its own `threads`, `memory_limit`, `temp_directory`
  and `max_temp_directory_size` (`DUCKDB_*`), derived at boot by detecting the process's own
  memory ceiling — cgroup v2, then cgroup v1, then `/proc/meminfo`, then a fallback — and
  dividing half of it, and the CPU allowance, by the pool size. Boot fails, naming both ways
  out, if that leaves under 512 MiB per connection — measured as the point below which the
  export DoAction's Parquet writer cannot run. The boot log names the detected ceiling,
  the source that answered, and every value in use. Each connection spills into a directory of its own,
  created under `PATH_SCRATCH/duckdb-spill` and removed with the connection: DuckDB names its
  spill files the same in every database, and two databases spilling into one shared
  directory crashed the process in three of four probe runs.

- **Concurrent uploads are bounded too (#713).** A DoPut writes Parquet without touching the
  catalog, so it borrows no pooled connection and nothing in the process capped how many ran
  at once — only what the edge admitted per client address. `FLIGHT_MAX_CONCURRENT_DOPUT`
  (default 8) is now a permit per upload, taken after the ticket verifies, and an upload past
  it is refused the same way and with the same message prefix a request past the pool is.
  Measured against a real data plane, peak RSS rose from 165 MiB at one upload to 1,103 MiB at
  sixteen with 8 MiB batches, and from 2,787 MiB at one upload to 8,834 MiB at four with
  896 MiB batches. It bounds uploads, not bytes — at the batch size the CLIs send, eight is
  ~18 GiB of decode buffers, so a host smaller than the deploy host wants it lowered.

### Changed

- **A request no longer opens its own DuckDB (#713).** All fourteen lake entry points in the
  data plane opened a fresh in-memory DuckDB and attached DuckLake per request, at DuckDB's
  defaults — which size themselves from the process's cgroup, so each concurrent
  request authorised itself against the whole host. They now take a borrowed connection.
  Reusing the attach is also most of a small request's cost: a fresh connect + attach + query
  measured 84–107 ms and about 16 catalog transactions, against 3.0–3.4 ms and about 3.7 on a
  reused attach.
- **A DoGet no longer preserves insertion order (#713).** Every pooled connection runs with
  `preserve_insertion_order` off, where each request previously opened a DuckDB at its
  default of on — so a plain scan, which used to arrive near the order its files were
  written in, now arrives in no defined order. Nothing in the repo depends on it: chunks are
  reassembled by `chunk_index`, a pair's mates share one row, and the masked-read DoGet is a
  join that was never ordered. A client outside the repo that relied on scan order needs an
  `ORDER BY` of its own.
- **A recoverable data-plane failure is classified retriable in both services (#713).** The
  conditions — the request refused for want of a free connection, the data plane unreachable
  or unable to open one, a catalog write that lost a serialization race, a signing token that
  aged out in transit — and the signatures that recognise them now live in
  `qiita_common.data_plane_errors`, shared by the control plane's pre-loop fetch classifier
  (held for an operator redrive) and the orchestrator's native-job dispatcher (the runner
  re-dispatches the step). The dispatcher previously classified none of them: pyarrow raises
  the refusal as `ArrowInvalid`, a plain `ValueError`, so an overload was recorded as
  malformed input, and raises the rest as `FlightError` subclasses, which escaped the
  dispatcher entirely and failed the step with an unstructured non-zero exit. A DoAction
  raised from the control plane's action library is still permanent.

### Fixed

- **An unsupported Arrow cast in a native job is no longer reported as an unimplemented job
  (#713).** `pyarrow.ArrowNotImplementedError` is a `NotImplementedError`, so the dispatcher's
  skeleton arm claimed it and recorded "native job not implemented" — a permanent failure
  naming a job module that ships without an `execute()`, for what is a data problem. The
  pyarrow arm now sits above it, and such a cast propagates with its traceback. Unrelated to
  the pool; found while testing the arm that classification change added.
