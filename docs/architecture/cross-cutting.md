# Wiring and Cross-cutting Structure

## Wiring Notes

- **Flight-ticket signing keypair (Ed25519, asymmetric):** The control plane holds the private seed (`FLIGHT_TICKET_SIGNING_KEY`) and signs; the data plane holds only the matching public key (`FLIGHT_TICKET_PUBLIC_KEY`) and verifies, so a data-plane compromise cannot forge tickets. Both are read from the per-service env file. Rotated by deploying a new keypair to both services and restarting them together — see [`docs/runbooks/key-rotation.md`](../runbooks/key-rotation.md). (The login-cookie HMAC secret, `LOGIN_COOKIE_SECRET_KEY`, is a separate control-plane-only key.)
- **JWKS caching:** Both services fetch AuthRocket's `/.well-known/jwks.json` on startup and refresh periodically (e.g., every 5 minutes). No per-request calls to AuthRocket.
- **nginx gRPC config:** Requires `grpc_pass` directive (not `proxy_pass`), `http2` on the listener, TLS termination. REST and gRPC routes split by path prefix or content-type.
- **slurmrestd:** Compute orchestrator authenticates to slurmrestd via SLURM JWT. All job parameters specified in JSON body (not `#SBATCH` directives). Environment variables must be explicitly listed.
- **DuckLake file registration:** `ducklake_add_data_files` registers a Parquet file by path — no data copying. Ownership transfers to DuckLake (compaction may later delete/rewrite). Schema and type validation performed at registration time. Registration failure (schema mismatch, corrupt Parquet) is an explicit FAILED state with `failed_stage=registration` and `failure_type=permanent`.
- **Service-to-service auth:** Data plane and compute orchestrator authenticate to control plane via pre-shared API keys for their respective service accounts (`data-plane`, `compute`).
- **Shared filesystem paths:** Two mounts — `/data/` (durable) and `/scratch/` (working, three-tier). Canonical layout and retention policy in [Data Storage](storage.md#data-storage). All components — control plane, data plane, SLURM jobs — access both mounts.
- **qiita-common dependency:** Both Python services depend on `qiita-common` as a path dependency in their `pyproject.toml` (e.g., `qiita-common = {path = "../qiita-common"}`). Shared Pydantic models ensure API contract consistency.
- **Data plane Unix user and file protection:** The data plane runs as the dedicated `qiita-data` system user (no login shell). SLURM jobs run as `qiita-job` and write step outputs into `/scratch/ephemeral/staging/<ticket_id>/` (intermediates) or directly into `/data/parquet/<table>/` (final-step outputs). Before exiting, each job sets `chmod 440` on its output files — owner and group read-only, no write, no world access. The data plane checks this permission as a pre-registration gate before calling `ducklake_add_data_files`; a file that is not `440` is rejected as a permanent registration failure. This ensures that once DuckLake takes ownership of a file, no compute job (or other process running as `qiita-job`) can overwrite or corrupt it. The data plane user must be in the same group as `qiita-job` (or the relevant directories must be group-readable) so the service can read files it does not own.
- **Data plane horizontal scaling:** The data plane is the read/write path for all DuckLake data. At scale, many concurrent SLURM jobs may issue large DoGet reads simultaneously, making a single data plane process a throughput bottleneck. The data plane scales horizontally because each instance is stateless with respect to request handling — it holds only a DuckDB+DuckLake connection to the shared Postgres catalog and reads Parquet files from the shared filesystem. DuckLake's concurrent read model is safe for this: multiple DuckDB instances connecting to the same Postgres catalog never block each other (readers use snapshot isolation with no row-level locking; conflicts only arise on concurrent writes, resolved via optimistic concurrency at commit time). Workers cannot bypass the data plane and read Parquet files directly for several reasons: (1) deletions are recorded as separate delete files in the DuckLake catalog — raw Parquet reads return logically deleted rows; (2) small inserts below the data inlining threshold are stored entirely within the Postgres catalog (`ducklake_inlined_data_tables`), with no Parquet file written at all; (3) snapshot visibility requires a catalog query to determine which files are live for the current consistent state; (4) compaction rewrites and deletes Parquet files under active management, making cached paths unreliable. The data plane remains the correct and only correct read path; the solution to the bottleneck is running more of them.
- **Reference ID minting flow:** Bulk reference ingestion is a multi-step pipeline: (1) SLURM hash job reads sequences via DuckDB + miint and computes MD5 hashes, writing a manifest; (2) orchestrator feeds hashes to control plane; (3) control plane does bulk dedup lookup (`features.sequence_hash` unique index, stored as Postgres `uuid`), reuses existing `feature_idx` for matches, mints new ones for novel sequences, writes membership records, returns ID mapping; (4) SLURM load job inserts sequences + taxonomy + annotations into DuckLake using assigned IDs; (5) SLURM index job builds aligner indices.
- **Alignment → reference join:** Alignment Parquet contains `feature_idx` but not `reference_idx`. To scope alignment results to a specific reference version, the query joins `reference_membership(reference_idx, feature_idx)` at query time. This join can happen entirely in the data plane (DuckLake) for analytical queries, or the control plane can provide the authorized feature set for a given reference to narrow a Flight ticket.
- **Reference filesystem paths:** Built reference data lives under `/scratch/persistent-local/references/{reference_idx}/{aligner}/` (local SSD, random-access aligner indices) or `/scratch/persistent/references/{reference_idx}/{aligner}/` (shared FS, references that don't need local-SSD random access). Built by SLURM jobs and read by alignment SLURM jobs at processing time. Processing workflow `params.json` includes the `reference_idx` to locate the correct path. If the local-SSD copy of an aligner index is missing (cluster purge), the orchestrator rebuilds it at dispatch time before the alignment job runs.
- **Phylogenetic addressing:** Internal nodes are addressed by `(reference_idx, node_index)` — scoped to a single tree, not referenced across references. A tip connects to the sequence identity layer through its own `feature_idx` column on the same DuckLake row, written at ingestion time; there is no junction table. Clade-scoped queries use a recursive CTE on `parent_index` to collect descendant tips and read `feature_idx` off them.
- **Feature deduplication:** `feature_idx` is content-addressed via MD5 hash of the sequence bytes. The SLURM ingestion job computes hashes using DuckDB's built-in `md5()` function on sequences read via miint's `read_fastx`. Hashes are fed back to the control plane through the orchestrator for bulk dedup lookup. The control plane stores hashes as Postgres `uuid` type (MD5 is exactly 128 bits = UUID-sized) with a unique B-tree index, and upserts on `sequence_hash` — if a sequence already exists, the existing `feature_idx` is reused and the new reference's membership row simply points to it.

---

## Cross-cutting structure

### Component map and ports

| Component | Language | Port | Role |
|---|---|---|---|
| `qiita-control-plane` | Python / FastAPI | 8080 | REST API, all identifier minting |
| `qiita-data-plane` | Rust / Arrow Flight (tonic) | 50051 | Bulk data I/O over gRPC |
| `qiita-compute-orchestrator` | Python / FastAPI | 8081 | SLURM job lifecycle |
| `qiita-common` | Python (path dep) | — | Shared Pydantic models, config, REST client |

nginx terminates TLS and routes: `REST → :8080`, `gRPC → :50051` (load-balanced across N data plane instances via `upstream qiita_data_plane` in `deploy/nginx/qiita.conf`).

### Identifier ownership

**All uint64 identifiers are minted exclusively by the control plane.** The data plane treats every identifier as an opaque integer. The hierarchy is:

```
biosample_idx → prep_sample_idx
```

`prep_sample.biosample_idx` is a direct FK to `qiita.biosample`. Not every CP-minted `*_idx` sits in a containment chain, so don't assume one: `study_idx` attaches through the many-to-many `prep_sample_to_study` junction, and `processing_idx` is a fleet-wide params-hash identity (`qiita.processing` keys on `params_hash UNIQUE`, with no FK to `prep_sample`), so one processing spans many prep_samples.

`processing_idx` deduplicates on `SHA-256(canonical JSON parameters)` — same workflow + version + params always resolves to the same `processing_idx`.

**Three names that look like levels of this chain are not.** Check here before writing code against any of them:

- `sample_idx` — no such column in any migration, and no `qiita.sample` table. The level the name suggests is `biosample_idx`.
- `processed_prep_sample_idx` — unbuilt. No column in any migration; the only migration mention is a comment in `20260712000000_alignment_definition.sql` recording the hierarchy below `processing_idx` as deferred.
- `prep_idx` — a real `qiita.work_ticket` scope-target column, not an entity; [`data-model.md`](data-model.md) calls it vestigial and says not to build on it.

Reference identifiers form a parallel hierarchy:

```
reference_idx ── reference_membership ── feature_idx ── feature_genome ── genome_idx
```

- `reference_idx` = (name, version) pair for a reference database; `kind` distinguishes sequence references from taxonomy authorities
- `genome_idx` = logical entity across references (nullable — not all features are genomes, e.g., 16S records). Keyed by `(source, source_id)`; whether that `source_id` is an external accession or an internal name is per-`source`, and `repositories/exported_feature.py` is the authority
- `feature_idx` = specific sequence, deduplicated by MD5 hash via DuckDB `md5()` (identical bytes = same `feature_idx`). A **BIGINT identity** — the 128-bit hash is the separate `feature.sequence_hash uuid`. A feature's human-facing name lives on the *membership* (`reference_membership.accession`, nullable), not on `feature`

`feature_idx` bridges sample processing results (alignment detail, counts) and reference data (sequences, taxonomy, annotations, phylogeny). Alignment output contains `feature_idx` but **not** `reference_idx` — reference scoping is a query-time join against `reference_membership`.

Phylogeny internal nodes are addressed by `(reference_idx, node_index)` — scoped to a single tree, not referenced across references. A tip carries its own `feature_idx` **column** on the DuckLake `reference_phylogeny` row (NULL on internal nodes); there is no junction table, and no exclusion-aware `_visible` view — see [Phylogeny and Placements](reference-data.md#phylogeny-and-placements) for why, and what a consumer must do instead.

**Hash storage: never carry MD5 as VARCHAR.** DuckDB's `md5(x)` returns the 32-char hex string by default — never write the string form into a column, temp table, or Parquet file. Cast to `UUID` (`md5(x)::uuid`, 128-bit internally) or use `md5_number(x)` for `UHUGEINT`. Both are 16-byte fixed-width, compare/JOIN as integers, and match the Postgres `uuid` column type the wire-side `sequence_hash` already uses — a string-form intermediate forces a CAST at write time and burns memory + I/O between phases. Same rule applies to any other content hash (SHA-256 as fixed-width bytes, etc.); pick the narrowest integer / fixed-width type the hash fits in.

### Data plane design

The data plane is intentionally "dumb": it only operates on identifiers it receives. Its three Arrow Flight operations map directly to DuckLake:

- **DoGet** — select rows by identifier set from a signed Flight ticket
- **DoPut** — stream RecordBatches to the shared filesystem (`/scratch/ephemeral/staging/`)
- **DoAction** — register Parquet into DuckLake, delete, or insert from processing method

**Flight ticket signing**: the control plane signs tickets with Ed25519 (asymmetric) before handing them to clients — it holds the private seed (`FLIGHT_TICKET_SIGNING_KEY`); the publicly-reachable data plane holds only the public key (`FLIGHT_TICKET_PUBLIC_KEY`) and verifies signatures on every request, so a data-plane compromise cannot forge tickets. It never trusts the client's claimed identifiers directly.

**Ticket replay is an accepted risk, and every DoAction must stay replay-safe.** Flight tickets have no single-use ledger (the data plane is stateless by design), so a still-valid ticket can be replayed within its ~1h lifetime. We accept this because every DoAction variant is idempotent or otherwise replay-safe. This invariant is enforced: the `REPLAY_SAFE_ACTIONS` registry in `qiita-data-plane/src/flight_service.rs` gates the `do_action` dispatcher (an unlisted action is rejected), a test pins the registry to the dispatcher's arms, and an anchored `# replay:` comment sits at the dispatch. When you add a DoAction, make it idempotent/replay-safe and add it to the registry — or, if it can't be, add replay protection before shipping. See [`docs/auth.md#ticket-replay`](../auth.md).

**Result file requirements**: the SLURM backend runs three gates before registration, in `qiita-compute-orchestrator/src/qiita_compute_orchestrator/slurm/verify.py` — a well-formed manifest, every listed file present at its declared `size_bytes`, and mode `0o440` on everything under `$QIITA_OUTPUT_PATH`. A failure is a permanent `CONTRACT_VIOLATION`. `LocalBackend.result_step` runs none of them, so a job that passes locally has not been checked against this.

Sorting a result Parquet by its identifier columns helps DuckLake prune and Parquet push predicates into row groups, and which columns a table has varies. **Nothing verifies the sort.**

> Other docs state that sort as a `must` and as enforced, and list identifier columns this section calls unbuilt. Reconciling them is its own change; where they disagree with this section, this section is the one measured against the code.

**Horizontal scaling**: each data plane instance holds an independent DuckDB+DuckLake connection to the shared Postgres catalog. DuckLake's snapshot isolation means instances never block each other. Add instances to `upstream qiita_data_plane` in nginx to scale.

**Two Rust build flavors**: `make build-data-plane` produces a release binary with `--features duckdb/bundled` (statically linked, slow to build). `make build-data-plane-debug` produces a debug binary that dynamically links libduckdb via `DUCKDB_DOWNLOAD_LIB=1` (fast). `make test-integration` and `make test-system` depend on the debug binary because Python integration tests spawn it directly from its target path instead of shelling out to `cargo run`.

### Compute orchestrator pattern

The orchestrator is a passive, **stateless** HTTP service: the control-plane runner drives the decoupled `POST /api/v1/step/{submit,status,result}` trio (plus `POST /api/v1/step/find-by-name`), each dispatching to the configured `ComputeBackend`. `submit_step` `sbatch`es and returns a handle (SLURM job id + workspace paths) immediately; `status_step` is a single non-looping slurmrestd read; `result_step` verifies output (the three gates above, on the SLURM backend) and returns the paths. **The CP owns the poll loop** — the orchestrator holds no in-flight job state between calls, so a long job never holds the CP→CO connection open and a CP restart re-attaches from persisted `qiita.work_ticket_step` progress. SLURM jobs themselves remain dumb (read input, write output, exit).

**The orchestrator has no DB access** — workflow lifecycle and DB writes happen entirely on the control plane side. CO → CP callbacks exist today for `POST /sequence-range` (called by the native `fastq_to_parquet` step) and authenticate with the compute service-account PAT (site-chosen principal name; `compute` on the live deploy) installed at `/etc/qiita/co-to-cp.token` ([provisioning](../runbooks/compute-service-account-provisioning.md), [rotation](../runbooks/orchestrator-token-rotation.md)). SLURM-backend integration (cluster prereqs, identity model, the `qiita-job` JWT auto-refresh timer) lives in [`docs/runbooks/slurm-backend-setup.md`](../runbooks/slurm-backend-setup.md).

The control plane's submit-time gate is **one ticket in flight per scope target**: `_check_disallow_without_delete` (`routes/work_ticket.py`) 409s a submission while another ticket for the same `(scope_target, action_id, action_version)` is in a non-terminal state, with the six `work_ticket_one_in_flight_per_*` partial unique indexes as the atomic backstop (`..._per_shard` covers the sharded reference fan-out, and `..._per_reference` excludes it with `shard_id IS NULL`). Every arm — reference, study_prep, prep_sample, block, sequenced_pool — binds `NON_TERMINAL_WORK_TICKET_STATES` and nothing else, so a terminal ticket does not block, COMPLETED included. The one exception is `sequenced_pool`, which additionally refuses a submit over a COMPLETED pool ticket (the pool's reads are already registered in the lake) unless `force=true`, gated to wet_lab_admin+.

**Disallow-without-delete is per-result, and lives outside that ticket check.** Two of the three sites are submit-time planners: the align planner refuses a fresh (`only_missing=false`) plan for any sample already carrying an `alignment_sample` gate row under the resolved `alignment_idx` — DELETE the alignment definition to re-align — and the block planner does the same on `mask_sample` under the resolved `mask_idx`. The third fires at step-run instead: `POST /sequence-range` (`routes/sequence_range.py`) 409s on the unique violation when a prep_sample already has a range, and the orchestrator's `sequence_range_retry.py` turns a range minted by a different ticket into a permanent failure naming the DELETE that clears it — so a read-ingest resubmission over a COMPLETED sample is admitted and then dies. Where no site applies, an action is resubmittable as it stands: a `long-read-assembly` submission over an already-COMPLETED prep_sample runs to completion, and what keeps it from duplicating its lake rows is the data plane's replace-by-key (`REPLACE_KEY_TABLES`), not a refusal.

### Workflow runner

`qiita_control_plane.runner.run_workflow` walks an action's `steps:` list for a single `qiita.work_ticket`. Lives in the control plane (direct DB access for work_ticket / action / reference rows is legitimate here). For each entry:

- `step:` — calls the orchestrator over HTTP via `qiita_common.compute_backend_client.ComputeBackendClient`, driving `submit_step` → poll `status_step` (CP-side loop, ~10s) → `result_step`. Per-`(step_index, attempt)` progress is written to `qiita.work_ticket_step` (write-ahead `submitting` before submit) so a CP restart re-attaches via `reconcile_inflight_tickets` instead of failing in-flight work. A CO-unreachable error is transient and retried in place, never failing the ticket.
- `action:` — calls the matching primitive in `qiita_control_plane.actions.library.LIBRARY` directly, no HTTP hop.

Status PATCHes declared in YAML (`target_status`) call `qiita_control_plane.actions.reference.transition_reference_status` in-process. Same atomic, transition-validated UPDATE the public `PATCH /reference/{idx}/status` route uses.
