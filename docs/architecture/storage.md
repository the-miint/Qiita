# Storage and Topology

## Database Topology

Two logical Postgres databases on a single hardened instance, one per data domain:

- **`qiita_miint`** — control-plane schema: principals, studies, samples, preparations, work tickets, provenance, references, genomes, features, reference membership, feature-genome mapping.
- **`qiita_miint_lake`** — DuckLake catalog (snapshots, data files, schemas) plus inlined small inserts (`ducklake_inlined_data_tables`).

Each database has a dedicated owner role and a read-only role:

| Role | Database | Purpose |
|---|---|---|
| `qiita_miint_rw` | `qiita_miint` | Owns DB, runs migrations, control-plane runtime connection |
| `qiita_miint_ro` | `qiita_miint` | Read-only consumers (analytics, debugging) |
| `qiita_miint_lake_rw` | `qiita_miint_lake` | Owns DB, data-plane runtime connection |
| `qiita_miint_lake_ro` | `qiita_miint_lake` | Read-only consumers (catalog inspection, audits) |

The control-plane user (`qiita-api`) connects to `qiita_miint` only; the data-plane user (`qiita-data`) connects to `qiita_miint_lake` only. The two domains communicate via the data plane's REST callbacks to the control plane, never via shared DB access.

## Data Storage

Three shared filesystems, mounted on every host that runs Qiita components or SLURM workers. Each is an env-var **base root** the operator sets per component; the services derive fixed subdirs from it (no per-leaf env var):

- **`PATH_PERSISTENT/`** — durable, backed up. System-of-record state. `PATH_PERSISTENT` is an env var the data plane reads directly; it derives the DuckLake data path as `PATH_PERSISTENT/ducklake`. The recommended runbook value is `/data` (see `docs/runbooks/first-deploy.md`); production deploys whose shared filesystem is mounted elsewhere override at deploy time. The data plane's fallback when `PATH_PERSISTENT` is unset is `$TMPDIR/qiita` — so DuckLake lands at `$TMPDIR/qiita/ducklake`, further falling back to `/tmp/qiita/ducklake` if `TMPDIR` itself is unset — a tmp-rooted default, never a production-looking path.
- **`PATH_SCRATCH/`** — fast, working. The control plane derives `PATH_SCRATCH/ticket` (per-ticket workspaces) and `PATH_SCRATCH/staging` (DoPut upload staging); the data plane derives the same `PATH_SCRATCH/staging` and its own `PATH_SCRATCH/duckdb-spill` (DuckDB spill), and the orchestrator derives the same `PATH_SCRATCH/ticket` (its readiness probe checks it). Recommended runbook value `/scratch`.
- **`PATH_DERIVED/`** — built artifacts. The compute orchestrator derives `PATH_DERIVED/images`, the Apptainer SIF tier SLURM container steps resolve bare `container:` filenames against (required when `COMPUTE_BACKEND=slurm`). Recommended runbook value `/scratch/persistent`.

Layout (`/scratch/` is the recommended runbook value for `PATH_SCRATCH`; substitute the configured roots in other deploys):

```
PATH_PERSISTENT/                            durable, backed up
  ducklake/<table>/<filename>               DuckLake data path (flat per logical table; CRC sharding only if file count pressures the FS)

/scratch/                                   recommended PATH_SCRATCH (and PATH_DERIVED = /scratch/persistent)
  ticket/<work_ticket_idx>/<step>/attempt-<N>/  per-attempt workspace: control-plane runner files and logs/{stdout,stderr} (written by the native-job launcher under SLURM, or by an in-process action); under SLURM also input/params.json, output/ (step outputs), tmp/; reclaimed on request by `qiita-admin scratch reclaim`, which keeps logs/
  staging/uploads/<upload_idx>/upload.parquet   DoPut upload staging; not reclaimed automatically
  staging/uploads/<upload_idx>/upload.parquet.partial  a DoPut being written; one a killed data plane left is not reclaimed automatically
  duckdb-spill/<host>-<pid>-<random>/       one data-plane connection's DuckDB spill
  references/staging/<name>/<version>/     operator-staged reference source files (docs/reference-data-staging.md)
  persistent/                               shared FS, never auto-deleted; cluster purge exemption requested
    references/<reference_idx>/<index>/     built reference indexes (PATH_DERIVED/references; derived_store.py)
    references/<reference_idx>/krepp/<build_label>/  master copy of a krepp build, placed by hand, sealed at registration
  qiita-miint/                              recommended PATH_NODE_LOCAL (optional): the node cache, on each compute node's own disk
    .locks/<sha256 of path>.lock            one per cached folder; jobs on a node take it in turn
    references/<reference_idx>/krepp/<build_label>/  this node's copy of a sealed folder, made by the first job that needed it
```

Two tiers hold built reference data:

- `PATH_DERIVED` (`/scratch/persistent/`) — the shared-FS persistent tier. Visible to every node, durable. Every index Qiita builds lives here (`derived_store.py` owns the layout), and the master copy of a krepp build.
- `PATH_NODE_LOCAL` (`/scratch/qiita-miint/`, optional) — the **node cache**: a directory on each compute node's own disk, writable by the job user, that `node_cache.py` alone writes. It holds copies of **sealed folders** (a folder under `PATH_DERIVED` whose `SHA256SUMS` hash is recorded in the database; today, krepp builds, `NODE_CACHED_INDEX_TYPES`), so a job reads them from local disk rather than the shared filesystem, where `place_krepp` with an index read by mmap stalled intermittently. Reading from a node's own disk has not been measured yet. A job asks `node_cache.materialize` for a folder: the first job on a node that needs it checks the free space, checks the master's file list, copies it while hashing every file against `SHA256SUMS`, and renames the folder into place, so a folder at that path was written whole; jobs on the same node wait on its lock, then check the copy (`SHA256SUMS` hash, file list, sizes) and use it. Nothing is copied at deploy or by hand. Unset, jobs read the master under `PATH_DERIVED`. The orchestrator forwards it into every job; the readiness probe's `node-cache` row checks that it is a writable directory on the node it ran on.

Retention:

- `PATH_PERSISTENT/` — never auto-deleted. Backed up by cluster policy.
- `/scratch/persistent/` — never auto-deleted by us; cluster purge exemption requested. Deleting a reference removes its `references/<reference_idx>/` subtree (`DELETE /reference-artifact/{idx}`), which the delete refuses while the reference has a krepp build not yet removed.
- `PATH_NODE_LOCAL` — written only by `node_cache.py`. A `reference-index-activate` job deletes its node's copies of removed builds before it reads its own (`node_cache.evict`, given the list by `export-krepp-build`); nothing else is deleted, and there is no size cap: a copy that does not fit fails the job, naming the space it needed.
- `PATH_SCRATCH/duckdb-spill/` — each directory is removed when the data-plane connection that owns it goes, which no stop reaches: the process has no shutdown path that drops its pool. A data plane at boot removes the directories its own host left whose process is gone (`sweep_abandoned_spill_directories` in `qiita-data-plane/src/pool.rs`); another host's are left to that host.
- `PATH_SCRATCH/ticket/` and `PATH_SCRATCH/staging/` — nothing reclaims them automatically: no sweep deletes a ticket's workspace or an upload's staging directory after the ticket ends, so both grow with every ticket. Registration moves a registered file out of its output dir; everything else (inputs, logs, `tmp/`, outputs no step registered) stays. `qiita-admin scratch reclaim --older-than DAYS [--execute]` deletes everything under `ticket/<idx>/<step>/attempt-<N>/` except `logs/` for finished tickets (see `--help` for eligibility; `--older-than` counts from the ticket's last update). It must run as the user that owns the job output dirs (the SLURM job user), since it restores owner write on 0550 container output dirs before deleting. It never touches `staging/`. A reclaimed ticket can no longer feed a backfill that reads its workspace, so the command skips the read-mask tickets the SynDNA read-count backfill would still write and the long-read-assembly tickets the gene-call recovery can still recover or is recovering; once a ticket is reclaimed, its backfill gap is permanent.

`PATH_SCRATCH` and `PATH_PERSISTENT` are separate filesystems in production, since scratch and the durable lake have different needs. Registration moves each file from the producing step's output dir under `PATH_SCRATCH/ticket/` into `PATH_PERSISTENT/ducklake/<table>/` (`move_file` in `qiita-data-plane/src/flight_service/registration.rs`). It renames the file without replacing an existing one, or, where the filesystem has no such rename, hard-links it and removes the source; failing both it copies the whole file and removes the source. The copy is taken when the two directories are on different filesystems, and also on one filesystem when it has no no-replace rename and the kernel refuses the link (Linux's `fs.protected_hardlinks` refuses a link to a file the data plane's user neither owns nor can write, as a 0440 job output owned by another account is). Measured on the deploy host (2026-10-22): every registration copies; the lake mount answers the no-replace rename with `EINVAL`, and refuses an exclusive create of an existing file with `EEXIST`.

No hive partitioning: a prep sample can be associated with multiple studies, so the on-disk layout is keyed by logical table only — never by `study_idx`. DuckLake's catalog is the sole index over file contents.

## Ticket Signing

The control plane signs short-lived Ed25519 Flight tickets that authorize a specific (table, identifier-set) read or a register-files action. Signing is asymmetric: the CP holds the private seed, the (publicly-reachable) data plane holds only the public key and verifies the signature and expiry on every request — it never trusts client-supplied identifiers directly, and a DP compromise cannot forge tickets. This is the trust boundary between CP and DP: the DP authenticates *the ticket*, not the user. See [`docs/auth.md`](../auth.md) for the verification path and ticket lifetime.
