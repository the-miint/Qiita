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
  ticket/<work_ticket_idx>/<step>/attempt-<N>/  per-attempt workspace: control-plane runner files; under SLURM also input/params.json, output/ (step outputs), logs/{stdout,stderr}, tmp/; not reclaimed automatically
  staging/uploads/<upload_idx>/upload.parquet   DoPut upload staging; not reclaimed automatically
  staging/uploads/<upload_idx>/upload.parquet.partial  a DoPut being written; one a killed data plane left is not reclaimed automatically
  duckdb-spill/<host>-<pid>-<random>/       one data-plane connection's DuckDB spill
  references/staging/<name>/<version>/     operator-staged reference source files (docs/reference-data-staging.md)
  persistent/                               shared FS, never auto-deleted; cluster purge exemption requested
    references/<reference_idx>/<aligner>/   built reference data that doesn't need local-SSD random access
  persistent-local/                         local SSD, never auto-deleted; cluster purge exemption requested
    references/<reference_idx>/<aligner>/   built reference data that needs local-SSD random access (e.g. aligner indices); rebuild-on-miss is the safety net
```

Two persistent tiers under `/scratch/`:

- `/scratch/persistent/` — the shared-FS persistent tier. Visible to every node, durable, but no random-access guarantees. Hosts built reference data whose access pattern doesn't justify the local-SSD copy (large databases, sequentially read inputs, anything streamed once per job).
- `/scratch/persistent-local/` — **placeholder name** for the local-SSD mount that holds random-access indexed databases (aligner indices today; other things later). The name is provisional — we expect to rename or repurpose it as the deploy grows. Treat it as "the local-SSD path for things we keep around."

Built reference data therefore lives under exactly one of `/scratch/persistent/references/<reference_idx>/<aligner>/` or `/scratch/persistent-local/references/<reference_idx>/<aligner>/`, picked per-reference at ingest time based on the database's access pattern. The path structure is the same; only the tier root differs.

Retention:

- `PATH_PERSISTENT/` — never auto-deleted. Backed up by cluster policy.
- `/scratch/persistent/` and `/scratch/persistent-local/` — never auto-deleted by us; cluster purge exemption requested for both. For aligner indices specifically, if the local-SSD copy is missing for any reason, the orchestrator rebuilds it on demand at job dispatch.
- `PATH_SCRATCH/duckdb-spill/` — each directory is removed when the data-plane connection that owns it goes, which no stop reaches: the process has no shutdown path that drops its pool. A data plane at boot removes the directories its own host left whose process is gone (`sweep_abandoned_spill_directories` in `qiita-data-plane/src/pool.rs`); another host's are left to that host.
- `PATH_SCRATCH/ticket/` and `PATH_SCRATCH/staging/` — nothing reclaims them: no sweep deletes a ticket's workspace or an upload's staging directory after the ticket ends, so both grow with every ticket. Registration moves a registered file out of its output dir; everything else (inputs, logs, `tmp/`, outputs no step registered) stays.

`PATH_SCRATCH` and `PATH_PERSISTENT` are separate filesystems in production, since scratch and the durable lake have different needs. Registration moves each file from the producing step's output dir under `PATH_SCRATCH/ticket/` into `PATH_PERSISTENT/ducklake/<table>/` (`move_file` in `qiita-data-plane/src/flight_service.rs`). It renames the file without replacing an existing one, or, where the filesystem has no such rename, hard-links it and removes the source; failing both it copies the whole file and removes the source. The copy is taken when the two directories are on different filesystems, and also on one filesystem when it has no no-replace rename and the kernel refuses the link (Linux's `fs.protected_hardlinks` refuses a link to a file the data plane's user neither owns nor can write, as a 0440 job output owned by another account is). Measured on the deploy host (2026-10-22): every registration copies; the lake mount answers the no-replace rename with `EINVAL`, and refuses an exclusive create of an existing file with `EEXIST`.

No hive partitioning: a prep sample can be associated with multiple studies, so the on-disk layout is keyed by logical table only — never by `study_idx`. DuckLake's catalog is the sole index over file contents.

## Ticket Signing

The control plane signs short-lived Ed25519 Flight tickets that authorize a specific (table, identifier-set) read or a register-files action. Signing is asymmetric: the CP holds the private seed, the (publicly-reachable) data plane holds only the public key and verifies the signature and expiry on every request — it never trusts client-supplied identifiers directly, and a DP compromise cannot forge tickets. This is the trust boundary between CP and DP: the DP authenticates *the ticket*, not the user. See [`docs/auth.md`](../auth.md) for the verification path and ticket lifetime.
