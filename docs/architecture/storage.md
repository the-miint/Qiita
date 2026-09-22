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
- **`PATH_SCRATCH/`** — fast, working. The control plane derives `PATH_SCRATCH/ticket` (per-ticket workspaces) and `PATH_SCRATCH/staging` (DoPut upload staging); the data plane derives the same `PATH_SCRATCH/staging`, and the orchestrator derives the same `PATH_SCRATCH/ticket` (its readiness probe checks it). Recommended runbook value `/scratch`. Three-tier retention.
- **`PATH_DERIVED/`** — built artifacts. The compute orchestrator derives `PATH_DERIVED/images`, the Apptainer SIF tier SLURM container steps resolve bare `container:` filenames against (required when `COMPUTE_BACKEND=slurm`). Recommended runbook value `/scratch/persistent`.

Layout (showing the recommended runbook value `/data/` for brevity; substitute `PATH_PERSISTENT` in non-default deploys):

```
PATH_PERSISTENT/                            durable, backed up
  ducklake/<table>/<filename>               DuckLake data path (flat per logical table; CRC sharding only if file count pressures the FS)
  logs/<ticket_id>/step_n-<job>.{out,err}   archived SLURM stdout/stderr after job terminal state

/scratch/
  persistent/                               shared FS, never auto-deleted; cluster purge exemption requested
    references/<reference_idx>/<aligner>/   built reference data that doesn't need local-SSD random access
  persistent-local/                         local SSD, never auto-deleted; cluster purge exemption requested
    references/<reference_idx>/<aligner>/   built reference data that needs local-SSD random access (e.g. aligner indices); rebuild-on-miss is the safety net
  ephemeral/                                auto-deleted 45 days after ticket terminal state
    workspace/<work_ticket_idx>/            control-plane runner workspace + SLURM-side params.json + per-step outputs
    staging/<ticket_id>/                    per-ticket SLURM step outputs
    references/incoming/<name>/<version>/   source FASTA staging during reference ingest
```

Two persistent tiers under `/scratch/`:

- `/scratch/persistent/` — the shared-FS persistent tier. Visible to every node, durable, but no random-access guarantees. Hosts built reference data whose access pattern doesn't justify the local-SSD copy (large databases, sequentially read inputs, anything streamed once per job).
- `/scratch/persistent-local/` — **placeholder name** for the local-SSD mount that holds random-access indexed databases (aligner indices today; other things later). The name is provisional — we expect to rename or repurpose it as the deploy grows. Treat it as "the local-SSD path for things we keep around."

Built reference data therefore lives under exactly one of `/scratch/persistent/references/<reference_idx>/<aligner>/` or `/scratch/persistent-local/references/<reference_idx>/<aligner>/`, picked per-reference at ingest time based on the database's access pattern. The path structure is the same; only the tier root differs.

Retention:

- `PATH_PERSISTENT/` — never auto-deleted. Backed up by cluster policy.
- `/scratch/persistent/` and `/scratch/persistent-local/` — never auto-deleted by us; cluster purge exemption requested for both. For aligner indices specifically, if the local-SSD copy is missing for any reason, the orchestrator rebuilds it on demand at job dispatch.
- `/scratch/ephemeral/` — per-ticket directories are deleted 45 days after the ticket reaches a terminal state. The 45-day grace exists for post-mortem debugging.

Same-FS constraint: the SLURM job's final-step output directory and the DuckLake data path (`PATH_PERSISTENT/ducklake`) must live on the same filesystem — the data plane moves files via atomic rename, falling back to copy+delete only on cross-filesystem moves (a slow path that bypasses the rename's atomicity guarantee). The final-step output therefore lives on `PATH_PERSISTENT/` even when intermediate map/reduce outputs use `PATH_SCRATCH/staging/`.

No hive partitioning: a prep sample can be associated with multiple studies, so the on-disk layout is keyed by logical table only — never by `study_idx`. DuckLake's catalog is the sole index over file contents.

## Ticket Signing

The control plane signs short-lived Ed25519 Flight tickets that authorize a specific (table, identifier-set) read or a register-files action. Signing is asymmetric: the CP holds the private seed, the (publicly-reachable) data plane holds only the public key and verifies the signature and expiry on every request — it never trusts client-supplied identifiers directly, and a DP compromise cannot forge tickets. This is the trust boundary between CP and DP: the DP authenticates *the ticket*, not the user. See [`docs/auth.md`](../auth.md) for the verification path and ticket lifetime.
