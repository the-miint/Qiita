# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Python version

This repo targets **Python 3.14**. Run tooling via `uv run` — a stray pre-3.14 `python3` misparses `except A, B:` ([PEP 758](https://peps.python.org/pep-0758/)), which is valid here. Don't "fix" it to `except (A, B):`.

## Common commands

```bash
# First-time setup (run once after cloning)
make install-hooks   # installs pre-commit hooks via uv tool

# Check tool prerequisites; prints install commands only for what's missing
make dev-setup

# Build all components (release data-plane binary uses bundled DuckDB)
make build

# Test
make test                          # pure-unit tests (all components, no infrastructure required)
make test-control-plane-with-db    # full control-plane suite incl. DB-bound (-m db) tests; brings up Postgres + applies dbmate migrations
make test-integration              # cross-component tests; requires Docker (or QIITA_USE_HOST_POSTGRES=1 with libpq env vars to use a host postgres); runs Python + Rust integration suites against postgres on :5433; excludes -m system
make test-system                   # real GG2 backbone data; slow (~10 min); needs localdocs/scratch/
make test-workflows                # requires apptainer (Linux-only — macOS skips gracefully); CI runs this on ubuntu only

# Lint
make lint

# Database migrations (auto-installs dbmate)
make migrate

# Deploy (prints systemd + nginx instructions; does not sudo)
make deploy
make verify-health         # auto-installs grpcurl

# Clean component build artifacts (.venv, target/, caches)
make clean
```

**Running a single test:**
```bash
# Python
cd qiita-control-plane && uv run pytest tests/test_smoke.py::test_health

# Rust — DUCKDB_DOWNLOAD_LIB=1 dynamically links a prebuilt libduckdb from
# target/duckdb-download instead of rebuilding the bundled DuckDB from source.
# Without it, every invocation can spend many minutes compiling DuckDB.
cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo test config_defaults
```

**Linting a single component:**
```bash
cd qiita-common && uv run ruff check . && uv run ruff format --check .
cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo clippy -- -D warnings && cargo fmt --check
```

**Cross-package staleness — handled by `make build` / `make test*`.** When `qiita-common`, `qiita-control-plane`, or `qiita-compute-orchestrator` change, plain `uv sync` in a dependent skips the rebuild because the version string is unchanged, leaving stale sources in `.venv/.../site-packages/<pkg>/` and producing confusing `ImportError`s for newly-added symbols or `TypeError: __init__() got an unexpected keyword argument` for newly-added fields. The `build-*` and `test-*` Makefile targets pass `--reinstall-package` to force a rebuild of the affected path deps in every consuming venv (the three project venvs plus `tests/integration/.venv`).

If you bypass `make` and run `uv` directly after a cross-package change, replicate the flag yourself, e.g.:
```bash
cd qiita-control-plane && uv sync --reinstall-package qiita-common
```

## Development ethos

**Fail fast, fail early, fail loudly.** Validate inputs at every boundary. Return structured errors with enough context to diagnose without a debugger. Prefer raising/panicking over silently returning defaults for unexpected states. Silent failures are bugs.

## Workflow runtimes

A step in a workflow YAML must declare **exactly one** of `container:` or `module:`. The `module:` form (a native step) runs in the orchestrator's Python environment under SLURM and may only use dependencies that already ship in `qiita-compute-orchestrator`'s `pyproject.toml`; anything heavier (bioinformatics deps, system packages) belongs in a container.

Native job modules export exactly two symbols — `class Inputs(BaseModel)` (typed input contract) and `async def execute(inputs, workspace)` (the work). A single framework dispatcher handles import, validation, and error classification; both the local backend and the SLURM launcher route through it.

The wire validator enforces shape only (exactly-one). The module-prefix invariant (`qiita_compute_orchestrator.jobs.`) is enforced separately at sync, submit, boot scan, and dispatcher — [`docs/architecture.md`](docs/architecture.md) carries the per-site breakdown.

### Container image tier

Container steps declare a bare SIF filename in `container:` (e.g. `bcl-convert-4.5.4.sif`). The orchestrator joins this against `Settings.qiita_images_dir` (`QIITA_IMAGES_DIR` env var, required when `COMPUTE_BACKEND=slurm`) to resolve the absolute SIF path. Registry-URL forms with `://` pass through; anything else with a path separator → `CONTRACT_VIOLATION`.

After editing a workflow YAML or its container artifacts (`workflows/<workflow>/Apptainer.def`, `entrypoint.sh`, or the shared `workflows/_shared/manifest_writer.py`):

These steps run on the **Linux deploy host** — they need `apptainer` (to build the SIF) and `systemd` (to restart the services), so they don't apply on a macOS dev box (mirrors `make test-workflows`, which skips gracefully off Linux). On macOS, edit the artifacts and run the unit tests; the SIF rebuild + restart happen at deploy time on the host.

```bash
# Rebuild the SIF (idempotent — skips when the existing SIF already reports the target version).
bash scripts/build-<workflow>-sif.sh
make deploy
sudo systemctl restart qiita-control-plane qiita-compute-orchestrator
make verify-health
```

Container input bind mounts are computed by `SlurmBackend._resolve_input_binds` (file → parent dir, directory → itself, deduped by resolved path). This means a step's YAML-declared `inputs:` paths must be absolute when they originate from `action_context` and must be visible from the compute node — bind mounts only expose host paths, they do not copy.

## Naming conventions

**DB tables, REST resource segments, scope strings, OpenAPI tags, and the source files that own them are always singular**, never plural — `reference` not `references`, `auth_event` not `auth_events`, `/user` not `/users`, `reference:read` not `references:read`, `routes/reference.py` not `routes/references.py`, `tests/test_user.py` not `tests/test_users.py`. This applies to junction tables (`user_identity`, not `user_identities`); use `_to_` for many-to-many junctions when both sides need to be named (e.g. `biosample_to_study`). Column names follow the same rule unless the column genuinely holds a list/array.

**Carve-outs:** verb / action path segments stay plural where natural (`/admin/principal/{idx}/revoke-all-tokens` — `revoke-all-tokens` is a verb, not a resource). On-disk directory names (`/scratch/persistent-local/references/`, `references/incoming/`) are not REST resources and are not constrained by this rule. `/user/me` reads awkwardly but is the correct form — the alternative is a permanent carve-out for `/me`-suffixed paths.

Fixed in #11 after the initial schema mixed both forms.

## REST path constants

REST paths live exclusively in `qiita-common/src/qiita_common/api_paths.py`. Never hardcode `"/api/v1/..."` literals in routes, tests, or clients — import the constants instead.

Two flavours per route:

- `PATH_*` — sub-path used by FastAPI `@router.<verb>(...)` decorators and the matching `prefix=` declaration.
- `URL_*` — full path under `API_PREFIX` for tests and clients, with `{placeholder}` segments where parameterized.

When you add or rename a route, define both flavours and register the triple in the parity test in `qiita-common/tests/test_api_paths.py`. A missing triple fails the test; a `URL_*` constant left out of the registration list also fails. Routers sharing a prefix (`/study` is reused by biosample and sequenced-sample; `/sequencing-run` by sequenced-sample) declare `prefix=PATH_STUDY_PREFIX` etc. so a prefix rename moves every router at once.

## Database migrations

The qiita-miint deploy is live; every migration currently in `qiita-control-plane/db/migrations/` (`YYYYMMDDHHMMSS_<name>.sql`, starting with `20260501000000_schema.sql`) has been applied to its Postgres. **Never edit an already-applied migration** — `dbmate` tracks applied versions in `schema_migrations` and won't re-run an edited file, so the live DB silently drifts from the source.

Every schema change is a **new migration file** (`YYYYMMDDHHMMSS_<name>.sql`, with `migrate:up` and `migrate:down` blocks). Common shapes:
- Add a column / index / constraint: a single `ALTER TABLE` migration.
- Add a Postgres ENUM value: `ALTER TYPE ... ADD VALUE`, with the Python `StrEnum` twin updated in the same PR (see Enum parity below).
- Rename / drop / type-change: expand-then-contract across two migrations (and usually two PRs) so a rolling deploy doesn't 500.

Before merging: `make test-control-plane-with-db` runs `dbmate up` against a fresh DB and must pass — that's the only safety net before the migration touches production. After merging: the operator runs `make migrate` against the live DB on the next deploy.

## Enum parity (Python ↔ Postgres)

Many closed value sets are **deliberately duplicated**: once as a Python `StrEnum` in `qiita-common` (so Pydantic models type-check at import time, with no DB connection) and once as a Postgres `CREATE TYPE ... AS ENUM` (so the database itself rejects bad values). Per issue #37 this duplication is a chosen compromise — the DB is *not* the single source of truth — so do **not** try to derive one side from the other.

Not every closed value set is a Postgres ENUM. `auth_event.event_type`, `reference.status`, `reference.kind`, and `upload.status` are intentionally plain `TEXT` (with a `CHECK` where appropriate) even though their Python twins exist (`AuthEventType`, `ReferenceStatus`, and `UploadStatus` are `StrEnum`s; `ReferenceKind` is a `Literal`) — see those migrations for the rationale. The rules below apply **only** to value sets that are `CREATE TYPE ... AS ENUM`; a `StrEnum`/`Literal` backed by a `TEXT`/`CHECK` column is a valid, deliberate choice and is out of scope for the parity test.

Whenever you add, rename, or remove a value in an enum that *does* have a Postgres `CREATE TYPE` twin:

1. **Change both sides in the same PR.** Update the Python `StrEnum` *and* the Postgres ENUM. Postgres ENUM changes go in a **new migration** (`ALTER TYPE ... ADD VALUE` / rename) — editing an already-applied `CREATE TYPE` migration does not reach databases that already ran it (see Database migrations above).
2. **Keep the two-way comment.** The Python enum's docstring names its Postgres twin; the Postgres `CREATE TYPE` comment names its Python twin. Both must stay accurate so anyone reading either one is reminded of the other.
3. **Register the pair for the parity test.** `ENUM_PAIRS` in `qiita-control-plane/tests/test_enum_parity.py` lists every `(Python enum, Postgres ENUM)` pair. `test_enum_parity` fails on value drift; `test_all_postgres_enums_are_covered` fails if a Postgres ENUM in the `qiita` schema is not registered there. Both run under `make test-control-plane-with-db`. A brand-new mirrored enum must be added to `ENUM_PAIRS`.

This also applies when reviewing code: a PR that changes a `CREATE TYPE ... AS ENUM` or its Python twin without the matching other-side change, two-way comment, and `ENUM_PAIRS` entry is incomplete. A new `StrEnum`/`Literal` with no Postgres ENUM is *not* a defect — see the `TEXT`/`CHECK` carve-out above — so do not flag it for a missing ENUM twin.

## Operator-facing changes (CHANGELOG.md)

`CHANGELOG.md` at the repo root is the operator's release-notes channel for deploys. It lives separately from the git log and the PR descriptions because future operators read `tail CHANGELOG.md` before running `sudo local-deploy.sh` to find out what's new since their last deploy — PR descriptions sit inside closed PRs and stop being a natural lookup target once merged.

Add a new entry to `CHANGELOG.md` *in the same PR* whenever the PR introduces any of:

- A new required env var (CP, DP, or CO) — the boot-time `from_env()` fail-fast catches it, but the operator should set it pre-deploy, not after the systemd unit fails to start.
- A new shared directory the operator must create with specific owner/group/mode (e.g. an upload-staging or workspace root).
- A migration that needs out-of-band setup the runbook doesn't already cover (CREATE EXTENSION, manual data backfill, etc.).
- A breaking change in `/etc/qiita/*.env` shape (renamed key, removed key with no compat alias).
- Any other action the operator must take on the deploy host *before* `sudo local-deploy.sh` for the deploy to succeed.

A PR that only changes Python/Rust code, tests, docs, or migrations that the existing dbmate flow handles autonomously does **not** need an entry. When in doubt: "if the operator follows the existing runbook + `sudo local-deploy.sh` without reading this PR, does the deploy succeed?" If no, add an entry.

Entry format: `## PR #N — <title>` heading, newest on top, with concrete `bash` commands the operator can copy/paste. The existing entries in the file show the shape. Reviewers check that the entry exists when the PR description (or the diff) implies operator action.

## Architecture

See `docs/architecture.md` for the full system diagram, `docs/reference-data-staging.md` for how reference databases are ingested, `docs/auth.md` for the authentication / authorization surface (principal subtypes, OIDC + opaque-token paths, role/scope ceilings, admin endpoints, and the `qiita-admin` CLI), and `docs/duckdb-miint.md` for the duckdb-miint SQL extension that powers our bioinformatics functions — that file carries a `Last checked` date; re-verify a signature against upstream before relying on it if the file looks stale. Operational runbooks for the auth surface live under `docs/runbooks/`. What follows is the non-obvious cross-cutting structure.

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
study_idx → prep_idx → sample_idx → prep_sample_idx → processing_idx → processed_prep_sample_idx
```

`processing_idx` deduplicates on `SHA-256(canonical JSON parameters)` — same workflow + version + params always resolves to the same `processing_idx`.

Reference identifiers form a parallel hierarchy:

```
reference_idx ── reference_membership ── feature_idx ── feature_genome ── genome_idx
                                    └── phylogeny_tip_feature (reference_idx, node_index) → feature_idx
```

- `reference_idx` = (name, version) pair for a reference database; `kind` distinguishes sequence references from taxonomy authorities
- `genome_idx` = logical entity across references (nullable — not all features are genomes, e.g., 16S records)
- `feature_idx` = specific sequence, deduplicated by MD5 hash via DuckDB `md5()` (identical bytes = same `feature_idx`; stored as Postgres `uuid`)

`feature_idx` bridges sample processing results (alignment detail, counts) and reference data (sequences, taxonomy, annotations, phylogeny). Alignment output contains `feature_idx` but **not** `reference_idx` — reference scoping is a query-time join against `reference_membership`.

Phylogeny internal nodes are addressed by `(reference_idx, node_index)` — scoped to a single tree, not referenced across references. Tip nodes connect to `feature_idx` via the `phylogeny_tip_feature` junction table.

**Hash storage: never carry MD5 as VARCHAR.** DuckDB's `md5(x)` returns the 32-char hex string by default — never write the string form into a column, temp table, or Parquet file. Cast to `UUID` (`md5(x)::uuid`, 128-bit internally) or use `md5_number(x)` for `UHUGEINT`. Both are 16-byte fixed-width, compare/JOIN as integers, and match the Postgres `uuid` column type the wire-side `sequence_hash` and `feature_idx` already use — a string-form intermediate forces a CAST at write time and burns memory + I/O between phases. Same rule applies to any other content hash (SHA-256 as fixed-width bytes, etc.); pick the narrowest integer / fixed-width type the hash fits in.

### Data plane design

The data plane is intentionally "dumb": it only operates on identifiers it receives. Its three Arrow Flight operations map directly to DuckLake:

- **DoGet** — select rows by identifier set from a signed Flight ticket
- **DoPut** — stream RecordBatches to the shared filesystem (`/scratch/ephemeral/staging/`)
- **DoAction** — register Parquet into DuckLake, delete, or insert from processing method

**Flight ticket signing**: the control plane signs tickets with HMAC-SHA256 before handing them to clients. The data plane verifies signatures on every request — it never trusts the client's claimed identifiers directly.

**Result file requirements**: Parquet files written by SLURM jobs must be mode `440` (verified before registration) and must contain the identifier columns sorted in this exact order: `study_idx, prep_idx, sample_idx, prep_sample_idx, processing_idx, processed_prep_sample_idx`. This sort order enables both DuckLake catalog-level file pruning and Parquet row-group predicate pushdown.

**Horizontal scaling**: each data plane instance holds an independent DuckDB+DuckLake connection to the shared Postgres catalog. DuckLake's snapshot isolation means instances never block each other. Add instances to `upstream qiita_data_plane` in nginx to scale.

**Two Rust build flavors**: `make build-data-plane` produces a release binary with `--features duckdb/bundled` (statically linked, slow to build). `make build-data-plane-debug` produces a debug binary that dynamically links libduckdb via `DUCKDB_DOWNLOAD_LIB=1` (fast). `make test-integration` and `make test-system` depend on the debug binary because Python integration tests spawn it directly from its target path instead of shelling out to `cargo run`.

### Compute orchestrator pattern

The orchestrator is a passive HTTP service: it accepts `POST /api/v1/step/run` from the control-plane runner, dispatches to its configured `ComputeBackend`, and returns the step's output paths. SLURM jobs themselves remain dumb (read input, write output, exit). The orchestrator owns slurmrestd polling and output verification (identifier integrity + file mode) inside its backend implementation.

**The orchestrator has no DB access** — workflow lifecycle and DB writes happen entirely on the control plane side. CO → CP callbacks exist today for `POST /sequence-range` (called by the native `fastq_to_parquet` step) and authenticate with the `compute-worker` service-account PAT installed at `/etc/qiita/co-to-cp.token` ([provisioning](docs/runbooks/compute-service-account-provisioning.md), [rotation](docs/runbooks/orchestrator-token-rotation.md)). SLURM-backend integration (cluster prereqs, identity model, the `qiita-job` JWT auto-refresh timer) lives in [`docs/runbooks/slurm-backend-setup.md`](docs/runbooks/slurm-backend-setup.md).

The control plane enforces **disallow-without-delete**: before submitting any job it checks `(prep_sample_idx, processing_idx)` pairs — COMPLETED results require explicit DELETE before resubmission; PENDING/QUEUED/PROCESSING states block new submission entirely.

### Workflow runner

`qiita_control_plane.runner.run_workflow` walks an action's `steps:` list for a single `qiita.work_ticket`. Lives in the control plane (direct DB access for work_ticket / action / reference rows is legitimate here). For each entry:

- `step:` — calls the orchestrator over HTTP via `qiita_common.compute_backend_client.ComputeBackendClient` (`POST /api/v1/step/run`). Synchronous in v1: blocks for the duration of the backend step.
- `action:` — calls the matching primitive in `qiita_control_plane.actions.library.LIBRARY` directly, no HTTP hop.

Status PATCHes declared in YAML (`target_status`) call `qiita_control_plane.actions.reference.transition_reference_status` in-process. Same atomic, transition-validated UPDATE the public `PATCH /reference/{idx}/status` route uses.

### qiita-common as a path dependency

```toml
# in qiita-control-plane/pyproject.toml and qiita-compute-orchestrator/pyproject.toml
qiita-common = { path = "../qiita-common" }
```

This is the contract layer between the two Python services. Pydantic models for work ticket states and API schemas live here. Changes here affect both dependents — re-run `uv sync` in each.

### Lock files

Both `uv.lock` (Python) and `Cargo.lock` (Rust) are committed. Do not add them to `.gitignore`.

### Test layout and tiers

The test suite is split into three tiers by the infrastructure each one needs:

- **Pure-unit** (no infrastructure): `make test`. Pure Python + Rust unit tests across all components. Excludes tests carrying the `db` marker.
- **Control-plane with DB**: `make test-control-plane-with-db`. Brings up Postgres on :5433 (or uses host Postgres via `QIITA_USE_HOST_POSTGRES=1`), applies dbmate migrations, and runs every control-plane test including the `db`-marked ones. Tests opt in either at module scope (`pytestmark = pytest.mark.db` — pulls every test in the file into the DB tier) or per-test (`@pytest.mark.db` decorator on the function — for mixed modules where only some tests need a DB).
- **Cross-component integration**: `make test-integration`. Same Postgres, plus builds the data-plane debug binary; runs the Python integration suite, then resets the `qiita_ducklake` catalog and runs the Rust DuckLake tests. System tests (`@pytest.mark.system`) are excluded — run those with `make test-system`.

**Shared fixtures across tiers**: the DB / session / OIDC-JWKS fixtures live in `qiita-control-plane/src/qiita_control_plane/testing/` and are imported by both the control-plane and integration conftests so they cannot drift.

**Postgres harness**: `docker-compose.yml` + `initdb/` live under `qiita-control-plane/tests/_postgres/` and are reused by both DB-bound tiers. Port `5433` (not `5432`) avoids collision with a host Postgres.

**DuckLake catalog reset between phases**: `make test-integration` runs the Python suite, drops and recreates the `qiita_ducklake` Postgres database, then runs the Rust suite. DuckLake pins `DATA_PATH` into the catalog at creation time and the two suites use different `DATA_PATH` values; reusing the catalog produces confusing "path mismatch" failures. The Python conftest has the same drop/recreate logic so a single phase is self-contained too — keep the two mechanisms in sync.
