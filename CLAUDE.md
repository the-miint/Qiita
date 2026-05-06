# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
make test-integration              # cross-component tests; requires Docker (or QIITA_USE_HOST_POSTGRES=1 with libpq env vars to use a host postgres — what CI does on macOS, see docs/runbooks/integration-tests-host-postgres.md); runs Python + Rust integration suites against postgres on :5433; excludes -m system
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

**After changing `qiita-common`, `qiita-control-plane`, or `qiita-compute-orchestrator`** (or after pulling/merging changes to any of them), re-sync dependents so they pick up the changes. Use `--reinstall-package` — plain `uv sync` skips the rebuild when the version string is unchanged, leaving stale sources in `.venv/.../site-packages/<pkg>/` and producing confusing `ImportError`s for newly-added symbols or `TypeError: __init__() got an unexpected keyword argument` for newly-added fields:
```bash
cd qiita-control-plane && uv sync --reinstall-package qiita-common
cd qiita-compute-orchestrator && uv sync --reinstall-package qiita-common
# tests/integration has its own venv that path-installs all three packages —
# reinstall all three after any cross-package merge:
cd tests/integration && uv sync \
  --reinstall-package qiita-common \
  --reinstall-package qiita-control-plane \
  --reinstall-package qiita-compute-orchestrator
```

## Development ethos

**Fail fast, fail early, fail loudly.** Validate inputs at every boundary. Return structured errors with enough context to diagnose without a debugger. Prefer raising/panicking over silently returning defaults for unexpected states. Silent failures are bugs.

## Naming conventions

**DB tables, REST resource segments, scope strings, OpenAPI tags, and the source files that own them are always singular**, never plural — `reference` not `references`, `auth_event` not `auth_events`, `/user` not `/users`, `reference:read` not `references:read`, `routes/reference.py` not `routes/references.py`, `tests/test_user.py` not `tests/test_users.py`. This applies to junction tables (`user_identity`, not `user_identities`); use `_to_` for many-to-many junctions when both sides need to be named (e.g. `biosample_to_study`). Column names follow the same rule unless the column genuinely holds a list/array.

**Carve-outs:** verb / action path segments stay plural where natural (`/admin/principal/{idx}/revoke-all-tokens` — `revoke-all-tokens` is a verb, not a resource). On-disk directory names (`/scratch/persistent/references/`, `references/incoming/`) are not REST resources and are not constrained by this rule. `/user/me` reads awkwardly but is the correct form — the alternative is a permanent carve-out for `/me`-suffixed paths.

Fixed in #11 after the initial schema mixed both forms.

## Architecture

See `docs/architecture.md` for the full system diagram, `docs/reference-data-staging.md` for how reference databases are ingested, and `docs/auth.md` for the authentication / authorization surface (principal subtypes, OIDC + opaque-token paths, role/scope ceilings, admin endpoints, and the `qiita-admin` CLI). Operational runbooks for the auth surface live under `docs/runbooks/`. What follows is the non-obvious cross-cutting structure.

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

### Data plane design

The data plane is intentionally "dumb": it only operates on identifiers it receives. Its three Arrow Flight operations map directly to DuckLake:

- **DoGet** — select rows by identifier set from a signed Flight ticket
- **DoPut** — stream RecordBatches to the shared filesystem (`/scratch/ephemeral/staging/`)
- **DoAction** — register Parquet into DuckLake, delete, or insert from processing method

**Flight ticket signing**: the control plane signs tickets with HMAC-SHA256 before handing them to clients. The data plane verifies signatures on every request — it never trusts the client's claimed identifiers directly.

**Result file requirements**: Parquet files written by SLURM jobs must be mode `440` (verified before registration) and must contain the identifier columns sorted in this exact order: `study_idx, prep_idx, sample_idx, prep_sample_idx, processing_idx, processed_prep_sample_idx`. This sort order enables both DuckLake catalog-level file pruning and Parquet row-group predicate pushdown.

**Horizontal scaling**: each data plane instance holds an independent DuckDB+DuckLake connection to the shared Postgres catalog. DuckLake's snapshot isolation means instances never block each other. Add instances to `upstream qiita_data_plane` in nginx to scale.

**DuckDB**: `duckdb = { version = "1.10502.0" }` links DuckDB v1.5.2. The Rust build cache in CI (`Swatinem/rust-cache`) avoids recompiling it on every push.

**Two Rust build flavors**: `make build-data-plane` produces a release binary with `--features duckdb/bundled` (statically linked, slow to build). `make build-data-plane-debug` produces a debug binary that dynamically links libduckdb via `DUCKDB_DOWNLOAD_LIB=1` (fast). `make test-integration` and `make test-system` depend on the debug binary because Python integration tests spawn it directly from its target path instead of shelling out to `cargo run`.

### Compute orchestrator pattern

The orchestrator is a passive HTTP service: it accepts `POST /api/v1/step/run` from the control-plane runner, dispatches to its configured `ComputeBackend`, and returns the step's output paths. SLURM jobs themselves remain dumb (read input, write output, exit). The orchestrator owns slurmrestd polling and output verification (identifier integrity + file mode) inside its backend implementation.

**The orchestrator has no DB access and no service-account PAT to the control plane** in v1 — workflow lifecycle and DB writes happen entirely on the control plane side. Async-step + CO → CP callbacks (and the `compute` service-account credential) come back when `SlurmBackend` lands.

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

- **Pure-unit** (no infrastructure): `make test` invokes `make test-python` which runs `test-control-plane-without-db` (control-plane tests not carrying the `db` marker), `test-common`, and `test-compute-orchestrator`. No Docker, no Postgres.
- **Control-plane with DB**: `make test-control-plane-with-db` brings up Postgres on :5433 (or uses host Postgres via `QIITA_USE_HOST_POSTGRES=1`), applies dbmate migrations, and runs the full control-plane suite — including `tests/auth/test_resolver.py`, `tests/auth/test_api_token_db.py`, and the route tests under `tests/routes/`. These files carry the `db` marker via `pytestmark = pytest.mark.db` at module level (applies to every test in the file).
- **Cross-component integration**: `make test-integration` brings up the same Postgres and additionally builds and spawns the data plane debug binary, runs the Python integration suite under `tests/integration/`, then resets the `qiita_ducklake` catalog and runs the Rust DuckLake tests.

**Shared fixture surface**: postgres / sessions / OIDC-JWKS fixtures live in `qiita-control-plane/src/qiita_control_plane/testing/` and are imported into both `qiita-control-plane/tests/conftest.py` and `tests/integration/conftest.py` so they cannot drift. Both suites consume the same `postgres_pool`, `human_admin_session`, `regular_user_session`, `compute_worker_service_account`, `jwks_harness`. The `fasta_file` fixture is integration-only — it lives in `tests/integration/conftest.py` because no control-plane test consumes it.

**Postgres harness location**: `docker-compose.yml` and `initdb/` live under `qiita-control-plane/tests/_postgres/` and are used by both `test-control-plane-with-db` and `test-integration`. Port `5433` (not `5432`) avoids collision with a system Postgres.

**DuckLake catalog reset between phases**: `make test-integration` runs the Python suite, then drops and recreates the `qiita_ducklake` Postgres database, then runs the Rust suite. This is required because DuckLake pins `DATA_PATH` into the catalog at creation time, and the two suites use different `DATA_PATH` values (Python picks a pytest `tmp_path_factory` dir; Rust defaults to `/tmp/qiita-integration-ducklake-data`). Reusing the catalog across phases causes confusing "path mismatch" failures. The Python-side analogue is `_reset_ducklake_catalog()` in `tests/integration/conftest.py`; keep the two mechanisms in sync.

**System vs integration marker**: system tests are marked `@pytest.mark.system` and are excluded from `make test-integration` via `-m 'not system'`. Run them with `make test-system`.
