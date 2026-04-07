# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common commands

```bash
# First-time setup (run once after cloning)
make install-hooks   # installs pre-commit hooks via uv tool

# Build all components
make build

# Test
make test                  # unit tests (all components)
make test-integration      # requires Docker; starts/stops postgres on :5433
make test-workflows        # requires apptainer; skips gracefully if absent

# Lint
make lint

# Database migrations (auto-installs dbmate)
make migrate

# Deploy (prints systemd + nginx instructions; does not sudo)
make deploy
make verify-health         # auto-installs grpcurl
```

**Running a single test:**
```bash
# Python
cd qiita-control-plane && uv run pytest tests/test_smoke.py::test_health

# Rust
cd qiita-data-plane && cargo test config_defaults
```

**Linting a single component:**
```bash
cd qiita-common && uv run ruff check . && uv run ruff format --check .
cd qiita-data-plane && cargo clippy -- -D warnings && cargo fmt --check
```

**After changing `qiita-common`**, re-sync dependents so they pick up the changes:
```bash
cd qiita-control-plane && uv sync
cd qiita-compute-orchestrator && uv sync
```

## Development ethos

**Fail fast, fail early, fail loudly.** Validate inputs at every boundary. Return structured errors with enough context to diagnose without a debugger. Prefer raising/panicking over silently returning defaults for unexpected states. Silent failures are bugs.

## Architecture

See `docs/architecture.md` for the full system diagram. What follows is the non-obvious cross-cutting structure.

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
- **DoPut** — stream RecordBatches to the shared filesystem (`/data/staging/`)
- **DoAction** — register Parquet into DuckLake, delete, or insert from processing method

**Flight ticket signing**: the control plane signs tickets with HMAC-SHA256 before handing them to clients. The data plane verifies signatures on every request — it never trusts the client's claimed identifiers directly.

**Result file requirements**: Parquet files written by SLURM jobs must be mode `440` (verified before registration) and must contain the identifier columns sorted in this exact order: `study_idx, prep_idx, sample_idx, prep_sample_idx, processing_idx, processed_prep_sample_idx`. This sort order enables both DuckLake catalog-level file pruning and Parquet row-group predicate pushdown.

**Horizontal scaling**: each data plane instance holds an independent DuckDB+DuckLake connection to the shared Postgres catalog. DuckLake's snapshot isolation means instances never block each other. Add instances to `upstream qiita_data_plane` in nginx to scale.

**Bundled DuckDB**: `duckdb = { version = "1.10501.0", features = ["bundled"] }` statically links DuckDB v1.5.1. First compile is slow (~2.5 min); subsequent incremental builds are fast. The Rust build cache in CI (`Swatinem/rust-cache`) avoids recompiling it on every push.

### Compute orchestrator pattern

The orchestrator owns the full job lifecycle — SLURM jobs themselves are kept dumb (read input, write output, exit). The orchestrator submits via slurmrestd, polls for completion, verifies output (identifier integrity + file mode), collects logs, then calls back to the control plane REST endpoint. It uses a dedicated `compute` service account with narrow permissions (update work tickets + request file registration only).

The control plane enforces **disallow-without-delete**: before submitting any job it checks `(prep_sample_idx, processing_idx)` pairs — COMPLETED results require explicit DELETE before resubmission; PENDING/QUEUED/PROCESSING states block new submission entirely.

### qiita-common as a path dependency

```toml
# in qiita-control-plane/pyproject.toml and qiita-compute-orchestrator/pyproject.toml
qiita-common = { path = "../qiita-common" }
```

This is the contract layer between the two Python services. Pydantic models for work ticket states and API schemas live here. Changes here affect both dependents — re-run `uv sync` in each.

### Lock files

Both `uv.lock` (Python) and `Cargo.lock` (Rust) are committed. Do not add them to `.gitignore`.

### Integration tests

Use port `5433` (not `5432`) for the test Postgres container to avoid collision with the system Postgres instance. The `make test-integration` target manages its own `docker compose up/down` — do not add a separate postgres service to the CI job for this.
