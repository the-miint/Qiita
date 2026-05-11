# Qiita

> [!WARNING]
> **This project is under active development. Do not use.**

Scalable multi-omic study management, processing, and analysis platform for microbiome data (amplicon, metagenomic, metatranscriptomic, metabolomic, proteomic). Designed for millions of samples and hundreds of TB of data.

See [`docs/architecture.md`](docs/architecture.md) for a full system overview.

## Components

| Component | Language | Role |
|---|---|---|
| [`qiita-control-plane`](qiita-control-plane/) | Python / FastAPI | REST API — study/sample/prep CRUD, search, work tickets |
| [`qiita-data-plane`](qiita-data-plane/) | Rust / Arrow Flight | Bulk data I/O via gRPC — DuckDB + DuckLake over Parquet |
| [`qiita-compute-orchestrator`](qiita-compute-orchestrator/) | Python / FastAPI | Job lifecycle — submit, poll, verify, and report via slurmrestd |
| [`qiita-common`](qiita-common/) | Python | Shared Pydantic models, config, and REST client utilities |

## Prerequisites

Run `make dev-setup` for exact install commands. Required tools:

- **uv** — Python dependency management (all Python components)
- **Rust / cargo** — data plane build
- **PostgreSQL** — local dev DB and DuckLake catalog (must be running before `make migrate`)
- **apptainer** — workflow container builds (Linux only; skipped gracefully on macOS)
- **dbmate** — database migrations (`make migrate`); auto-installed by the target
- **grpcurl** — `make verify-health` only; auto-installed by the target

## Installation

```sh
# 1. Check tool prerequisites. Prints install commands only for what's missing.
make dev-setup

# 2. Install pre-commit hooks (one-time, per clone).
make install-hooks

# 3. Install deps for all components.
make build

# 4. Create a local `.env` from the committed template (`.env` is gitignored).
cp .env.example .env

# 5. Edit .env: fill in DATABASE_URL, HMAC_SECRET_KEY, CONTROL_PLANE_URL,
#    and DUCKLAKE_CATALOG_CONNSTR. Then source it:
. .env

# 6. Make sure Postgres is running and DATABASE_URL points at a reachable host
#    before `make migrate` (which does not start Postgres or create roles):
#      macOS:   brew services start postgresql@17
#      Linux:   sudo systemctl start postgresql
#    On Linux, a fresh apt/dnf install ships only a peer-auth `postgres` role,
#    so first-time setup also needs a superuser role for your OS user:
#      sudo -u postgres createuser -s $USER

# 7. Create the `qiita` database if absent, then run all pending migrations.
make migrate
```

## Development

```sh
# Pure-unit tests (no infrastructure)
make test

# Full control-plane suite including DB-bound tests. Brings up Docker Postgres
# (or set QIITA_USE_HOST_POSTGRES=1 to use a host Postgres — see
# docs/runbooks/integration-tests-host-postgres.md, which also covers test-integration).
make test-control-plane-with-db

# Cross-component integration tests (same Postgres requirement as above)
make test-integration

# Lint
make lint

# Clean build artifacts
make clean
```

## Deployment

```sh
make deploy        # builds and prints systemd + nginx instructions
make verify-health # checks all three services are up
```

Services run as systemd units. nginx routes REST traffic to the control plane and gRPC to the data plane (load-balanced across N instances).
