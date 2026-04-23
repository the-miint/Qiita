# Qiita

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
- **dbmate** — database migrations (`make migrate`)
- **apptainer** — workflow container builds (Linux only; skipped gracefully on macOS)
- **grpcurl** — `make verify-health` only

## Installation

```sh
# Install deps for all components
make build


# Before running migrations or starting services, create a local `.env` from the committed template and source it (`.env` is already gitignored):

cp .env.example .env

# EDIT: ----------------------------------
# edit .env and fill in DATABASE_URL, HMAC_SECRET_KEY, CONTROL_PLANE_URL, and DUCKLAKE_CATALOG_CONNSTR
# ----------------------------------------

. .env


# create the `qiita` database if it does not already exist, then runs all pending migrations:

make migrate
```

## Development

```sh
# Unit tests
make test

# Lint
make lint

# Integration tests (requires Docker)
make test-integration

# Clean build artifacts
make clean
```

## Deployment

```sh
make deploy        # builds and prints systemd + nginx instructions
make verify-health # checks all three services are up
```

Services run as systemd units. nginx routes REST traffic to the control plane and gRPC to the data plane (load-balanced across N instances).
