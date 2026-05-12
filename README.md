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

**Setup by area.** For first-time setup; existing contributors can skim. Most contributions don't need the full sequence below — pick the closest fit:

| What you're doing | What you need |
|---|---|
| Pure-unit tests on any component (`make test` for Python, `cargo test` in qiita-data-plane for Rust) | Steps 1–3 only. No env files. No Postgres. |
| CP routes / schemas / DB-bound tests (`make test-control-plane-with-db`) | Add Postgres + `.env.control-plane` only. |
| Cross-component / DB-bound Rust / `make test-integration` | Full setup below (both `.env.control-plane` *and* `.env.data-plane`; the DP needs its DuckLake catalog DB, which `make migrate` does not create). |
| Docs / scripts only | Step 1; `make lint` to check. |

These are common entry points, not an exhaustive map — pick the closest fit and add what your change needs.

```sh
# 1. Check tool prerequisites. Prints install commands only for what's missing.
make dev-setup

# 2. Install pre-commit hooks (one-time, per clone).
make install-hooks

# 3. Install deps for all components.
make build

# 4. Create local env files from the committed templates. One template per
#    component (each is the same artifact production installs into
#    /etc/qiita/ — see docs/runbooks/first-deploy.md). The stripped-suffix
#    copies are gitignored.
for svc in control-plane data-plane compute-orchestrator; do
    cp ".env.$svc.example" ".env.$svc"
done

# 5. Generate the shared HMAC secret once and sed-substitute it into both
#    .env.control-plane and .env.data-plane (CP and DP must agree byte-for-byte;
#    doing it here avoids the failure mode where you edit one file by hand,
#    forget the other, and the second-sourced placeholder silently wins).
HMAC_SECRET_KEY=$(openssl rand -base64 32)
for f in .env.control-plane .env.data-plane; do
    sed -i.bak "s|^HMAC_SECRET_KEY=.*|HMAC_SECRET_KEY=$HMAC_SECRET_KEY|" "$f"
    rm "$f.bak"
done

# 6. Edit each .env.<svc> to fill in the remaining placeholders:
#       .env.control-plane            DATABASE_URL <username>
#       .env.data-plane               DUCKLAKE_CATALOG_CONNSTR <username>
#    Then source all three into your shell:
set -a
source .env.control-plane
source .env.data-plane
source .env.compute-orchestrator
set +a

# 7. Make sure Postgres is running and DATABASE_URL points at a reachable host
#    before `make migrate` (which does not start Postgres or create roles):
#      macOS:   brew services start postgresql@17
#      Linux:   sudo systemctl start postgresql
#    On Linux, a fresh apt/dnf install ships only a peer-auth `postgres` role,
#    so first-time setup also needs a superuser role for your OS user:
#      sudo -u postgres createuser -s $USER

# 8. Create the `qiita` database if absent, then run all pending migrations.
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

The full first-deploy procedure — env files installed under `/etc/qiita/`,
systemd units enabled, AuthRocket realm wired up, data-plane bootstrap, and
the end-to-end smoke — lives in
[`docs/runbooks/first-deploy.md`](docs/runbooks/first-deploy.md). The same
`.env.<component>.example` templates the Installation section above uses are
what the runbook installs into `/etc/qiita/`; the dev path and the prod path
consume the same artifacts.

```sh
make deploy        # builds and prints systemd + nginx instructions
make verify-health # checks all three services are up
```

Services run as systemd units. nginx routes REST traffic to the control plane and gRPC to the data plane (load-balanced across N instances).
