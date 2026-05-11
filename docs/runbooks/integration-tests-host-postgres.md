# Integration tests on host Postgres (no Docker)

**Purpose.** Run `make test-integration` against a Homebrew-installed Postgres
on the host instead of the default Docker Compose stack. This is the path
GitHub Actions takes on `macos-latest`, where Docker isn't available on the
runner. On Linux and on macOS setups that do have Docker, the default Docker
mode works without any of this — `make test-integration` brings up its own
container on `:5433` and tears it down at the end.

> ⚠ This setup currently requires editing several config values that are
> duplicated across the codebase. The exact list below was assembled by
> reading the Makefile, `tests/integration/_pg_env.py`,
> `qiita-control-plane/tests/_postgres/docker-compose.yml`, and
> `.github/workflows/ci.yml`. A future refactor should consolidate these.

## When to use this runbook

- macOS without Docker (or with Docker disabled)
- Reproducing a CI failure that only appears on the macOS leg

### Prerequisites

- macOS with Homebrew installed
- Nothing else listening on `:5432` — stop any pre-existing system Postgres
  before running the setup script, or it will fail
- Shell with no `PGSSLMODE=require` exported (see Troubleshooting)

### Note re Postgres version

The Docker fixture ([`qiita-control-plane/tests/_postgres/docker-compose.yml`](../../qiita-control-plane/tests/_postgres/docker-compose.yml))
and CI's macOS leg ([`.github/workflows/ci.yml`](../../.github/workflows/ci.yml))
both run Postgres 17. Nothing in the codebase enforces a major version for
host-mode local runs, but if the goal is to match CI, install `postgresql@17`
— version drift can surface in DuckLake's Postgres-extension behavior even
when plain SQL works fine. (`make dev-setup` separately recommends
`postgresql@17` for production-style local development.)

## Setup

Run this once after cloning the repo (or after dropping the test objects to
reprovision — see the cleanup block at the end of this section). It installs
Postgres 17, starts the service, and creates the `qiita` test-fixture role
and the two databases the integration tests need.

```bash
# --- One-time install ---
brew install postgresql@17
brew services start postgresql@17

export PATH="$(brew --prefix postgresql@17)/bin:$PATH"

# Wait for the server to actually be ready before the role/db CREATEs:
for i in {1..30}; do pg_isready -h localhost -p 5432 && break; sleep 1; done

# --- One-time: create the role and the two databases the tests use.
# Hardcoded test-fixture credentials, not secrets. qiita_test is the
# control-plane app DB (matches what docker-compose.yml provisions in
# Docker mode); qiita_ducklake is the DuckLake catalog DB, which the Rust
# integration suite drops and recreates between phases. ---
psql -d postgres -c "CREATE USER qiita WITH SUPERUSER PASSWORD 'qiita';"
psql -d postgres -c "CREATE DATABASE qiita_test OWNER qiita;"
psql -d postgres -c "CREATE DATABASE qiita_ducklake OWNER qiita;"
```

## Test Run

Run this to export the seven
environment variables `make test-integration` reads in host mode.
Must be run once in every shell where testing is being done.

```bash
# --- Persistent: env vars the test harness reads. Must be set in every
# shell that runs `make test-integration` in host mode. Missing any one
# produces a confusing failure (see Troubleshooting and the table below). ---
export PATH="$(brew --prefix postgresql@17)/bin:$PATH"
export QIITA_USE_HOST_POSTGRES=1
export QIITA_TEST_POSTGRES_URL='postgresql://qiita:qiita@localhost:5432/qiita_test?sslmode=disable'
export DUCKLAKE_CATALOG_CONNSTR='dbname=qiita_ducklake host=localhost port=5432 user=qiita password=qiita sslmode=disable'
export PGHOST=localhost
export PGPORT=5432
export PGUSER=qiita
export PGPASSWORD=qiita
```

## Run the tests

```bash
make test-integration
```

### Reprovisioning

If you need to reprovision (e.g. to start from a clean state, or after
changing test-fixture credentials), drop the objects first then re-run the
setup block above:

```bash
psql -d postgres -c "DROP DATABASE IF EXISTS qiita_ducklake;"
psql -d postgres -c "DROP DATABASE IF EXISTS qiita_test;"
psql -d postgres -c "DROP USER IF EXISTS qiita;"
```

## Why each variable matters

| Variable | Read by | Default if unset | Failure if missing |
|---|---|---|---|
| `QIITA_USE_HOST_POSTGRES` | Makefile | unset → Docker mode | `docker compose` runs, conflicts with host Postgres |
| `QIITA_TEST_POSTGRES_URL` | `tests/integration/_pg_env.py:27` | `postgresql://qiita:qiita@localhost:5433/qiita_test?sslmode=disable` | dbmate / Python tests try `:5433`, fail with "connection refused" |
| `DUCKLAKE_CATALOG_CONNSTR` | `tests/integration/_pg_env.py:31`, Rust DuckLake tests | `dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita sslmode=disable` | Rust tests panic with "Failed to attach DuckLake MetaData" on `:5433` |
| `PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD` | `psql` invocations in Makefile's `test-integration` recipe | libpq's own defaults (often wrong) | catalog reset between Python and Rust phases fails silently or runs against the wrong server |

## Troubleshooting

- **`SSL is not enabled on the server`** — your shell has `PGSSLMODE=require`
  exported, which overrides the per-connection `sslmode=disable` in the env
  vars above. Run `unset PGSSLMODE` and try again.
- **`connection refused` on `:5433`** — host mode listens on `:5432`. A
  `:5433` error means one of `QIITA_TEST_POSTGRES_URL` or
  `DUCKLAKE_CATALOG_CONNSTR` is unset and the test harness fell back to its
  Docker-mode default. Re-run the persistent `export` lines from the setup
  block in your current shell.
- **`role "qiita" does not exist`** — the one-time setup hasn't run, or the
  role was dropped. Re-run the `CREATE USER` / `CREATE DATABASE` commands.
- **`database "qiita_test" already exists`** — setup was already run.
  Either skip the role/db creation, or use the cleanup block to drop the
  existing objects first.
- **Tests pass locally but the macOS CI leg fails** — verify your local
  Postgres major version matches CI's `postgresql@17`. The catalog reset SQL
  and DuckLake's Postgres extension are version-sensitive.
- **`make test-integration` invokes `docker compose` even though
  `QIITA_USE_HOST_POSTGRES=1` is set** — confirm the variable is exported
  (`export QIITA_USE_HOST_POSTGRES=1`, not just assigned), and that you're
  invoking `make` from the same shell where you exported it.

## Related files (for future consolidation)

The values above are duplicated across these locations and must be kept in
sync until the broader cleanup happens:

- [`tests/integration/_pg_env.py`](../../tests/integration/_pg_env.py)
  — Python test defaults
- [`qiita-control-plane/tests/_postgres/docker-compose.yml`](../../qiita-control-plane/tests/_postgres/docker-compose.yml)
  — Docker-mode Postgres user/password/db/port/major version
- [`tests/integration/conftest.py`](../../tests/integration/conftest.py)
  — DB-name literals in the catalog-reset SQL
- [`qiita-data-plane/src/ducklake.rs`](../../qiita-data-plane/src/ducklake.rs)
  — Rust test-fixture connection string fallback
- [`Makefile`](../../Makefile) — `PG_PSQL`, `test-integration` recipe SQL,
  the `dev-setup` Postgres install hint
- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) — the macOS
  provisioning steps mirror the setup block above
