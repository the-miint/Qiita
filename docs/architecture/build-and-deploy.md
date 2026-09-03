# Build, Layout and CI

## Deployment

On-premise Linux, systemd services. Local dev on macOS.

The `make deploy` target builds all components and prints the required admin commands for systemd/nginx installation. An admin executes the privileged commands manually.

The data plane is deployed as multiple systemd instances of the `qiita-data-plane@.service` template. The instance specifier *is* the listen port — `qiita-data-plane@50051` binds `127.0.0.1:50051`, `qiita-data-plane@50052` binds `:50052`, etc. nginx upstream `qiita_data_plane` (in `deploy/nginx/qiita.conf`) load-balances gRPC traffic across the configured ports. Instance count is tunable without code changes — only the nginx upstream block and the number of systemd units need updating.

## Monorepo Structure

```
qiita/
├── Makefile                        # unified entry point: build, test, lint, deploy, migrate
├── .github/
│   └── workflows/
│       └── ci.yml                  # lint + unit/integration tests across components (no CI deploy)
├── qiita-common/
│   ├── pyproject.toml              # shared Pydantic models, config, client utilities
│   └── src/
│       └── qiita_common/
│           ├── __init__.py
│           ├── models/                     # work-ticket / API schemas, principal + action types (domain submodules re-exported via models/__init__.py)
│           ├── api_paths.py                # canonical REST path constants (shared CP↔CO)
│           ├── auth_constants.py           # scope names, token prefixes
│           ├── config.py                   # env-var loading helpers
│           ├── log.py                      # structured-logging setup
│           ├── client.py                   # base async REST client for service-to-service
│           ├── compute_backend_client.py   # CP → orchestrator /step/* client (submit/status/result/find-by-name)
│           ├── backend_failure.py          # typed BackendFailure model + JSON round-trip
│           ├── actions.py                  # action YAML schema + loader
│           └── parquet.py                  # parquet column/sort helpers
├── qiita-control-plane/
│   ├── pyproject.toml              # uv-managed, depends on qiita-common
│   ├── uv.lock
│   ├── db/
│   │   └── migrations/             # dbmate SQL migration files
│   ├── src/
│   │   └── qiita_control_plane/
│   │       ├── __init__.py
│   │       ├── main.py             # FastAPI app entry point + /health endpoint
│   │       ├── config.py           # settings (DB URL, Flight signing seed, cookie secret, AuthRocket JWKS URL)
│   │       ├── db.py               # asyncpg connection pool setup
│   │       ├── deps.py             # FastAPI dependency-injection helpers (sessions, scopes)
│   │       ├── dispatch.py         # dispatch + reconcile_inflight_tickets (restart re-attach)
│   │       ├── runner/             # per-ticket workflow runner package (walks action steps; drives submit→poll→result)
│   │       ├── step_progress.py    # qiita.work_ticket_step writers/readers (restart-recovery spine)
│   │       ├── auth/               # JWT verification, Ed25519 ticket signing, AuthRocket integration
│   │       ├── actions/            # action library + sync from workflows/
│   │       ├── cli/                # qiita-admin CLI surface
│   │       ├── repositories/       # asyncpg query layer per resource (biosample, study, user, ...)
│   │       ├── testing/            # shared test fixtures (postgres, sessions, JWKS harness)
│   │       └── routes/
│   │           ├── _helpers.py              # response shaping shared by sibling route modules
│   │           ├── admin.py                 # admin endpoints (service-account mint, role grants, ...)
│   │           ├── alignment.py             # sharded-alignment config identity (alignment_idx minting)
│   │           ├── assembly.py              # per-run contig DoGet ticket minting
│   │           ├── auth.py                  # login flow + PAT mint + handoff
│   │           ├── biosample.py             # biosample import + study-scoped metadata / field routes
│   │           ├── host_filter_profile.py   # read-only host-filter profile catalog
│   │           ├── prep_protocol.py         # prep-protocol discovery for the bcl-convert flow
│   │           ├── prep_sample.py           # prep-sample reads, retirement, study-local field create
│   │           ├── read.py                  # block-read DoGet ticket minting
│   │           ├── read_masked.py           # mask_idx minting + masked-read DoGet ticket
│   │           ├── reference.py             # reference CRUD, membership, genome/feature minting
│   │           ├── sequence_range.py        # contiguous sequence-range allocation per prep_sample
│   │           ├── sequenced_sample.py      # sequenced-sample import + study-scoped reads / metadata
│   │           ├── sequencing_run.py        # sequencing-run + sequenced-pool mint routes
│   │           ├── study.py
│   │           ├── upload.py                # generic Arrow-data staging slots + DoPut ticket
│   │           ├── user.py
│   │           └── work_ticket.py           # work-ticket CRUD + Flight ticket issuance
│   └── tests/
│       ├── conftest.py
│       ├── _postgres/              # docker-compose.yml + initdb for Postgres harness (shared with tests/integration)
│       ├── auth/
│       ├── cli/
│       ├── repositories/
│       └── routes/
├── qiita-data-plane/
│   ├── Cargo.toml                  # deps: arrow-flight, tonic, duckdb, ed25519-dalek, sha2
│   └── src/
│       ├── main.rs                 # tonic server entry, Flight service + gRPC health check registration
│       ├── config.rs               # settings (DuckLake catalog DB URL, Flight public key)
│       ├── flight_service.rs       # impl FlightService trait (do_get, do_put, do_action)
│       ├── auth.rs                 # Ed25519 Flight-ticket verification (public key)
│       └── ducklake.rs             # DuckDB/DuckLake connection management, ducklake_add_data_files
├── qiita-compute-orchestrator/
│   ├── pyproject.toml              # uv-managed, depends on qiita-common
│   ├── uv.lock
│   ├── src/
│   │   └── qiita_compute_orchestrator/
│   │       ├── __init__.py
│   │       ├── main.py             # service entry point + /health; lifespan runs jobs/ boot scan
│   │       ├── config.py           # settings (compute backend, shared FS root, CP↔CO token, SLURM creds)
│   │       ├── backend.py          # ComputeBackend abstract base (submit/status/result/find-by-name + aclose)
│   │       ├── step.py             # /api/v1/step/{submit,status,result,find-by-name} routes + submit-time prefix check
│   │       ├── backends/
│   │       │   ├── local.py        # LocalBackend (DuckDB + miint in-process; dev / test)
│   │       │   └── slurm.py        # SlurmBackend (slurmrestd dispatch + polling)
│   │       ├── jobs/
│   │       │   ├── __init__.py     # run_native_job framework dispatcher + boot-time scan
│   │       │   ├── __main__.py     # `python -m` SLURM launcher (params.json → run_native_job)
│   │       │   └── fastq_to_parquet.py  # native job: FASTQ → Parquet via DuckDB + miint
│   │       │                            # (per-sample, sequenced_sample-scoped)
│   │       ├── miint.py            # shared miint install + DuckDB-conn helpers, PARQUET_OPTS
│   │       └── slurm/
│   │           ├── client.py       # slurmrestd REST client
│   │           ├── contract.py     # shared constants + JobParams: EXPECTED_FILE_MODE,
│   │           │                   # MANIFEST_FILENAME, JOB_PARAMS_FILENAME, JobParams (params.json shape)
│   │           ├── payload.py      # JSON job-submit payload builder (container + native scripts)
│   │           └── verify.py       # post-job output verification (mode 440, identifier sort)
│   └── tests/
│       └── conftest.py
├── tests/
│   └── integration/
│       ├── conftest.py             # cross-component fixtures: postgres, services, dataplane binary
│       ├── _pg_env.py              # postgres connection helpers (Docker vs host mode)
│       ├── _runner_helpers.py      # workflow-runner test helpers
│       ├── test_smoke.py
│       ├── test_doget.py           # CP-signed ticket → DP DoGet round-trip
│       ├── test_step_dispatch.py   # CP → orchestrator /step/submit flow
│       ├── test_action_library.py
│       ├── test_action_sync.py
│       ├── test_reference_add_smoke.py
│       ├── test_e2e_reference.py
│       └── test_system_gg2_backbone.py  # @pytest.mark.system; real GG2 backbone
├── workflows/
│   ├── amplicon/
│   │   ├── Apptainer.def           # container definition (single image for all steps)
│   │   ├── workflow.yaml           # ordered steps: name, type (map|reduce), entrypoint, resources
│   │   └── scripts/                # per-step entrypoints and helpers
│   └── reference-add/
│       └── 1.0.0.yaml              # versioned reference-ingest workflow
├── deploy/
│   ├── systemd/
│   │   ├── qiita-control-plane.service
│   │   ├── qiita-data-plane@.service       # template unit; instance = listen port (e.g. @50051)
│   │   └── qiita-compute-orchestrator.service
│   └── nginx/
│       └── qiita.conf              # REST and gRPC routing, TLS termination, HTTP/2
└── .gitignore
```

## Build System (Makefile)

The unified build entry point lives in [`Makefile`](../../Makefile). The recipes below mirror the public-API targets verbatim; the test in [`qiita-common/tests/test_makefile_doc_sync.py`](../../qiita-common/tests/test_makefile_doc_sync.py) asserts they stay in sync and is part of `make test`. Internal helpers (`$(DBMATE_BIN)` / `$(GRPCURL_BIN)` auto-fetch, the `UNAME_S/UNAME_M` arch detection, the verbose `dev-setup` install hints) live in `Makefile` only.

<!-- KEEP IN SYNC WITH ../Makefile; qiita-common/tests/test_makefile_doc_sync.py enforces this -->
```makefile
# Build
build: build-common build-control-plane build-data-plane build-compute-orchestrator build-integration build-workflows

build-common:
	cd qiita-common && uv sync

build-control-plane:
	cd qiita-control-plane && uv sync --reinstall-package qiita-common

build-data-plane:
	cd qiita-data-plane && cargo build --release --features duckdb/bundled

build-data-plane-debug:
	cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo build

build-compute-orchestrator:
	cd qiita-compute-orchestrator && uv sync --reinstall-package qiita-common

build-integration:
	cd tests/integration && uv sync \
	  --reinstall-package qiita-common \
	  --reinstall-package qiita-control-plane \
	  --reinstall-package qiita-compute-orchestrator

build-workflows:
	@if ! command -v apptainer > /dev/null 2>&1; then \
		echo "apptainer not found — skipping workflow container builds"; \
		exit 0; \
	fi; \
	for dir in workflows/*/; do \
		if [ -f "$$dir/Apptainer.def" ]; then \
			apptainer build "$$dir/$$(basename $$dir).sif" "$$dir/Apptainer.def"; \
		fi \
	done

# Test (layered by infrastructure cost)
test: test-python test-rust

test-python: test-common test-control-plane-without-db test-compute-orchestrator

test-rust: test-data-plane

test-common: build-common
	cd qiita-common && uv run pytest

test-control-plane-without-db: build-control-plane
	cd qiita-control-plane && uv run pytest -n auto --dist worksteal -m 'not db'

test-control-plane-with-db: build-control-plane $(DBMATE_BIN)
	(cd $(PG_COMPOSE_DIR) && $(PG_BRINGUP)) && \
	  ((cd qiita-control-plane && uv run pytest -n auto --dist worksteal); PY_EC=$$?; \
	   (cd $(PG_COMPOSE_DIR) && $(PG_TEARDOWN)); \
	   exit $$PY_EC)

test-data-plane:
	cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo test

test-compute-orchestrator: build-compute-orchestrator
	cd qiita-compute-orchestrator && uv run pytest

test-workflows:
	@if ! command -v apptainer > /dev/null 2>&1; then \
		echo "apptainer not found — skipping workflow smoke tests"; \
		exit 0; \
	fi; \
	set -e; \
	apptainer build --force /tmp/qiita-workflow-smoke.sif workflows/amplicon/Apptainer.def; \
	apptainer exec /tmp/qiita-workflow-smoke.sif echo "hello world"; \
	rm -f /tmp/qiita-workflow-smoke.sif; \
	smoke_derived=$$(mktemp -d); trap 'rm -rf "$$smoke_derived"' EXIT; \
	mkdir -p "$$smoke_derived/images"; \
	PATH_DERIVED="$$smoke_derived" bash scripts/build-sif.sh _sif-build-smoke

test-integration: build-data-plane-debug build-integration $(DBMATE_BIN)
	(cd $(PG_COMPOSE_DIR) && $(PG_BRINGUP)) && \
	  ((cd tests/integration && uv run pytest -m 'not system'); PY_EC=$$?; \
	   (cd $(PG_COMPOSE_DIR) && $(PG_PSQL) -d postgres \
	     -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'qiita_ducklake' AND pid != pg_backend_pid()" \
	     -c "DROP DATABASE IF EXISTS qiita_ducklake" \
	     -c "CREATE DATABASE qiita_ducklake OWNER qiita"); \
	   (cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo test --features integration); RS_EC=$$?; \
	   (cd $(PG_COMPOSE_DIR) && $(PG_TEARDOWN)); \
	   exit $$(( PY_EC > RS_EC ? PY_EC : RS_EC )))

test-system: build-data-plane-debug build-integration
	(cd $(PG_COMPOSE_DIR) && $(PG_BRINGUP)) && \
	  ((cd tests/integration && uv run pytest -m system -x --timeout=5400); PY_EC=$$?; \
	   (cd $(PG_COMPOSE_DIR) && $(PG_TEARDOWN)); \
	   exit $$PY_EC)

# Lint
lint: lint-python lint-rust

lint-python: lint-common lint-control-plane lint-compute-orchestrator

lint-rust: lint-data-plane

lint-common:
	cd qiita-common && uv run ruff check . && uv run ruff format --check .

lint-control-plane:
	cd qiita-control-plane && uv run ruff check . && uv run ruff format --check .

lint-data-plane:
	cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo clippy -- -D warnings && cargo fmt --check

lint-compute-orchestrator:
	cd qiita-compute-orchestrator && uv run ruff check . && uv run ruff format --check .

# DB / actions
migrate: $(DBMATE_BIN)
	cd qiita-control-plane && $(DBMATE_BIN) --migrations-table public.schema_migrations --no-dump-schema up

sync-actions:
	cd qiita-control-plane && uv run qiita-admin actions sync --workflows-dir ../workflows

# Deploy / health
deploy: build
	@echo "=== Build complete. Run the following commands as admin: ==="
	@echo ""
	@echo "  sudo cp deploy/systemd/qiita-control-plane.service /etc/systemd/system/"
	@echo "  sudo cp deploy/systemd/qiita-data-plane@.service /etc/systemd/system/"
	@echo "  sudo cp deploy/systemd/qiita-compute-orchestrator.service /etc/systemd/system/"
	@echo "  sudo cp deploy/nginx/qiita.conf /etc/nginx/conf.d/"
	@echo "  sudo systemctl daemon-reload"
	@echo "  sudo systemctl restart qiita-control-plane"
	@echo "  sudo systemctl restart 'qiita-data-plane@50051'"
	@echo "  sudo systemctl restart qiita-compute-orchestrator"
	@echo "  sudo systemctl reload nginx"
	@echo ""
	@echo "Then verify: make verify-health"

verify-health: $(GRPCURL_BIN)
	@echo "Checking control plane..."
	@curl -sf http://localhost:8080/health || (echo "FAIL: control plane" && exit 1)
	@echo " OK"
	@echo "Checking compute orchestrator..."
	@curl -sf http://localhost:8081/health || (echo "FAIL: compute orchestrator" && exit 1)
	@echo " OK"
	@echo "Checking data plane..."
	@$(GRPCURL_BIN) -plaintext localhost:50051 grpc.health.v1.Health/Check || (echo "FAIL: data plane" && exit 1)
	@echo " OK"
	@echo "All services healthy."

# Setup / hooks
install-hooks:
	uv tool install pre-commit
	pre-commit install

# Cleanup
clean:
	cd qiita-common && rm -rf .venv __pycache__ .pytest_cache .ruff_cache
	cd qiita-control-plane && rm -rf .venv __pycache__ .pytest_cache .ruff_cache
	cd qiita-data-plane && cargo clean
	cd qiita-compute-orchestrator && rm -rf .venv __pycache__ .pytest_cache .ruff_cache
```

## CI (GitHub Actions)

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: make lint

  test-unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: make test

  test-integration:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:17
        env:
          POSTGRES_PASSWORD: test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: make test-integration
```
