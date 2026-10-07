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
│   ├── pull_request_template.md    # PR description skeleton, incl. the Reviewer loop block
│   └── workflows/
│       ├── ci.yml                  # lint + unit/integration tests across components (no CI deploy)
│       └── review-loop-check.yml   # the PR description carries the Reviewer loop block
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
│   │           ├── prep_sample.py           # prep-sample reads, retirement, study-local field CRUD
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
│   │   └── 1.0.0.yaml              # versioned amplicon denoise workflow
│   ├── golay-demux/
│   │   └── 1.0.0.yaml              # versioned bcl-convert + Golay demux workflow
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

The unified build entry point lives in [`Makefile`](../../Makefile). The recipes below mirror the public-API targets verbatim; the test in [`qiita-common/tests/test_makefile_doc_sync.py`](../../qiita-common/tests/test_makefile_doc_sync.py) asserts they stay in sync and is part of `make test`. Internal helpers (`$(DBMATE_BIN)` / `$(GRPCURL_BIN)` auto-fetch, the `UNAME_S/UNAME_M` arch detection, the verbose `dev-setup` install hints) live in `Makefile` only, as do the make-level comments — the sync test compares recipe bodies, so read `Makefile` for why a recipe is shaped the way it is (`test-workflows`' single-line apptainer guard and its `_sif-build-smoke` half are the ones that need it).

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
	set -ex; \
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
on:
  # Scope push to long-lived branches only. PR branches get a single
  # pull_request run — avoids the duplicate push+PR runs (and the PR-only
  # gates showing up as "Skipped" on a meaningless push-event run).
  push:
    branches: [main]
  # `labeled` is here so adding the `ci-macos` label re-runs CI with the macOS
  # matrix (see the config job) — the default types don't fire on a label change.
  pull_request:
    types: [opened, synchronize, reopened, labeled]

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  # Which OSes the matrix jobs fan out over. macOS runners are ~6-15× slower than
  # Ubuntu for this suite and dominate PR wall-clock, while the deploy target is
  # Linux — so PRs run Ubuntu-only for fast feedback and macOS coverage runs on
  # every push to main (post-merge).
  #
  # The gap that leaves: a defect only macOS can see is found AFTER merge, with
  # main already red. The test DB is isolated per xdist WORKER, so a test leaking
  # rows into the shared DB only collides with an innocent test when the runner's
  # core count co-locates them on one worker — which is precisely a bug the Ubuntu
  # matrix cannot see. Label a PR `ci-macos` to fan out over macOS BEFORE merging;
  # use it for anything touching shared test fixtures or cross-test DB state.
  #
  # Emitted as a JSON array string for `fromJSON` in each matrix below.
  config:
    runs-on: ubuntu-latest
    outputs:
      os: ${{ steps.os.outputs.os }}
    steps:
      - id: os
        env:
          WANT_MACOS: ${{ github.event_name != 'pull_request'
            || contains(github.event.pull_request.labels.*.name, 'ci-macos') }}
        run: |
          if [ "$WANT_MACOS" = "true" ]; then
            echo 'os=["ubuntu-latest", "macos-latest"]' >> "$GITHUB_OUTPUT"
          else
            echo 'os=["ubuntu-latest"]' >> "$GITHUB_OUTPUT"
          fi

  lint-python:
    needs: config
    strategy:
      fail-fast: false
      matrix:
        os: ${{ fromJSON(needs.config.outputs.os) }}
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v5
      - uses: astral-sh/setup-uv@v7
      - run: make lint-python

  # Lint and test the Rust workspace in one job: clippy/fmt and the test build
  # share a checkout, toolchain, and warm rust-cache, and a single job avoids
  # two concurrent jobs racing to write the same cache. lint runs first, so a
  # clippy/fmt failure still stops the job before the test compile (matching the
  # old test-rust -> lint-rust dependency). The one gating change: test-workflows
  # now waits on this whole job (lint + test) rather than the lint alone.
  rust:
    needs: config
    strategy:
      fail-fast: false
      matrix:
        os: ${{ fromJSON(needs.config.outputs.os) }}
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
        with:
          # Defensive: lint needs both. dtolnay defaults already include
          # them, but pinning the request makes the intent explicit so a
          # future default change can't silently drop one.
          components: clippy, rustfmt
      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: qiita-data-plane
          prefix-key: "v1"  # bump to invalidate stale bundled-feature cache
          # macOS preinstalls a `cargo` shim that is actually rustup-init,
          # and ~/.cargo/bin/ isn't first on PATH. Caching that directory
          # can shadow the toolchain dtolnay just installed and break
          # `cargo clippy` on cache restore. Disable bin caching here.
          cache-bin: false
      - uses: ./.github/actions/setup-libduckdb
        with:
          runtime-libpath: "true"   # cargo test dlopens libduckdb
          cache-extensions: "true"
      - run: make lint-rust
      - run: make test-rust

  test-python:
    needs: [config, lint-python]
    strategy:
      fail-fast: false
      matrix:
        os: ${{ fromJSON(needs.config.outputs.os) }}
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v5
      - uses: astral-sh/setup-uv@v7
      - run: make test-python

  test-control-plane-with-db:
    needs: [config, lint-python]
    strategy:
      fail-fast: false
      matrix:
        os: ${{ fromJSON(needs.config.outputs.os) }}
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v5
      - uses: astral-sh/setup-uv@v7
      - uses: ./.github/actions/setup-host-postgres
        if: runner.os == 'macOS'
      - run: make test-control-plane-with-db

  test-integration:
    needs: [config, test-python, rust]
    strategy:
      fail-fast: false
      matrix:
        os: ${{ fromJSON(needs.config.outputs.os) }}
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v5
      - uses: astral-sh/setup-uv@v7
      # make test-integration builds the data-plane debug binary and the Rust
      # DuckLake test binary, which without a cargo cache recompiles ~all deps
      # (incl. the duckdb crate) cold — ~80s, the single largest slice of this
      # job. Pin the toolchain + warm rust-cache + provide libduckdb (same setup
      # as the `rust` job) so that compile is incremental on repeat runs.
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: qiita-data-plane
          prefix-key: "v1"  # bump to invalidate stale bundled-feature cache
          cache-bin: false  # see the rust job for rationale
      - uses: ./.github/actions/setup-libduckdb
        with:
          runtime-libpath: "true"   # spawned data-plane + cargo test dlopen libduckdb
          cache-extensions: "true"
      - uses: ./.github/actions/setup-host-postgres
        if: runner.os == 'macOS'
        with:
          ducklake: "true"
      - run: make test-integration

  test-workflows:
    needs: [lint-python, rust]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: actions/cache@v5
        with:
          path: ~/.apptainer/cache
          key: apptainer-${{ runner.os }}-ubuntu-24.04
      - name: Install Apptainer
        run: |
          APPTAINER_VERSION=1.4.5
          sudo apt-get install -y fuse2fs uidmap
          wget -q https://github.com/apptainer/apptainer/releases/download/v${APPTAINER_VERSION}/apptainer_${APPTAINER_VERSION}_amd64.deb
          sudo dpkg -i apptainer_${APPTAINER_VERSION}_amd64.deb
          rm apptainer_${APPTAINER_VERSION}_amd64.deb
      - run: make test-workflows

  # Enforces the DEPLOY_CHECKLIST.md "## Pending deploy" fold (CLAUDE.md "Operator-facing
  # changes"). If a PR touches an operator-impacting surface but never edits
  # DEPLOY_CHECKLIST.md, it almost certainly forgot to run /deploy-note. Heuristic, not
  # content-aware — the 'no-deploy-note' label opts out a PR that genuinely needs
  # no operator action. No Claude involved; pure git diff.
  deploy-note-check:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
        with:
          fetch-depth: 0
      - name: Require a Pending-deploy fold for operator-impacting changes
        env:
          BASE_REF: ${{ github.base_ref }}
          LABELS: ${{ join(github.event.pull_request.labels.*.name, ',') }}
        run: |
          set -euo pipefail
          case ",$LABELS," in *,no-deploy-note,*)
            echo "Skipped: 'no-deploy-note' label present."; exit 0;;
          esac
          git fetch --quiet origin "$BASE_REF"
          changed=$(git diff --name-only "origin/$BASE_REF...HEAD")
          # High-signal operator-impacting surfaces. A change here almost always
          # means env / migration / workflow / scope action on the deploy host.
          impacting=$(printf '%s\n' "$changed" | grep -E \
            -e '\.env\..*\.example$' \
            -e '^qiita-control-plane/db/migrations/' \
            -e '^workflows/' \
            -e '^qiita-control-plane/src/qiita_control_plane/auth/scopes\.py$' || true)
          if [ -z "$impacting" ]; then
            echo "No operator-impacting changes — deploy-note fold not required."
            exit 0
          fi
          echo "Operator-impacting files changed:"; printf '%s\n' "$impacting" | sed 's/^/  /'
          if printf '%s\n' "$changed" | grep -qx 'DEPLOY_CHECKLIST.md'; then
            echo "DEPLOY_CHECKLIST.md updated — assuming the steps were folded into ## Pending deploy."
            exit 0
          fi
          {
            echo "ERROR: this PR changes operator-impacting surfaces (above) but does not touch DEPLOY_CHECKLIST.md."
            echo "Fold the operator steps into the '## Pending deploy' buckets — run /deploy-note on this branch."
            echo "If this PR genuinely needs no operator action (a migration the dbmate flow handles on its"
            echo "own, a workflow with no new env/scope, etc.), add the 'no-deploy-note' label to say so."
          } >&2
          exit 1

  # Every PR records what it changed in CHANGELOG.md (the per-change log, distinct
  # from the operator deploy checklist DEPLOY_CHECKLIST.md). Pure git diff, no
  # Claude. A PR that genuinely warrants no entry (typo, CI-only, the changelog
  # tooling itself) opts out with the 'no-changelog' label.
  changelog-check:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
        with:
          fetch-depth: 0
      - name: Require a CHANGELOG.md entry
        env:
          BASE_REF: ${{ github.base_ref }}
          LABELS: ${{ join(github.event.pull_request.labels.*.name, ',') }}
        run: |
          set -euo pipefail
          case ",$LABELS," in *,no-changelog,*)
            echo "Skipped: 'no-changelog' label present."; exit 0;;
          esac
          git fetch --quiet origin "$BASE_REF"
          changed=$(git diff --name-only "origin/$BASE_REF...HEAD")
          if printf '%s\n' "$changed" | grep -qx 'CHANGELOG.md'; then
            echo "CHANGELOG.md updated."
            exit 0
          fi
          {
            echo "ERROR: this PR does not touch CHANGELOG.md."
            echo "Add an entry under '## [Unreleased]' describing what changed, tagged (#N)."
            echo "If this PR genuinely needs no changelog entry (typo, CI-only change, etc.),"
            echo "add the 'no-changelog' label to opt out."
          } >&2
          exit 1
```
