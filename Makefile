.PHONY: build test test-python test-rust test-integration test-workflows lint lint-python lint-rust deploy migrate sync-actions clean verify-health dev-setup install-hooks
.PHONY: build-common build-control-plane build-data-plane build-data-plane-debug build-compute-orchestrator build-integration build-workflows
.PHONY: test-common test-control-plane-without-db test-control-plane-with-db test-data-plane test-compute-orchestrator
.PHONY: lint-common lint-control-plane lint-data-plane lint-compute-orchestrator

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

LOCAL_BIN       := $(HOME)/.local/bin
GRPCURL_VERSION := 1.9.3

ifeq ($(UNAME_S),Linux)
  ifeq ($(UNAME_M),aarch64)
    DBMATE_ARCH  := linux-arm64
    GRPCURL_ARCH := linux_arm64
  else
    DBMATE_ARCH  := linux-amd64
    GRPCURL_ARCH := linux_x86_64
  endif
else ifeq ($(UNAME_S),Darwin)
  ifeq ($(UNAME_M),arm64)
    DBMATE_ARCH  := macos-arm64
    GRPCURL_ARCH := osx_arm64
  else
    DBMATE_ARCH  := macos-amd64
    GRPCURL_ARCH := osx_x86_64
  endif
endif

DBMATE_BIN  := $(LOCAL_BIN)/dbmate
GRPCURL_BIN := $(LOCAL_BIN)/grpcurl

# Build all components
build: build-common build-control-plane build-data-plane build-compute-orchestrator build-integration build-workflows

build-common:
	cd qiita-common && uv sync

# --reinstall-package qiita-common forces uv to rebuild the path-installed
# copy of qiita-common. Plain `uv sync` short-circuits when the dep's version
# string is unchanged, leaving a stale copy in .venv/.../site-packages/ that
# produces confusing ImportErrors for newly-added symbols.
build-control-plane:
	cd qiita-control-plane && uv sync --reinstall-package qiita-common

build-data-plane:
	cd qiita-data-plane && cargo build --release --features duckdb/bundled

# Debug binary used by Python integration tests (fast to build; dynamically
# links libduckdb from target/duckdb-download via DUCKDB_DOWNLOAD_LIB).
build-data-plane-debug:
	cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo build

build-compute-orchestrator:
	cd qiita-compute-orchestrator && uv sync --reinstall-package qiita-common

# tests/integration has its own venv that path-installs all three Python
# packages, so all three need --reinstall-package to avoid the same stale-
# source short-circuit described above.
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

# Postgres bring-up / tear-down macros (shared by test-control-plane-with-db
# and test-integration). On macOS CI, set QIITA_USE_HOST_POSTGRES=1 to use a
# host-provisioned Postgres instead of Docker (Docker is not available on
# macos-latest).
ifeq ($(QIITA_USE_HOST_POSTGRES),1)
PG_BRINGUP  := true
PG_TEARDOWN := true
# Host mode reads PGHOST/PGPORT/PGUSER/PGPASSWORD from the environment.
PG_PSQL     := psql
else
PG_BRINGUP  := docker compose up -d --wait
PG_TEARDOWN := docker compose down
PG_PSQL     := docker compose exec -T postgres psql -U qiita
endif

PG_COMPOSE_DIR := qiita-control-plane/tests/_postgres

# Run unit tests (no infrastructure required)
test: test-python test-rust

test-python: test-common test-control-plane-without-db test-compute-orchestrator

test-rust: test-data-plane

test-common: build-common
	cd qiita-common && uv run pytest

# Run only the control-plane tests that do not need a database. The DB-bound
# tests carry the `db` marker (set at module level via
# `pytestmark = pytest.mark.db`) and are excluded here; run them via
# `make test-control-plane-with-db`.
test-control-plane-without-db: build-control-plane
	cd qiita-control-plane && uv run pytest -m 'not db'

# Run the full control-plane suite, including DB-bound tests. Brings up
# Postgres + applies dbmate migrations; tears down on exit. Set
# QIITA_USE_HOST_POSTGRES=1 to skip Docker bring-up and use a host Postgres.
test-control-plane-with-db: build-control-plane $(DBMATE_BIN)
	(cd $(PG_COMPOSE_DIR) && $(PG_BRINGUP)) && \
	  ((cd qiita-control-plane && uv run pytest); PY_EC=$$?; \
	   (cd $(PG_COMPOSE_DIR) && $(PG_TEARDOWN)); \
	   exit $$PY_EC)

test-data-plane:
	cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo test

test-compute-orchestrator: build-compute-orchestrator
	cd qiita-compute-orchestrator && uv run pytest

# Smoke-test workflow containers (requires apptainer; skips gracefully if absent)
test-workflows:
	@if ! command -v apptainer > /dev/null 2>&1; then \
		echo "apptainer not found — skipping workflow smoke tests"; \
		exit 0; \
	fi
	apptainer build --force /tmp/qiita-workflow-smoke.sif workflows/amplicon/Apptainer.def
	apptainer exec /tmp/qiita-workflow-smoke.sif echo "hello world"
	rm -f /tmp/qiita-workflow-smoke.sif

# Run integration tests (requires Docker for Postgres, OR set
# QIITA_USE_HOST_POSTGRES=1 to use a Postgres provisioned outside this Makefile
# — useful on macOS where Docker isn't available; CI uses this on macos-latest).
# Runs Python integration tests + Rust DuckLake tests (which need Postgres).
# System tests (real GG2 data) are excluded — use make test-system.
# Builds the data plane debug binary first so Python tests can spawn it
# without shelling out to cargo.
#
# The qiita_ducklake catalog is dropped/recreated between the Python and Rust
# phases because DuckLake pins DATA_PATH into the catalog, and the two suites
# use different DATA_PATH values (Python picks a pytest tmp_path_factory dir,
# Rust defaults to /tmp/qiita-integration-ducklake-data). Mirrors the Python
# _reset_ducklake_catalog() helper in tests/integration/conftest.py.
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

# Run system tests with real GG2 backbone data. Intended to be invoked
# manually before cutting a release candidate — not part of the default
# CI matrix. Requires Docker + data in localdocs/scratch/, or
# QIITA_USE_HOST_POSTGRES=1 with a host-provisioned Postgres.
#
# Auto-skips when the GG2 dataset is absent: the test's
# pytest.mark.skipif gate yields a "1 skipped" pytest summary and exit 0
# rather than an error, so running this target on a machine without the
# data is harmless. See tests/integration/test_system_gg2_backbone.py
# for the expected file paths under localdocs/scratch/.
#
# Slow (~10 min): hashes 331K sequences, mints features, writes chunked Parquet.
test-system: build-data-plane-debug build-integration
	(cd $(PG_COMPOSE_DIR) && $(PG_BRINGUP)) && \
	  ((cd tests/integration && uv run pytest -m system -x --timeout=2700); PY_EC=$$?; \
	   (cd $(PG_COMPOSE_DIR) && $(PG_TEARDOWN)); \
	   exit $$PY_EC)

# Lint all components
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

$(DBMATE_BIN):
	mkdir -p $(LOCAL_BIN)
	curl -fsSL -o $@ https://github.com/amacneil/dbmate/releases/latest/download/dbmate-$(DBMATE_ARCH)
	chmod +x $@

$(GRPCURL_BIN):
	mkdir -p $(LOCAL_BIN)
	curl -fsSL https://github.com/fullstorydev/grpcurl/releases/download/v$(GRPCURL_VERSION)/grpcurl_$(GRPCURL_VERSION)_$(GRPCURL_ARCH).tar.gz \
	  | tar -xz -C $(LOCAL_BIN) grpcurl

# Run database migrations
migrate: $(DBMATE_BIN)
	cd qiita-control-plane && $(DBMATE_BIN) --migrations-table public.schema_migrations --no-dump-schema up

# Sync action YAMLs from workflows/ into qiita.action.
# Idempotent: only writes YAML-authoritative columns; operational columns
# (enabled, first_seen_at, disabled_*) are preserved across runs. CI runs
# this on deploy after `make migrate`; operators can re-run by hand for
# manual recovery (e.g., to push a hotfixed YAML without redeploying).
# Reads DATABASE_URL from env, same as `make migrate`.
sync-actions:
	cd qiita-control-plane && uv run qiita-admin actions sync --workflows-dir ../workflows

# Build and print deploy instructions (no sudo)
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

# Verify all services are healthy after deploy
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

# Check developer tool prerequisites and print install instructions only for missing ones
dev-setup:
	@echo "=== Qiita developer setup ==="
	@echo ""
	@if command -v uv > /dev/null 2>&1; then \
		echo "  uv         ✓  $$(uv --version)"; \
	else \
		echo "--- uv: NOT installed (required) ---"; \
		echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"; \
		echo ""; \
	fi
	@if command -v cargo > /dev/null 2>&1; then \
		echo "  Rust/cargo ✓  $$(cargo --version)"; \
	else \
		echo "--- Rust/cargo: NOT installed (required) ---"; \
		echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"; \
		echo ""; \
	fi
	@if command -v psql > /dev/null 2>&1; then \
		echo "  PostgreSQL ✓  $$(psql --version)"; \
	else \
		echo "--- PostgreSQL: NOT installed (required) ---"; \
		echo "  NOTE: Postgres serves two roles: the control-plane app DB and the"; \
		echo "        DuckLake catalog for the data plane."; \
		echo "  Debian/Ubuntu:"; \
		echo "    sudo apt-get install -y postgresql && sudo systemctl enable --now postgresql"; \
		echo "  RHEL/CentOS/Fedora:"; \
		echo "    sudo dnf install -y postgresql-server postgresql-contrib"; \
		echo "    sudo postgresql-setup --initdb && sudo systemctl enable --now postgresql"; \
		echo "  macOS:"; \
		echo "    brew install postgresql@17 && brew services start postgresql@17"; \
		echo ""; \
	fi
	@if command -v apptainer > /dev/null 2>&1; then \
		echo "  apptainer  ✓  $$(apptainer --version)"; \
	else \
		echo "--- apptainer: NOT installed (optional; workflow containers are built in CI if absent) ---"; \
		echo "  macOS: not natively supported — 'make build' and 'make test' skip gracefully."; \
		echo "    Use Lima for a Linux VM if needed: brew install lima"; \
		echo "  Debian/Ubuntu amd64:"; \
		echo "    APPTAINER_VERSION=1.4.5"; \
		echo "    sudo apt-get install -y fuse2fs uidmap"; \
		echo "    wget https://github.com/apptainer/apptainer/releases/download/v\$${APPTAINER_VERSION}/apptainer_\$${APPTAINER_VERSION}_amd64.deb"; \
		echo "    sudo dpkg -i apptainer_\$${APPTAINER_VERSION}_amd64.deb && rm apptainer_\$${APPTAINER_VERSION}_amd64.deb"; \
		echo "  RHEL/CentOS/Fedora amd64:"; \
		echo "    APPTAINER_VERSION=1.4.5"; \
		echo "    wget https://github.com/apptainer/apptainer/releases/download/v\$${APPTAINER_VERSION}/apptainer-\$${APPTAINER_VERSION}-1.x86_64.rpm"; \
		echo "    sudo dnf install -y ./apptainer-\$${APPTAINER_VERSION}-1.x86_64.rpm && rm apptainer-\$${APPTAINER_VERSION}-1.x86_64.rpm"; \
		echo ""; \
	fi
	@echo ""
	@echo "NOTE: dbmate and grpcurl are fetched automatically by 'make migrate' and"
	@echo "      'make verify-health' — no manual install needed."
	@echo ""
	@echo "After setup: make install-hooks && make build && make test && make lint"

# Install and activate pre-commit hooks (run once after cloning)
install-hooks:
	uv tool install pre-commit
	pre-commit install

clean:
	cd qiita-common && rm -rf .venv __pycache__ .pytest_cache .ruff_cache
	cd qiita-control-plane && rm -rf .venv __pycache__ .pytest_cache .ruff_cache
	cd qiita-data-plane && cargo clean
	cd qiita-compute-orchestrator && rm -rf .venv __pycache__ .pytest_cache .ruff_cache
