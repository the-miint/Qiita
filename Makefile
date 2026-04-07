.PHONY: build test test-python test-rust test-integration test-workflows lint lint-python lint-rust deploy migrate clean verify-health dev-setup install-hooks
.PHONY: build-common build-control-plane build-data-plane build-compute-orchestrator build-workflows
.PHONY: test-common test-control-plane test-data-plane test-compute-orchestrator
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
    DBMATE_ARCH  := darwin-arm64
    GRPCURL_ARCH := osx_arm64
  else
    DBMATE_ARCH  := darwin-amd64
    GRPCURL_ARCH := osx_x86_64
  endif
endif

DBMATE_BIN  := $(LOCAL_BIN)/dbmate
GRPCURL_BIN := $(LOCAL_BIN)/grpcurl

# Build all components
build: build-common build-control-plane build-data-plane build-compute-orchestrator build-workflows

build-common:
	cd qiita-common && uv sync

build-control-plane:
	cd qiita-control-plane && uv sync

build-data-plane:
	cd qiita-data-plane && cargo build --release --features duckdb/bundled

build-compute-orchestrator:
	cd qiita-compute-orchestrator && uv sync

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

# Run unit tests
test: test-python test-rust

test-python: test-common test-control-plane test-compute-orchestrator

test-rust: test-data-plane

test-common:
	cd qiita-common && uv run pytest

test-control-plane:
	cd qiita-control-plane && uv run pytest

test-data-plane:
	cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo test

test-compute-orchestrator:
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

# Run integration tests (requires Docker for Postgres)
test-integration:
	cd tests/integration && docker compose up -d --wait && \
	  (uv run pytest; EC=$$?; docker compose down; exit $$EC)

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
