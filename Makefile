.PHONY: build test test-integration lint deploy migrate clean verify-health dev-setup
.PHONY: build-common build-control-plane build-data-plane build-compute-orchestrator build-workflows
.PHONY: test-common test-control-plane test-data-plane test-compute-orchestrator
.PHONY: lint-common lint-control-plane lint-data-plane lint-compute-orchestrator

# Build all components
build: build-common build-control-plane build-data-plane build-compute-orchestrator build-workflows

build-common:
	cd qiita-common && uv sync

build-control-plane:
	cd qiita-control-plane && uv sync

build-data-plane:
	cd qiita-data-plane && cargo build --release

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
test: test-common test-control-plane test-data-plane test-compute-orchestrator

test-common:
	cd qiita-common && uv run pytest

test-control-plane:
	cd qiita-control-plane && uv run pytest

test-data-plane:
	cd qiita-data-plane && cargo test

test-compute-orchestrator:
	cd qiita-compute-orchestrator && uv run pytest

# Run integration tests (requires Docker for Postgres)
test-integration:
	cd tests/integration && docker compose up -d --wait
	cd tests/integration && uv run pytest
	cd tests/integration && docker compose down

# Lint all components
lint: lint-common lint-control-plane lint-data-plane lint-compute-orchestrator

lint-common:
	cd qiita-common && uv run ruff check . && uv run ruff format --check .

lint-control-plane:
	cd qiita-control-plane && uv run ruff check . && uv run ruff format --check .

lint-data-plane:
	cd qiita-data-plane && cargo clippy -- -D warnings && cargo fmt --check

lint-compute-orchestrator:
	cd qiita-compute-orchestrator && uv run ruff check . && uv run ruff format --check .

# Run database migrations
migrate:
	cd qiita-control-plane && dbmate up

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
	@echo "  sudo systemctl restart 'qiita-data-plane@1'"
	@echo "  sudo systemctl restart qiita-compute-orchestrator"
	@echo "  sudo systemctl reload nginx"
	@echo ""
	@echo "Then verify: make verify-health"

# Verify all services are healthy after deploy
verify-health:
	@echo "Checking control plane..."
	@curl -sf http://localhost:8080/health || (echo "FAIL: control plane" && exit 1)
	@echo " OK"
	@echo "Checking compute orchestrator..."
	@curl -sf http://localhost:8081/health || (echo "FAIL: compute orchestrator" && exit 1)
	@echo " OK"
	@echo "Checking data plane..."
	@grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check || (echo "FAIL: data plane" && exit 1)
	@echo " OK"
	@echo "All services healthy."

# Print developer setup instructions for required tools not managed by uv/cargo
dev-setup:
	@echo "=== Qiita developer setup ==="
	@echo ""
	@echo "--- uv (all platforms, no sudo) ---"
	@echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
	@echo ""
	@echo "--- Rust/cargo (all platforms, no sudo) ---"
	@echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
	@echo ""
	@echo "--- apptainer ---"
	@echo "  NOTE: apptainer requires Linux kernel namespaces and cannot run natively on macOS."
	@echo "  macOS (ARM or Intel): workflow container builds are not supported locally."
	@echo "    Use Lima for a local Linux VM if needed:  brew install lima"
	@echo "    Otherwise, container images are built in CI; 'make build' skips gracefully."
	@echo ""
	@echo "  Debian/Ubuntu amd64  (sudo required for system package install):"
	@echo "    APPTAINER_VERSION=1.4.1"
	@echo "    sudo apt-get install -y fuse2fs uidmap"
	@echo "    wget https://github.com/apptainer/apptainer/releases/download/v\$${APPTAINER_VERSION}/apptainer_\$${APPTAINER_VERSION}_amd64.deb"
	@echo "    sudo dpkg -i apptainer_\$${APPTAINER_VERSION}_amd64.deb"
	@echo "    rm apptainer_\$${APPTAINER_VERSION}_amd64.deb"
	@echo ""
	@echo "  RHEL/CentOS/Fedora amd64  (sudo required for system package install):"
	@echo "    APPTAINER_VERSION=1.4.1"
	@echo "    wget https://github.com/apptainer/apptainer/releases/download/v\$${APPTAINER_VERSION}/apptainer-\$${APPTAINER_VERSION}-1.x86_64.rpm"
	@echo "    sudo dnf install -y ./apptainer-\$${APPTAINER_VERSION}-1.x86_64.rpm"
	@echo "    rm apptainer-\$${APPTAINER_VERSION}-1.x86_64.rpm"
	@echo ""
	@echo "  Verify: apptainer --version"
	@echo ""
	@echo "--- dbmate (no sudo — installs to ~/.local/bin) ---"
	@echo "  mkdir -p ~/.local/bin"
	@echo "  Linux amd64:"
	@echo "    curl -fsSL -o ~/.local/bin/dbmate https://github.com/amacneil/dbmate/releases/latest/download/dbmate-linux-amd64"
	@echo "    chmod +x ~/.local/bin/dbmate"
	@echo "  macOS ARM:"
	@echo "    brew install dbmate"
	@echo "  Ensure ~/.local/bin is on PATH (Linux):  echo 'export PATH=\$$HOME/.local/bin:\$$PATH' >> ~/.bashrc"
	@echo ""
	@echo "--- grpcurl, for make verify-health (no sudo — installs to ~/.local/bin) ---"
	@echo "  GRPCURL_VERSION=1.9.3"
	@echo "  Linux amd64:"
	@echo "    mkdir -p ~/.local/bin"
	@echo "    wget -qO- https://github.com/fullstorydev/grpcurl/releases/download/v\$${GRPCURL_VERSION}/grpcurl_\$${GRPCURL_VERSION}_linux_x86_64.tar.gz \\"
	@echo "      | tar -xz -C ~/.local/bin grpcurl"
	@echo "  macOS ARM:"
	@echo "    brew install grpcurl"
	@echo ""
	@echo "After setup: make build && make test && make lint"

clean:
	cd qiita-common && rm -rf .venv __pycache__ .pytest_cache .ruff_cache
	cd qiita-control-plane && rm -rf .venv __pycache__ .pytest_cache .ruff_cache
	cd qiita-data-plane && cargo clean
	cd qiita-compute-orchestrator && rm -rf .venv __pycache__ .pytest_cache .ruff_cache
