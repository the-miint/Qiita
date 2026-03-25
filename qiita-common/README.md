# qiita-common

Shared Python library consumed by `qiita-control-plane` and `qiita-compute-orchestrator` as a path dependency.

**Contents:**
- Pydantic models for work ticket states and API request/response schemas
- Config patterns shared across services
- REST client utilities

Keeping the contract in one place prevents drift between the two services' understanding of the API.

## Development

```sh
uv sync
uv run pytest
uv run ruff check . && uv run ruff format --check .
```

## Usage

Referenced as a path dependency in sibling components' `pyproject.toml`:

```toml
[tool.uv.sources]
qiita-common = { path = "../qiita-common" }
```
