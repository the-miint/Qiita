# qiita-compute-orchestrator

Python service that owns the full compute job lifecycle for Qiita. SLURM jobs themselves are kept intentionally dumb (read input, process, write output, exit); this service handles everything else.

**Stack:** Python 3.14, FastAPI, uvicorn, uv

**Responsibilities:**
- Submit jobs to SLURM via slurmrestd REST API
- Poll for job status and detect completion or failure
- Verify output files (correct identifiers, sorted column order, mode `440`)
- Collect job logs and store them
- Report results back to the control plane via REST callback
- Uses a dedicated `compute` service account with narrow permissions (update work tickets and request file registration only)

The compute backend is abstracted behind a `ComputeBackend` interface — SLURM is the primary backend, with a secondary offload backend as a future extension point.

## Development

```sh
uv sync
uv run pytest
uv run ruff check . && uv run ruff format --check .
```

The service listens on port `8081`. Health endpoint: `GET /health`.
