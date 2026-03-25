# qiita-control-plane

Client-facing REST API for Qiita. The authoritative source for all identifiers (`study_idx`, `sample_idx`, `prep_idx`, etc.) — every uint64 in the system is minted here.

**Stack:** Python 3.14, FastAPI, asyncpg, Postgres, dbmate, uv

**Responsibilities:**
- CRUD for studies, samples, and preparations
- Metadata search and access-control gating (returns authorized identifier sets to clients)
- Work ticket creation, status management, and processing deduplication enforcement
- Signs Flight tickets (HMAC-SHA256) authorizing client access to the data plane
- Orchestrates DuckLake file registration after compute completes
- JWT verification via cached AuthRocket JWKS

## Development

```sh
uv sync
uv run pytest
uv run ruff check . && uv run ruff format --check .
```

Migrations (requires Postgres):

```sh
dbmate up
```

The service listens on port `8080`. Health endpoint: `GET /health`.
