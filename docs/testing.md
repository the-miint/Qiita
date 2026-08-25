# Test layout and tiers

The three test tiers, what infrastructure each needs, and where the shared
fixtures live. Referenced from [`CLAUDE.md`](../CLAUDE.md).

The test suite is split into three tiers by the infrastructure each one needs:

- **Pure-unit** (no infrastructure): `make test`. Pure Python + Rust unit tests across all components. Excludes tests carrying the `db` marker.
- **Control-plane with DB**: `make test-control-plane-with-db`. Brings up Postgres on :5433 (or uses host Postgres via `QIITA_USE_HOST_POSTGRES=1`), applies dbmate migrations, and runs every control-plane test including the `db`-marked ones. Tests opt in either at module scope (`pytestmark = pytest.mark.db` — pulls every test in the file into the DB tier) or per-test (`@pytest.mark.db` decorator on the function — for mixed modules where only some tests need a DB).
- **Cross-component integration**: `make test-integration`. Same Postgres, plus builds the data-plane debug binary; runs the Python integration suite, then resets the `qiita_ducklake` catalog and runs the Rust DuckLake tests. System tests (`@pytest.mark.system`) are excluded — run those with `make test-system`.

**Shared fixtures across tiers**: the DB / session / OIDC-JWKS fixtures live in `qiita-control-plane/src/qiita_control_plane/testing/` and are imported by both the control-plane and integration conftests so they cannot drift.

**Postgres harness**: `docker-compose.yml` + `initdb/` live under `qiita-control-plane/tests/_postgres/` and are reused by both DB-bound tiers. Port `5433` (not `5432`) avoids collision with a host Postgres.

**DuckLake catalog reset between phases**: `make test-integration` runs the Python suite, drops and recreates the `qiita_ducklake` Postgres database, then runs the Rust suite. DuckLake pins `DATA_PATH` into the catalog at creation time and the two suites use different `DATA_PATH` values; reusing the catalog produces confusing "path mismatch" failures. The Python conftest has the same drop/recreate logic so a single phase is self-contained too — keep the two mechanisms in sync.
