# qiita-data-plane

Data layer for Qiita. Serves bulk measurement data via the Arrow Flight protocol (gRPC). Intentionally "dumb" — operates on opaque integer identifiers assigned by the control plane; no metadata, no business logic.

**Stack:** Rust, arrow-flight, tonic (gRPC), DuckDB v1.5.4 + duckdb-miint extension, DuckLake with Postgres catalog

**Responsibilities:**
- `DoGet` — select by key (table + identifiers from a signed Flight ticket)
- `DoPut` — stream RecordBatches to the shared filesystem
- `DoAction` — register Parquet files into DuckLake, delete by key, insert from processing method
- Verifies JWTs (AuthRocket JWKS) and Flight ticket signatures (HMAC-SHA256)
- Validates `processed_prep_sample_idx` integrity before registration (subset check + duplicate check)
- Calls back to the control plane on upload completion or failure
- Runs as the `qiita-data` system user; rejects result files that are not mode `440`

**Horizontally scalable:** each instance holds an independent DuckDB+DuckLake connection to the shared Postgres catalog. DuckLake's snapshot-isolated read model means instances never block each other. nginx load-balances gRPC traffic across all instances.

## Development

```sh
cargo build --release
cargo test
cargo clippy -- -D warnings && cargo fmt --check
```

The service listens on port `50051` (gRPC). Health check via the standard gRPC health protocol (`grpc.health.v1.Health/Check`).
