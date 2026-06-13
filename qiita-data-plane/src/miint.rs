//! miint extension contract for the data plane — the cross-language twin of
//! `qiita_common.duckdb_miint` (Python). Rust can't import that module, so the
//! `MIINT_EXTENSION_REPO` / `MIINT_EXTENSION_DIRECTORY` env contract is
//! duplicated here. KEEP IN SYNC with
//! `qiita-common/src/qiita_common/duckdb_miint.py` (deliberate, like the
//! Python<->Postgres enum parity).
//!
//! Like every other Qiita component, the data plane uses a SINGLE deploy-staged
//! miint build: the deploy installs it once into the shared
//! `MIINT_EXTENSION_DIRECTORY` (`scripts/stage-miint-extension.sh`) and the data
//! plane LOADs from there — it never installs its own. Two installed builds
//! drifting apart is the nightmare this avoids.
//!
//! Production use: open the connection with [`miint_config`] (which sets
//! `allow_unsigned_extensions` + the staged `extension_directory`), then call
//! [`load_miint`] — never `INSTALL`. `MIINT_EXTENSION_REPO` / [`miint_repo`] are
//! read only for an install (deploy staging / tests), never on the LOAD path.
//!
//! `#![allow(dead_code)]`: the data plane does not LOAD miint in production yet —
//! these helpers are wired and exercised by the `miint_extension_smoke` test so
//! the first real use is a clean LOAD-from-staged, not a fresh install. Drop the
//! allow when a production path starts calling them.
#![allow(dead_code)]

use duckdb::{Config, Connection};

/// Team mirror — the single source of truth for the miint build. Keep in sync
/// with `MIINT_MIRROR_URL` in `qiita_common.duckdb_miint`.
pub const MIINT_MIRROR_URL: &str = "https://ftp.microbio.me/pub/miint";

/// The miint extension repo: `MIINT_EXTENSION_REPO` override, else the team
/// mirror. Only an INSTALL (deploy staging / tests) reads this; the cluster
/// LOAD path never does.
pub fn miint_repo() -> String {
    std::env::var("MIINT_EXTENSION_REPO").unwrap_or_else(|_| MIINT_MIRROR_URL.to_string())
}

/// DuckDB connection config for loading miint: unsigned extensions are always
/// allowed (the mirror build carries the team's signing chain, not DuckDB's),
/// and the deploy-staged `extension_directory` is used when
/// `MIINT_EXTENSION_DIRECTORY` is set. Mirrors
/// `qiita_common.duckdb_miint.miint_connect_config`. `allow_unsigned_extensions`
/// is a startup-only DuckDB config, so it is set here at open rather than via a
/// later `SET`.
pub fn miint_config() -> Result<Config, duckdb::Error> {
    let mut config = Config::default().allow_unsigned_extensions()?;
    if let Ok(dir) = std::env::var("MIINT_EXTENSION_DIRECTORY") {
        config = config.with("extension_directory", dir.as_str())?;
    }
    Ok(config)
}

/// LOAD the pre-staged miint extension on `conn`. The data plane never INSTALLs
/// in production — the deploy stages one build into `MIINT_EXTENSION_DIRECTORY`
/// and every component LOADs it. Mirrors
/// `qiita_common.duckdb_miint.miint_load_sql`.
pub fn load_miint(conn: &Connection) -> Result<(), duckdb::Error> {
    conn.execute_batch("LOAD miint;")
}
