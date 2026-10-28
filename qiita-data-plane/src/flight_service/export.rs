//! The `export_read`, `count_masked` and `mask_metrics` DoActions and the read
//! helpers they share with the block-read DoGet.

use std::path::{Path, PathBuf};

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Action;
use duckdb::Connection;
use futures::stream;
use tonic::{Response, Status};

use crate::auth;

use super::QiitaFlightService;

impl QiitaFlightService {
    pub(super) async fn export_read_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload = auth::verify_export_read(&action.body, &self.flight_public_key)
            .map_err(|e| Status::unauthenticated(e.to_string()))?;

        if payload.action != "export_read" {
            return Err(Status::invalid_argument(format!(
                "action type mismatch: header says 'export_read', payload says {:?}",
                payload.action
            )));
        }

        // Defense in depth on the signature-trusted destination before it is
        // inlined into a DuckDB `COPY ... TO` literal and written to.
        let dest = validate_export_dest(&payload.dest, &self.scratch_root)?;

        // `COPY` is long-lived for a whole prep_sample.
        let scratch_root = self.scratch_root.clone();
        let prep = payload.prep_sample_idx;
        let count = self
            .blocking_lake("export_read", move |conn| {
                export_read_to_parquet(conn, prep, &dest, &scratch_root)
            })
            .await?;

        let result_body = serde_json::to_vec(&serde_json::json!({
            "count": count,
            "dest": payload.dest,
        }))
        .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;

        let result = arrow_flight::Result {
            body: result_body.into(),
        };
        let output = stream::once(futures::future::ready(Ok(result)));
        Ok(Response::new(Box::pin(output)))
    }

    pub(super) async fn count_masked_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        // Reuse the *DoGet* read_masked ticket rather than minting a
        // bespoke action token: counting the rows a ticket selects is
        // strictly less than streaming them, so the ticket's
        // (prep_sample_idx, mask_idx) authorization already covers it —
        // no new ticket type or control-plane route is needed, the CLI
        // sends the same signed bytes it would otherwise stream with.
        let payload = auth::verify_ticket(&action.body, &self.flight_public_key)
            .map_err(|e| Status::unauthenticated(e.to_string()))?;
        if payload.table != "read_masked" {
            return Err(Status::invalid_argument(format!(
                "count_masked requires a read_masked ticket, got table {:?}",
                payload.table
            )));
        }
        // Refused, not ignored — the same rule the DoGet projection
        // follows. Unreachable today (`read_masked` takes no projection,
        // so the control plane cannot sign one) and harmless if it were
        // reached, since this returns a count and no rows. It is here
        // because "a signed field this arm silently disregards" is the one
        // shape that turns a narrowed ticket into a wider answer, and this
        // was the only place in the ticket surface still allowing it.
        if !payload.columns.is_empty() {
            return Err(Status::invalid_argument(
                "count_masked takes no projection column list",
            ));
        }
        let prep_sample_idx = single_i64_filter(&payload.filter, "prep_sample_idx")?;
        let mask_idx = single_i64_filter(&payload.filter, "mask_idx")?;

        let count = self
            .blocking_lake("count_masked", move |conn| {
                count_masked_reads(conn, prep_sample_idx, mask_idx)
            })
            .await?;

        let result_body = serde_json::to_vec(&serde_json::json!({ "count": count }))
            .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;
        let result = arrow_flight::Result {
            body: result_body.into(),
        };
        let output = stream::once(futures::future::ready(Ok(result)));
        Ok(Response::new(Box::pin(output)))
    }

    pub(super) async fn mask_metrics_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        // Unlike `count_masked` (which reuses a `read_masked` DoGet
        // ticket the CLI already holds), the block reconcile primitive
        // runs control-plane-side and signs a first-class action token —
        // so this arm verifies a `mask_metrics` payload, not a ticket.
        let payload = auth::verify_mask_metrics(&action.body, &self.flight_public_key)
            .map_err(|e| Status::unauthenticated(e.to_string()))?;
        if payload.action != "mask_metrics" {
            return Err(Status::invalid_argument(format!(
                "action type mismatch: header says 'mask_metrics', payload says {:?}",
                payload.action
            )));
        }
        let mask_idx = payload.mask_idx;
        let prep_sample_idx = payload.prep_sample_idx;

        // A count over the light `read_mask` table, not over `read`.
        let counts = self
            .blocking_lake("mask_metrics", move |conn| {
                mask_metrics_counts(conn, mask_idx, prep_sample_idx)
            })
            .await?;

        let result_body = serde_json::to_vec(&counts)
            .map_err(|e| Status::internal(format!("json serialization failed: {e}")))?;
        let result = arrow_flight::Result {
            body: result_body.into(),
        };
        let output = stream::once(futures::future::ready(Ok(result)));
        Ok(Response::new(Box::pin(output)))
    }
}

/// Canonical Parquet write options for an exported reads file: a copy of
/// `qiita_common.parquet.PARQUET_OPTS_TRANSFER`, pinned equal to it by a qiita-common
/// test. The materialized file has the shape of the
/// durable copy `ingest_reads` first wrote (Parquet v2, zstd, 64 MB row groups), but
/// a job reads it and it is dropped, so it takes zstd's default level rather than
/// the at-rest one.
const EXPORT_READ_PARQUET_OPTS: &str =
    "FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'zstd', ROW_GROUP_SIZE_BYTES '64MB'";

/// The read projection, in `read` / `read_masked` table order. Shared by the
/// per-prep_sample `export_read` DoAction (from `qiita_lake.read`) and by BOTH
/// block-read DoGet selectors (`read_block` from `qiita_lake.read`,
/// `read_masked_block` from the `read_masked` MACRO), so every read payload the
/// data plane hands a compute job has the identical column shape — the shape
/// `align_sharded.reads` / the read-mask jobs bind. `read_masked` exposes exactly
/// these columns (plus `mask_idx`), already trimmed and `pass`-filtered.
pub(super) const EXPORT_READ_COLUMNS: &str =
    "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2";

/// Validate a control-plane-signed `export_read` destination before the data
/// plane writes to it. The token is signature-trusted, so this is defense in depth:
/// the dest must be absolute, contain no single quote (it is inlined into a
/// DuckDB `COPY ... TO '<dest>'` literal), carry no `..`/prefix component, and
/// resolve under the data plane's scratch root (the shared tree the control
/// plane's ticket workspaces live under). Returns the validated path.
pub(super) fn validate_export_dest(dest: &str, scratch_root: &Path) -> Result<PathBuf, Status> {
    let path = Path::new(dest);
    if !path.is_absolute() {
        return Err(Status::invalid_argument(format!(
            "export dest must be an absolute path: {dest:?}"
        )));
    }
    if dest.contains('\'') {
        return Err(Status::invalid_argument(format!(
            "export dest must not contain a single quote: {dest:?}"
        )));
    }
    if path.components().any(|c| {
        matches!(
            c,
            std::path::Component::ParentDir | std::path::Component::Prefix(_)
        )
    }) {
        return Err(Status::invalid_argument(format!(
            "export dest must not contain '..' or a prefix component: {dest:?}"
        )));
    }
    if !path.starts_with(scratch_root) {
        return Err(Status::invalid_argument(format!(
            "export dest {dest:?} is not under the data plane scratch root {}",
            scratch_root.display()
        )));
    }
    Ok(path.to_path_buf())
}

/// Run a caller-supplied `select_sql` (an already-safe SELECT — signature-verified
/// inlined integers only) and materialize its rows into a Parquet at `dest`.
/// Returns the row count.
///
/// Machinery for the `export_read` DoAction — one whole prep_sample from
/// `qiita_lake.read`. (Its block siblings are gone: a block's reads STREAM over
/// the `read_block` / `read_masked_block` DoGet selectors.) The caller builds its
/// own SELECT (same `EXPORT_READ_COLUMNS` projection so the output shape is
/// identical); this function owns the publish. An empty selection writes NO file
/// and returns 0 — the control plane turns that into a clean submission failure.
/// The `COPY` streams row groups to disk, so memory stays bounded regardless of
/// selection size. Takes a borrowed instance so the caller can run it on the
/// blocking pool (mirrors `register_files`).
///
/// `dest` arrives already lexically validated (`validate_export_dest`), but this
/// is human-read data, so we ALSO resolve symlinks: another job on the shared
/// scratch tree could plant a symlink to redirect the write outside the
/// controlled workspace. We `canonicalize` the (created) parent and re-assert it
/// resolves under `scratch_root` before writing. The file is then published
/// atomically (write a sibling `.partial`, then rename) so `dest` only ever
/// appears complete — a failed or partial `COPY` never leaves a half-written
/// `reads.parquet` a retry could read. The row count is read back from the
/// written file, so it always matches the bytes on disk (no separate catalog
/// scan that could race the `COPY`).
fn export_select_to_parquet(
    conn: &Connection,
    select_sql: &str,
    dest: &Path,
    scratch_root: &Path,
) -> Result<i64, Status> {
    // ROW_GROUP_SIZE_BYTES (in EXPORT_READ_PARQUET_OPTS) requires insertion
    // order NOT be preserved — and we don't need it: the file is unordered
    // because every consumer (qc is per-row, host_filter collapses with
    // DISTINCT) is order-independent. The pool's baseline is already off; the
    // SET stays so this requirement does not rest on that baseline.
    conn.execute_batch("SET preserve_insertion_order = false;")
        .map_err(|e| Status::internal(format!("failed to set preserve_insertion_order: {e}")))?;

    let parent = dest.parent().ok_or_else(|| {
        Status::internal(format!("export dest has no parent: {}", dest.display()))
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|e| Status::internal(format!("failed to create {}: {e}", parent.display())))?;

    // Symlink-safe containment: the lexical `validate_export_dest` check is not
    // enough on a shared scratch tree, where another job could plant a symlink
    // that redirects these (human) reads outside the controlled workspace.
    // Canonicalize the now-existing parent and re-assert it resolves under the
    // scratch root before we write anything.
    let real_parent = std::fs::canonicalize(parent)
        .map_err(|e| Status::internal(format!("failed to resolve {}: {e}", parent.display())))?;
    let real_root = std::fs::canonicalize(scratch_root).map_err(|e| {
        Status::internal(format!(
            "failed to resolve scratch root {}: {e}",
            scratch_root.display()
        ))
    })?;
    if !real_parent.starts_with(&real_root) {
        return Err(Status::permission_denied(format!(
            "export dest parent {} resolves outside the scratch root {}",
            real_parent.display(),
            real_root.display()
        )));
    }

    // Write to a sibling temp, then publish atomically. `dest` is validated
    // (absolute, under the scratch root, no `..`, no single quote) and the
    // `.partial` suffix preserves all of that; the `select_sql` carries only
    // signature-verified inlined integers — all safe to inline. Callers project the
    // full `EXPORT_READ_COLUMNS` set in table order, so the file is a drop-in for
    // the durable staging copy (modulo row order, which does not matter).
    let tmp = {
        let mut s = dest.as_os_str().to_os_string();
        s.push(".partial");
        PathBuf::from(s)
    };
    let tmp_sql = tmp
        .to_str()
        .ok_or_else(|| Status::internal(format!("non-UTF-8 dest path: {}", tmp.display())))?;
    let copy_sql = format!("COPY ({select_sql}) TO '{tmp_sql}' ({EXPORT_READ_PARQUET_OPTS})");

    // The fallible sequence is isolated so the temp file is cleaned up on the
    // empty path (count 0) and on any error; on success it is renamed away.
    let published = (|| -> Result<i64, Status> {
        conn.execute_batch(&copy_sql)
            .map_err(|e| Status::internal(format!("read export COPY failed: {e}")))?;
        // Count from the file we just wrote, so it matches the bytes exactly.
        let count: i64 = conn
            .query_row(
                &format!("SELECT count(*) FROM read_parquet('{tmp_sql}')"),
                [],
                |row| row.get(0),
            )
            .map_err(|e| Status::internal(format!("read export count failed: {e}")))?;
        if count == 0 {
            // Nothing selected — publish nothing; the CP raises.
            return Ok(0);
        }
        // Match the read result-file convention: owner/group read-only.
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o440))
            .map_err(|e| Status::internal(format!("failed to chmod {}: {e}", tmp.display())))?;
        std::fs::rename(&tmp, dest).map_err(|e| {
            Status::internal(format!(
                "failed to publish {} -> {}: {e}",
                tmp.display(),
                dest.display()
            ))
        })?;
        Ok(count)
    })();

    // On the empty path (Ok(0)) or any failure the temp still exists; remove it.
    // On success (Ok(n>0)) it was renamed to `dest`, so this is a no-op.
    if !matches!(published, Ok(n) if n > 0) {
        let _ = std::fs::remove_file(&tmp);
    }
    published
}

/// Re-materialize one prep_sample's reads into a per-ticket `reads.parquet` a
/// read-mask job consumes (the per-prep_sample export). A prep_sample with no stored reads
/// writes NO file and returns 0. `prep_sample_idx` is a signature-verified i64, safe
/// to inline. See `export_select_to_parquet` for the shared write/publish.
fn export_read_to_parquet(
    conn: &Connection,
    prep_sample_idx: i64,
    dest: &Path,
    scratch_root: &Path,
) -> Result<i64, Status> {
    export_select_to_parquet(
        conn,
        &format!(
            "SELECT {EXPORT_READ_COLUMNS} FROM qiita_lake.read \
             WHERE prep_sample_idx = {prep_sample_idx}"
        ),
        dest,
        scratch_root,
    )
}

/// Build a block's `read` WHERE clause from its members. The ONE translator from
/// block members to SQL: the block-read DoGet selectors (`build_block_read_query`)
/// and the block DELETE actions all go through it, so a block's read footprint and
/// its delete footprint cannot drift, and the pushdown performance-assessment test
/// exercises the EXACT predicate they emit rather than a hand-written copy.
///
/// Two coarse conjuncts drive pruning (both are top-level, so DuckDB pushes them
/// to the scan): `prep_sample_idx IN (...)` prunes DuckLake data files by their
/// per-file `prep_sample_idx` stats, and `sequence_idx BETWEEN block_min AND
/// block_max` bounds the row-group span. A third conjunct — the per-member OR of
/// `(prep_sample_idx = p AND sequence_idx BETWEEN start AND stop)` — is the exact
/// residual on the pruned rows, so a split member never leaks a sibling block's
/// rows (independent of tiling order). The coarse pair is a superset of the OR,
/// so `coarse AND exact == exact`. `members` must be non-empty (caller guards);
/// all integers are signature-verified i64s, safe to inline.
pub(super) fn block_read_where_clause(members: &[auth::BlockReadMember]) -> String {
    let in_list = block_member_preps(members)
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    // Unwraps are safe: `members` is non-empty (caller guards).
    let block_min = members.iter().map(|m| m.sequence_idx_start).min().unwrap();
    let block_max = members.iter().map(|m| m.sequence_idx_stop).max().unwrap();
    let member_terms = members
        .iter()
        .map(|m| {
            format!(
                "(prep_sample_idx = {} AND sequence_idx BETWEEN {} AND {})",
                m.prep_sample_idx, m.sequence_idx_start, m.sequence_idx_stop
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "prep_sample_idx IN ({in_list}) \
         AND sequence_idx BETWEEN {block_min} AND {block_max} \
         AND ({member_terms})"
    )
}

/// Pull exactly one i64 out of a ticket filter column. Several paths need a
/// single-valued scope rather than an IN-set — the count/metrics aggregates on
/// `prep_sample_idx` / `mask_idx`, and the `read_masked_block` DoGet on
/// `mask_idx` — so a missing, empty, multi-valued, or non-integer column is a
/// malformed-ticket error; the control plane always signs a one-element list for
/// these. Input is signature-verified (set by the control plane), but we validate
/// anyway for defense in depth.
pub(super) fn single_i64_filter(filter: &auth::TicketFilter, col: &str) -> Result<i64, Status> {
    let values = filter.get(col).ok_or_else(|| {
        Status::invalid_argument(format!(
            "ticket missing single-valued filter column {col:?}"
        ))
    })?;
    match values.as_slice() {
        [v] => v.as_i64().ok_or_else(|| {
            Status::invalid_argument(format!(
                "filter value for {col:?} must be an integer, got {v}"
            ))
        }),
        _ => Err(Status::invalid_argument(format!(
            "expected exactly one value for {col:?}, got {}",
            values.len()
        ))),
    }
}

/// A non-empty integer set from a ticket filter column.
pub(super) fn i64_list_filter(filter: &auth::TicketFilter, col: &str) -> Result<Vec<i64>, Status> {
    let values = filter
        .get(col)
        .ok_or_else(|| Status::invalid_argument(format!("ticket missing filter column {col:?}")))?;
    if values.is_empty() {
        return Err(Status::invalid_argument(format!(
            "filter column {col:?} has empty values list"
        )));
    }
    values
        .iter()
        .map(|v| {
            v.as_i64().ok_or_else(|| {
                Status::invalid_argument(format!(
                    "filter values for {col:?} must be integers, got {v}"
                ))
            })
        })
        .collect()
}

/// The sorted, deduplicated prep_sample set a block's members cover.
pub(super) fn block_member_preps(members: &[auth::BlockReadMember]) -> Vec<i64> {
    let mut preps: Vec<i64> = members.iter().map(|m| m.prep_sample_idx).collect();
    preps.sort_unstable();
    preps.dedup();
    preps
}

/// The `read_masked` table-macro call for one (mask, prep_samples) scope.
///
/// `read_masked` is a MACRO, not a relation — it takes its scope as arguments;
/// `ducklake.rs` carries why.
///
/// `preps` must be non-empty, and that is the CALLER's guarantee, not this
/// function's: `build_read_masked_query` gets it from `i64_list_filter` (which
/// rejects an empty list) and `build_block_read_query` refuses empty `members`
/// first. Those two are the enforcement. An empty list here would emit
/// `read_masked(m, [])`, which the macro reads as "match nothing" — safe, but a
/// silent zero-row answer rather than a loud error. The `debug_assert` below is a
/// development-time tripwire for a future caller that forgets to guard; it is
/// compiled out of the release binary the deploy builds, so it protects the next
/// edit rather than production.
pub(super) fn read_masked_relation(mask_idx: i64, preps: &[i64]) -> String {
    debug_assert!(
        !preps.is_empty(),
        "read_masked scope must name at least one prep_sample; callers guard this"
    );
    let csv = preps
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("qiita_lake.read_masked({mask_idx}, [{csv}])")
}

/// Count the masked reads a `read_masked` ticket selects, without streaming them.
///
/// Runs `count(*)` against the light `read_mask` table (keyed
/// `(mask_idx, prep_sample_idx, sequence_idx)`) rather than the `read_masked`
/// view: every `reason = 'pass'` row in `read_mask` has its `read` row by
/// construction, so the filtered `read_mask` count equals the view's row count —
/// while touching only the small key/`reason` columns, never joining to `read`
/// or materializing the sequence/quality bytes. This is what makes the export's
/// idempotency probe cheap. Takes a borrowed instance so the caller can run it on
/// the blocking pool (mirrors `export_read_to_parquet`).
fn count_masked_reads(
    conn: &Connection,
    prep_sample_idx: i64,
    mask_idx: i64,
) -> Result<i64, Status> {
    // `prep_sample_idx`/`mask_idx` are signature-verified i64s, safe to inline (same
    // rationale as build_query: parsed integers reach SQL, no string data); the
    // 'pass' filter mirrors the read_masked macro's privacy filter.
    let sql = format!(
        "SELECT count(*) FROM qiita_lake.read_mask \
         WHERE mask_idx = {mask_idx} AND prep_sample_idx = {prep_sample_idx} \
         AND reason = 'pass'"
    );
    conn.query_row(&sql, [], |row| row.get(0))
        .map_err(|e| Status::internal(format!("count_masked query failed: {e}")))
}

/// Reason lists for the read-mask count buckets — the Rust twin of qiita-common's
/// `READ_MASK_BUCKET` (`read_mask_reason_sql_list`). A WHITELIST: a ReadMaskReason
/// absent from both lists counts toward `raw` only, which is the correct default
/// for a QC failure or `twist_no_adaptor`, and is why the fail-open
/// `NOT LIKE 'qc_%'` predicate was retired. Adding a reason means editing here AND
/// the Python map; `test_rust_reason_lists_match_the_python_bucket_map` asserts the two
/// paths agree (NOT the block e2e test — see the note on the counts fn below).
const BIOLOGICAL_REASONS: &str = "'host_minimap2', 'host_rype', 'pass'";
const SPIKEIN_REASONS: &str = "'spikein_syndna'";

/// Aggregate a prep_sample's `read_mask` rows for one mask into the per-stage read
/// counts the block reconcile primitive persists onto `sequenced_sample`.
///
/// The counterpart of the per-prep_sample read-mask's local-parquet rollup
/// (`qiita_control_plane.actions.library._read_mask_counts`), but read from the
/// persisted DuckLake `read_mask` table because a block-masked prep_sample's rows are
/// written by SEVERAL blocks — any one block's local parquet covers only its
/// slice. Returns the both-mates (`*_r1r2`) totals `sequenced_sample` stores plus
/// `row_count` (one per read/pair) the reconcile count-assertion checks against
/// the prep_sample's `sequence_range`.
///
/// `right_trim2` is non-NULL for paired-end and NULL for single-end, so
/// `count(right_trim2)` is the R2 count and `count(*) + count(right_trim2)` is the
/// both-mates total — matching `_read_mask_counts` exactly (SE / PE / mixed, no
/// branching).
///
/// Bucketing mirrors it too, and is a WHITELIST rather than the fail-open
/// `reason NOT LIKE 'qc_%'` it replaced: raw = every row; biological = `pass` +
/// the `host_*` hits (a human read is still a biological read); spikein =
/// `spikein_syndna`, disjoint from biological (a spike-in is added in the lab);
/// quality_filtered = the `pass` subset. `qc_*` and `twist_no_adaptor` count
/// toward raw only.
///
/// The reason lists below are the Rust twin of `READ_MASK_BUCKET` in qiita-common
/// (`read_mask_reason_sql_list`). Rust cannot import it, so a control-plane test
/// parses these consts out of this file and compares them to it character for
/// character. (NOT the block e2e test: its fixture emits no `spikein_syndna` or
/// `host_minimap2` rows, so a typo here would miscount silently there.) Adding a
/// ReadMaskReason means touching BOTH sides.
///
/// Takes a borrowed instance so the caller can run it on the blocking pool
/// (mirrors `count_masked_reads`).
fn mask_metrics_counts(
    conn: &Connection,
    mask_idx: i64,
    prep_sample_idx: i64,
) -> Result<serde_json::Value, Status> {
    // `mask_idx`/`prep_sample_idx` are signature-verified i64s, safe to inline (same
    // rationale as count_masked_reads: parsed integers reach SQL, no string data).
    let sql = format!(
        "SELECT \
           count(*) + count(right_trim2), \
           count(*) FILTER (WHERE reason IN ({BIOLOGICAL_REASONS})) \
             + count(right_trim2) FILTER (WHERE reason IN ({BIOLOGICAL_REASONS})), \
           count(*) FILTER (WHERE reason = 'pass') \
             + count(right_trim2) FILTER (WHERE reason = 'pass'), \
           count(*) FILTER (WHERE reason IN ({SPIKEIN_REASONS})) \
             + count(right_trim2) FILTER (WHERE reason IN ({SPIKEIN_REASONS})), \
           count(*) \
         FROM qiita_lake.read_mask \
         WHERE mask_idx = {mask_idx} AND prep_sample_idx = {prep_sample_idx}"
    );
    let (raw, biological, quality_filtered, spikein, row_count): (i64, i64, i64, i64, i64) = conn
        .query_row(&sql, [], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .map_err(|e| Status::internal(format!("mask_metrics query failed: {e}")))?;
    Ok(serde_json::json!({
        "raw": raw,
        "biological": biological,
        "quality_filtered": quality_filtered,
        "spikein": spikein,
        "row_count": row_count,
    }))
}

#[cfg(test)]
#[path = "export_tests.rs"]
mod tests;
