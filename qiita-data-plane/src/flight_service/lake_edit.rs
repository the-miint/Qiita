//! The DoActions that change the lake other than registration: the `delete_*`
//! actions, `sync_reference_exclusion` and `mint_phylogeny_edge_id`.

use std::path::Path;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Action;
use tonic::{Response, Status};

use crate::auth;
use crate::pool::LakeWriter;

use super::export::validate_export_dest;
use super::query::block_read_where_clause;
use super::registration::{sql_i64_list, take_registration_lock, transact_with_retry};
use super::QiitaFlightService;

impl QiitaFlightService {
    pub(super) async fn delete_reference_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload = self
            .verify_action_payload(action, auth::verify_delete_reference, |p| p.action.as_str())?;

        let reference_idx = payload.reference_idx;
        let deleted = self
            .blocking_lake_write("delete_reference", move |conn| {
                delete_reference(conn, reference_idx)
            })
            .await?;

        Self::action_response(&deleted)
    }

    pub(super) async fn delete_mask_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload =
            self.verify_action_payload(action, auth::verify_delete_mask, |p| p.action.as_str())?;

        let mask_idx = payload.mask_idx;
        let deleted = self
            .blocking_lake_write("delete_mask", move |conn| delete_mask(conn, mask_idx))
            .await?;

        Self::action_response(&deleted)
    }

    pub(super) async fn delete_pool_reads_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload = self.verify_action_payload(action, auth::verify_delete_pool_reads, |p| {
            p.action.as_str()
        })?;

        let prep_sample_idxs = payload.prep_sample_idxs;
        let deleted = self
            .blocking_lake_write("delete_pool_reads", move |conn| {
                delete_pool_reads(conn, &prep_sample_idxs)
            })
            .await?;

        Self::action_response(&deleted)
    }

    pub(super) async fn delete_read_mask_block_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload =
            self.verify_action_payload(action, auth::verify_delete_read_mask_block, |p| {
                p.action.as_str()
            })?;

        // An empty block is a control-plane bug, not a valid ask —
        // reject it loudly rather than deleting nothing silently.
        if payload.members.is_empty() {
            return Err(Status::invalid_argument(
                "delete_read_mask_block requires a non-empty members list",
            ));
        }

        let mask_idx = payload.mask_idx;
        let members = payload.members;
        let deleted = self
            .blocking_lake_write("delete_read_mask_block", move |conn| {
                delete_read_mask_block(conn, mask_idx, &members)
            })
            .await?;

        Self::action_response(&deleted)
    }

    pub(super) async fn delete_alignment_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload = self
            .verify_action_payload(action, auth::verify_delete_alignment, |p| p.action.as_str())?;

        let alignment_idx = payload.alignment_idx;
        let deleted = self
            .blocking_lake_write("delete_alignment", move |conn| {
                delete_alignment(conn, alignment_idx)
            })
            .await?;

        Self::action_response(&deleted)
    }

    pub(super) async fn delete_alignment_block_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload =
            self.verify_action_payload(action, auth::verify_delete_alignment_block, |p| {
                p.action.as_str()
            })?;

        // An empty block is a control-plane bug, not a valid ask —
        // reject it loudly rather than deleting nothing silently.
        if payload.members.is_empty() {
            return Err(Status::invalid_argument(
                "delete_alignment_block requires a non-empty members list",
            ));
        }

        let alignment_idx = payload.alignment_idx;
        let members = payload.members;
        let deleted = self
            .blocking_lake_write("delete_alignment_block", move |conn| {
                delete_alignment_block(conn, alignment_idx, &members)
            })
            .await?;

        Self::action_response(&deleted)
    }

    pub(super) async fn delete_alignment_sample_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload =
            self.verify_action_payload(action, auth::verify_delete_alignment_sample, |p| {
                p.action.as_str()
            })?;

        let alignment_idx = payload.alignment_idx;
        let prep_sample_idx = payload.prep_sample_idx;
        let deleted = self
            .blocking_lake_write("delete_alignment_sample", move |conn| {
                delete_alignment_sample(conn, alignment_idx, prep_sample_idx)
            })
            .await?;

        Self::action_response(&deleted)
    }

    pub(super) async fn sync_reference_exclusion_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload =
            self.verify_action_payload(action, auth::verify_sync_reference_exclusion, |p| {
                p.action.as_str()
            })?;

        // Defense in depth on the signature-trusted destination before it
        // is inlined into a DuckDB `read_parquet(...)` literal and read.
        let dest = validate_export_dest(&payload.dest, &self.scratch_root)?;

        // A full replace, plus a blocking `canonicalize` in the handler.
        let scratch_root = self.scratch_root.clone();
        let result = self
            .blocking_lake_write("sync_reference_exclusion", move |conn| {
                sync_reference_exclusion(conn, &dest, &scratch_root)
            })
            .await?;

        Self::action_response(&result)
    }

    pub(super) async fn mint_phylogeny_edge_id_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload =
            self.verify_action_payload(action, auth::verify_mint_phylogeny_edge_id, |p| {
                p.action.as_str()
            })?;

        let reference_idx = payload.reference_idx;
        let result = self
            .blocking_lake_write("mint_phylogeny_edge_id", move |conn| {
                mint_phylogeny_edge_id(conn, reference_idx)
            })
            .await?;

        Self::action_response(&result)
    }
}

/// Delete every DuckLake row belonging to a reference.
///
/// Scoping rules mirror the identifier hierarchy:
/// - `reference_taxonomy`, `reference_phylogeny`, `reference_placements`,
///   `reference_annotation`, and `reference_membership` carry `reference_idx`
///   directly → deleted by a plain `WHERE reference_idx = ?`.
/// - `reference_sequences` / `reference_sequence_chunks` are keyed by
///   `feature_idx` and **shared across references** (a feature deduplicates by
///   sequence hash). Only *orphan* features — owned by this reference and no
///   other — are removed; a feature another reference still claims keeps its
///   sequence. Orphans are computed from this data plane's own
///   `reference_membership`, so the action ticket needs only `reference_idx`.
///
/// Order matters: the sequence deletes run *before* the membership delete so
/// the orphan subquery can still see this reference's rows. Idempotent — a
/// reference with no loaded data deletes zero rows and still succeeds.
fn delete_reference(conn: &LakeWriter, reference_idx: i64) -> Result<serde_json::Value, Status> {
    let exec = |sql: &str, params: &[i64]| -> Result<usize, Status> {
        // duckdb's params! wants &dyn ToSql; i64 implements it. Build the
        // slice explicitly so the same helper serves the 1- and 2-param calls.
        let boxed: Vec<&dyn duckdb::ToSql> =
            params.iter().map(|p| p as &dyn duckdb::ToSql).collect();
        conn.execute(sql, boxed.as_slice())
            .map_err(|e| Status::internal(format!("delete failed ({sql}): {e}")))
    };

    // All seven deletes are one DuckLake transaction so the action is
    // all-or-nothing: a mid-delete failure rolls every table back rather than
    // leaving a half-purged reference. That atomicity is what lets the control
    // plane safely retry — a failed call leaves DuckLake membership fully
    // intact, so the orphan recomputation on the next attempt is unchanged.
    //
    // It runs under the same lock and retry as `register_files`, because it
    // writes two of the same content-addressed tables. Without the lock a
    // registration could add a feature between this transaction's snapshot and
    // its commit, and the orphan filter — which reads `reference_membership` —
    // would not see the claim; with it, the two serialize. Without the retry a
    // registration's DELETE would newly conflict this one out, which the bare
    // COMMIT here predates.
    //
    // Orphan features: this reference's features minus every other reference's.
    //
    // A reference claims a feature in TWO ways and both count: as a MEMBER (a whole
    // sequence, indexed and aligned against) or as an annotated INTERVAL (a SynDNA
    // insert on its plasmid — minted its own feature_idx, deliberately kept out of
    // membership). Omitting reference_annotation on the left would leak annotated
    // features; omitting it on the right would delete a sequence another reference
    // still annotates.
    //
    // These two claim sets MUST match the Postgres-side orphan computation in
    // qiita_control_plane.actions.reference.delete_reference_cascade — the two
    // stores GC the same features independently, so a change to one query must
    // change the other or sequences/features desync across stores. That query
    // carries one further term this filter omits (`qiita.assembly_membership`);
    // its comment holds the rationale for the asymmetry.
    let orphan_filter = "feature_idx IN (
            (SELECT feature_idx FROM qiita_lake.reference_membership WHERE reference_idx = ?
             UNION
             SELECT feature_idx FROM qiita_lake.reference_annotation WHERE reference_idx = ?)
            EXCEPT
            (SELECT feature_idx FROM qiita_lake.reference_membership WHERE reference_idx <> ?
             UNION
             SELECT feature_idx FROM qiita_lake.reference_annotation WHERE reference_idx <> ?)
        )";

    // Sequence/chunk deletes run BEFORE the membership AND annotation deletes: the
    // orphan subquery reads both of this reference's claim tables, so both must
    // still be present.
    let counts = transact_with_retry(
        conn,
        "the reference delete",
        reference_idx,
        || {
            take_registration_lock(conn)?;
            let sequences_deleted = exec(
                &format!("DELETE FROM qiita_lake.reference_sequences WHERE {orphan_filter}"),
                &[reference_idx, reference_idx, reference_idx, reference_idx],
            )?;
            let chunks_deleted = exec(
                &format!("DELETE FROM qiita_lake.reference_sequence_chunks WHERE {orphan_filter}"),
                &[reference_idx, reference_idx, reference_idx, reference_idx],
            )?;
            let membership_deleted = exec(
                "DELETE FROM qiita_lake.reference_membership WHERE reference_idx = ?",
                &[reference_idx],
            )?;
            let taxonomy_deleted = exec(
                "DELETE FROM qiita_lake.reference_taxonomy WHERE reference_idx = ?",
                &[reference_idx],
            )?;
            let phylogeny_deleted = exec(
                "DELETE FROM qiita_lake.reference_phylogeny WHERE reference_idx = ?",
                &[reference_idx],
            )?;
            let placements_deleted = exec(
                "DELETE FROM qiita_lake.reference_placements WHERE reference_idx = ?",
                &[reference_idx],
            )?;
            // Annotation ROWS delete by reference_idx (they carry it directly), not by
            // the orphan filter — but they must go AFTER the sequence/chunk deletes
            // above, which read this table as one of the two claim sets in
            // `orphan_filter`. The annotated FEATURES themselves are GC'd through that
            // filter, like any other orphan.
            let annotations_deleted = exec(
                "DELETE FROM qiita_lake.reference_annotation WHERE reference_idx = ?",
                &[reference_idx],
            )?;
            Ok(serde_json::json!({
                "sequences_deleted": sequences_deleted,
                "chunks_deleted": chunks_deleted,
                "membership_deleted": membership_deleted,
                "taxonomy_deleted": taxonomy_deleted,
                "phylogeny_deleted": phylogeny_deleted,
                "placements_deleted": placements_deleted,
                "annotations_deleted": annotations_deleted,
            }))
        },
        |_| {
            // The deletes commit together, so none of the reference's rows left in
            // the tables keyed by it means the reference is deleted: by this
            // attempt, or by a delete of the same reference that committed first,
            // whose counts this one then reports.
            let mut remaining = 0i64;
            for table in [
                "reference_membership",
                "reference_taxonomy",
                "reference_phylogeny",
                "reference_placements",
                "reference_annotation",
            ] {
                remaining += conn
                    .query_row(
                        &format!("SELECT count(*) FROM qiita_lake.{table} WHERE reference_idx = ?"),
                        [reference_idx],
                        |r| r.get::<_, i64>(0),
                    )
                    .map_err(|e| Status::internal(format!("failed to count {table} rows: {e}")))?;
            }
            Ok(remaining == 0)
        },
    )?;

    let mut out = counts;
    out["reference_idx"] = serde_json::json!(reference_idx);
    Ok(out)
}

/// Logically delete every row a mask owns from the DuckLake `read_mask` table.
///
/// Mirrors `delete_reference`: one DuckLake transaction, logical `DELETE` only.
/// No raw parquet `unlink` — DuckLake owns file lifecycle and a manual unlink
/// would corrupt the catalog; orphan parquets are tolerated until a future
/// maintenance pass (matches `delete_reference`, which also reclaims nothing
/// from disk). Idempotent: deleting a `mask_idx` with zero rows is success and
/// returns `rows_deleted: 0`, so the control plane can safely retry.
fn delete_mask(conn: &LakeWriter, mask_idx: i64) -> Result<serde_json::Value, Status> {
    // Single-statement delete wrapped in an explicit transaction so the action
    // is all-or-nothing and the control plane can safely retry: a failed call
    // leaves the mask's rows fully intact, so a retry sees the same row set.
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let deleted = conn.execute(
        "DELETE FROM qiita_lake.read_mask WHERE mask_idx = ?",
        [&mask_idx as &dyn duckdb::ToSql],
    );

    let rows_deleted = match deleted {
        Ok(n) => n,
        Err(e) => {
            // Best-effort rollback; surface the original delete error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(Status::internal(format!(
                "delete failed (DELETE FROM qiita_lake.read_mask WHERE mask_idx = ?): {e}"
            )));
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit delete transaction: {e}")))?;

    Ok(serde_json::json!({
        "mask_idx": mask_idx,
        "rows_deleted": rows_deleted,
    }))
}

/// Replace the DuckLake `reference_exclusion` mirror wholesale from the control
/// plane's resolved-blocklist Parquet.
///
/// The control plane owns the authoritative blocklist (`qiita.reference_exclusion`),
/// resolves it to the excluded `feature_idx` set (direct feature blocks plus every
/// feature of a blocked genome), and writes that set to `dest` — a single-column
/// (`feature_idx`) Parquet on the shared scratch tree. This handler clears the
/// mirror and reloads it from that file in ONE transaction, so the two-statement
/// swap is all-or-nothing: a failure leaves the previous mirror intact and the
/// control plane can safely retry.
///
/// Full-replace makes it idempotent / replay-safe: re-running with the same
/// Parquet converges to the same table, and a replay of a stale-but-valid token
/// reloads whatever the (authoritative) file holds. An empty blocklist ships a
/// valid zero-row Parquet, so the DELETE + zero-row INSERT simply clears the
/// mirror (re-enabling everything). Takes a borrowed instance so the caller can
/// run it on the blocking pool (mirrors `delete_mask` / `register_files`).
/// Returns the loaded row count.
///
/// `dest` arrives lexically validated (`validate_export_dest`: absolute, under
/// the scratch root, no `..`, no single quote), but that check is a string test
/// done before any I/O — insufficient on the shared scratch tree, where another
/// job could plant a symlink that redirects this read outside the controlled
/// workspace. That matters MORE here than for the read-export sibling: this
/// feeds the GLOBAL `reference_exclusion` mirror both `_visible` views enforce
/// against, so a redirected read of an attacker-planted Parquet's `feature_idx`
/// column would mass-exclude arbitrary features system-wide. So — the read-path
/// analogue of `export_select_to_parquet`'s parent-canonicalize on write — we
/// canonicalize `dest` ITSELF (the file already exists; the CP wrote it before
/// signing) and re-assert it resolves under the canonicalized scratch root
/// before reading, then inline the CANONICALIZED path (re-checked for a single
/// quote a resolved symlink target could introduce) into `read_parquet`.
fn sync_reference_exclusion(
    conn: &LakeWriter,
    dest: &Path,
    scratch_root: &Path,
) -> Result<serde_json::Value, Status> {
    // Symlink-safe containment (see the doc comment): resolve the real target
    // and re-assert it stays under the real scratch root.
    let real_dest = std::fs::canonicalize(dest)
        .map_err(|e| Status::internal(format!("failed to resolve {}: {e}", dest.display())))?;
    let real_root = std::fs::canonicalize(scratch_root).map_err(|e| {
        Status::internal(format!(
            "failed to resolve scratch root {}: {e}",
            scratch_root.display()
        ))
    })?;
    if !real_dest.starts_with(&real_root) {
        return Err(Status::permission_denied(format!(
            "sync dest {} resolves outside the scratch root {}",
            real_dest.display(),
            real_root.display()
        )));
    }
    let dest_str = real_dest
        .to_str()
        .ok_or_else(|| Status::internal(format!("non-UTF-8 path: {}", real_dest.display())))?;
    // The lexical check rejected a single quote in the requested path, but a
    // resolved symlink could land on a real directory whose name contains one;
    // re-check the canonicalized string we are about to inline.
    if dest_str.contains('\'') {
        return Err(Status::invalid_argument(format!(
            "resolved sync dest must not contain a single quote: {dest_str:?}"
        )));
    }

    // Clear-then-reload in one transaction so the swap is atomic and retryable:
    // a failure leaves the previous mirror intact.
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let loaded = (|| -> Result<i64, Status> {
        conn.execute(
            "DELETE FROM qiita_lake.reference_exclusion",
            duckdb::params![],
        )
        .map_err(|e| Status::internal(format!("failed to clear reference_exclusion: {e}")))?;
        // `dest_str` is the canonicalized path, verified above to resolve under
        // the scratch root and to carry no single quote, so inlining it into the
        // read_parquet literal is safe. Project `feature_idx` by name so the load
        // is insensitive to any extra column the producer might add later.
        let inserted = conn
            .execute(
                &format!(
                    "INSERT INTO qiita_lake.reference_exclusion (feature_idx) \
                     SELECT feature_idx FROM read_parquet('{dest_str}')"
                ),
                duckdb::params![],
            )
            .map_err(|e| {
                Status::internal(format!(
                    "failed to load reference_exclusion from {dest_str}: {e}"
                ))
            })?;
        Ok(inserted as i64)
    })();

    let loaded = match loaded {
        Ok(n) => n,
        Err(e) => {
            // Best-effort rollback; surface the original error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit sync transaction: {e}")))?;

    Ok(serde_json::json!({
        "feature_count": loaded,
    }))
}

/// Give one reference's phylogeny rows the edge numbering krepp placement joins on.
///
/// Why the numbering has to be minted, and what an all-NULL column costs: "Edge
/// numbering (`edge_id`)" in `docs/architecture/reference-data.md`, and
/// duckdb-miint#272. `reference_load._write_phylogeny` does this at load; this
/// action does it for a reference already in the lake.
///
/// All-or-nothing per tree, the rule `reference_load._write_phylogeny` applies at
/// load: a tree that numbers any node keeps the numbering it arrived with. Minting
/// only the NULL rows of a partly numbered tree would leave two numberings in one
/// column with nothing to tell them apart, so this counts first and mints only when
/// the whole tree is NULL. `reference_idx` is an `i64` from the Ed25519-signed
/// payload, bound as a parameter rather than inlined.
///
/// Reports `phylogeny_rows` (0 means the reference has no tree), the pre-mint
/// `already_numbered_rows`, and `minted_rows`. The caller decides what each state
/// means: a partly numbered tree comes back with `minted_rows` 0 rather than a
/// partial write.
fn mint_phylogeny_edge_id(
    conn: &LakeWriter,
    reference_idx: i64,
) -> Result<serde_json::Value, Status> {
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let counted = (|| -> Result<(i64, i64, i64), Status> {
        let (total, already_numbered) = conn
            .query_row(
                "SELECT count(*), count(edge_id) FROM qiita_lake.reference_phylogeny \
                 WHERE reference_idx = ?",
                duckdb::params![reference_idx],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(|e| {
                Status::internal(format!(
                    "failed to count phylogeny rows for reference {reference_idx}: {e}"
                ))
            })?;
        if already_numbered != 0 {
            return Ok((total, already_numbered, 0));
        }
        let minted = conn
            .execute(
                "UPDATE qiita_lake.reference_phylogeny SET edge_id = node_index \
                 WHERE reference_idx = ? AND edge_id IS NULL",
                duckdb::params![reference_idx],
            )
            .map_err(|e| {
                Status::internal(format!(
                    "failed to mint edge_id for reference {reference_idx}: {e}"
                ))
            })?;
        Ok((total, already_numbered, minted as i64))
    })();

    let (total, already_numbered, minted) = match counted {
        Ok(v) => v,
        Err(e) => {
            // Best-effort rollback; surface the original error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit mint transaction: {e}")))?;

    Ok(serde_json::json!({
        "reference_idx": reference_idx,
        "phylogeny_rows": total,
        "already_numbered_rows": already_numbered,
        "minted_rows": minted,
    }))
}

/// Logically delete every `read` and `read_mask` row owned by a set of
/// prep_samples from DuckLake.
///
/// Called when the control plane purges a sequenced_pool: the pool's
/// prep_samples are exclusive to it, so their reads (written once by
/// `ingest_reads`) and any masks over them are orphaned once the pool's Postgres
/// rows are gone. Both tables are keyed by `prep_sample_idx`.
///
/// Mirrors `delete_reference` / `delete_mask`: one DuckLake transaction
/// (all-or-nothing, so a failure leaves both tables intact and the control plane
/// can safely retry), logical `DELETE` only — no raw parquet `unlink` (DuckLake
/// owns file lifecycle; orphan parquets are reclaimed by a future maintenance
/// pass). Idempotent: an empty set, or a set whose rows are already gone,
/// returns zero counts. The `prep_sample_idxs` are `i64` parsed from the
/// Ed25519-signed payload and inlined through `sql_i64_list`.
fn delete_pool_reads(
    conn: &LakeWriter,
    prep_sample_idxs: &[i64],
) -> Result<serde_json::Value, Status> {
    // Empty set: nothing to do, and `IN ()` is not valid SQL. Return the
    // zero-count shape without touching the catalog.
    if prep_sample_idxs.is_empty() {
        return Ok(serde_json::json!({
            "prep_sample_count": 0,
            "read_rows_deleted": 0,
            "read_mask_rows_deleted": 0,
        }));
    }

    let in_list = sql_i64_list(prep_sample_idxs);

    // Both deletes run in one transaction so the action is all-or-nothing and
    // retriable: a mid-delete failure rolls both tables back rather than
    // leaving reads gone but masks behind (or vice versa).
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let deletes = (|| -> Result<(usize, usize), Status> {
        let read_rows_deleted = conn
            .execute(
                &format!("DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({in_list})"),
                [],
            )
            .map_err(|e| Status::internal(format!("delete from read failed: {e}")))?;
        let read_mask_rows_deleted = conn
            .execute(
                &format!("DELETE FROM qiita_lake.read_mask WHERE prep_sample_idx IN ({in_list})"),
                [],
            )
            .map_err(|e| Status::internal(format!("delete from read_mask failed: {e}")))?;
        Ok((read_rows_deleted, read_mask_rows_deleted))
    })();

    let (read_rows_deleted, read_mask_rows_deleted) = match deletes {
        Ok(counts) => counts,
        Err(e) => {
            // Best-effort rollback; surface the original delete error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit delete transaction: {e}")))?;

    Ok(serde_json::json!({
        "prep_sample_count": prep_sample_idxs.len(),
        "read_rows_deleted": read_rows_deleted,
        "read_mask_rows_deleted": read_mask_rows_deleted,
    }))
}

/// Delete exactly one block's footprint from the DuckLake `read_mask` table:
/// the rows for `mask_idx` whose `(prep_sample_idx, sequence_idx)` fall in the
/// members' sub-ranges. This is the idempotent-block-replace primitive — the
/// block workflow runs it immediately before `register-files`, so a re-run
/// deletes the prior run's rows before writing fresh ones and never double-counts
/// (the reconcile count-assertion would otherwise trip on a 2× row count).
///
/// The WHERE clause is the SAME exact-by-construction footprint selector
/// the `read_block` selector emits (`block_read_where_clause`), scoped further by
/// `mask_idx = ?`: `mask_idx = {m} AND prep_sample_idx IN (...) AND sequence_idx
/// BETWEEN block_min AND block_max AND (per-member OR)`. The per-member OR
/// residual makes it exact — a split member deletes ONLY its own sub-range, so a
/// sibling block's rows for a shared prep_sample survive (independent of tiling
/// order). The coarse `IN + BETWEEN` pair is a pushdown hint (see
/// `block_read_where_clause`).
///
/// Mirrors `delete_mask` / `delete_pool_reads`: one DuckLake transaction
/// (all-or-nothing, retriable — a failed call leaves the block's rows intact so a
/// retry sees the same set), logical `DELETE` only (no raw parquet unlink —
/// DuckLake owns file lifecycle). Idempotent: a fresh block (no rows yet) deletes
/// 0. Empty `members` is a control-plane bug (the DoAction arm rejects it before
/// this); guarded here too, returning a zero-count noop. All integers are
/// signature-verified i64s, safe to inline.
fn delete_read_mask_block(
    conn: &LakeWriter,
    mask_idx: i64,
    members: &[auth::BlockReadMember],
) -> Result<serde_json::Value, Status> {
    if members.is_empty() {
        return Ok(serde_json::json!({
            "mask_idx": mask_idx,
            "rows_deleted": 0,
        }));
    }

    // Scope the shared footprint selector to this filtering identity. The
    // `read` export needs no mask column; `read_mask` is keyed by mask_idx too.
    let where_clause = format!(
        "mask_idx = {mask_idx} AND {}",
        block_read_where_clause(members)
    );

    // Single-statement delete wrapped in an explicit transaction so the action
    // is all-or-nothing and the control plane can safely retry: a failed call
    // leaves the block's rows fully intact, so a retry sees the same row set.
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let deleted = conn.execute(
        &format!("DELETE FROM qiita_lake.read_mask WHERE {where_clause}"),
        [],
    );

    let rows_deleted = match deleted {
        Ok(n) => n,
        Err(e) => {
            // Best-effort rollback; surface the original delete error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(Status::internal(format!(
                "delete failed (DELETE FROM qiita_lake.read_mask WHERE {where_clause}): {e}"
            )));
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit delete transaction: {e}")))?;

    Ok(serde_json::json!({
        "mask_idx": mask_idx,
        "rows_deleted": rows_deleted,
    }))
}

/// Every `qiita_lake` table an alignment delete clears, in delete order.
/// `alignment` leads: every `delete_alignment*` reports its count as
/// `rows_deleted`.
///
/// `alignment_delete_covers_every_alignment_scoped_lake_table` pins the set
/// against the catalog, so a third table keyed by `alignment_idx` cannot be added
/// without joining this list.
const ALIGNMENT_DELETE_TABLES: &[&str] = &["alignment", "alignment_origin_spanning"];

/// Every delete reads the leading count as `counts[0]`, which panics on an empty
/// list. Emptying the list is a build failure instead.
const _: () = assert!(!ALIGNMENT_DELETE_TABLES.is_empty());

/// Delete `where_clause`'s rows from each of `tables`, in order, returning one
/// count per table positionally. The shared body of the `delete_alignment*`
/// handlers; each builds its own clause and shapes its own response.
///
/// The tables go in lockstep because they describe each other:
/// `alignment_origin_spanning` names the `alignment` rows that make up one
/// origin-spanning read, so dropping the fragments while keeping the evidence
/// leaves a read described by rows that are gone.
///
/// One DuckLake transaction, so the action is all-or-nothing and the control
/// plane can safely retry: a failed call leaves every table's rows fully intact,
/// so a retry sees the same row set. Logical `DELETE` only. No raw parquet
/// `unlink` — DuckLake owns file lifecycle and a manual unlink would corrupt the
/// catalog; orphan parquets are tolerated until a future maintenance pass
/// (matches `delete_mask`).
///
/// `tables` and `where_clause` are interpolated and `params` binds any `?` the
/// clause carries, so the caller owns the injection argument for both — as
/// `replace_key_delete_sql`'s does at its own site. Every caller passes an
/// `ALIGNMENT_DELETE_TABLES` literal and a clause whose values are either bound
/// `?`s or inlined Ed25519-verified i64s.
fn delete_lake_rows(
    conn: &LakeWriter,
    tables: &[&str],
    where_clause: &str,
    params: &[&dyn duckdb::ToSql],
) -> Result<Vec<usize>, Status> {
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

    let deletes = tables
        .iter()
        .map(|table| {
            let sql = format!("DELETE FROM qiita_lake.{table} WHERE {where_clause}");
            conn.execute(&sql, params)
                .map_err(|e| Status::internal(format!("delete failed ({sql}): {e}")))
        })
        .collect::<Result<Vec<usize>, Status>>();

    let counts = match deletes {
        Ok(counts) => counts,
        Err(e) => {
            // Best-effort rollback; surface the original delete error.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    };
    conn.execute_batch("COMMIT")
        .map_err(|e| Status::internal(format!("failed to commit delete transaction: {e}")))?;
    Ok(counts)
}

/// Logically delete one `alignment_idx`'s rows from every
/// `ALIGNMENT_DELETE_TABLES` table in DuckLake — the whole-alignment purge the
/// disallow-without-delete resubmission rule needs (a completed
/// `alignment_sample` must be cleared before re-aligning).
///
/// The alignment twin of `delete_mask`; transaction, lockstep and parquet
/// lifecycle are `delete_lake_rows`'. Idempotent: deleting an `alignment_idx`
/// with zero rows is success and returns `rows_deleted: 0`, so the control plane
/// can safely retry. `alignment_idx` is an Ed25519-verified i64.
///
/// `rows_deleted` is the `alignment` count alone; the side table's count is not
/// reported. `delete_pool_reads` reports one qualified key per table because the
/// control plane consumes both (`PoolReadPurgeResponse`); here there is no
/// consumer, and no producer writing the side table, so a second key would carry
/// a structural zero. A producer PR that adds a consumer adds the key with it.
fn delete_alignment(conn: &LakeWriter, alignment_idx: i64) -> Result<serde_json::Value, Status> {
    let counts = delete_lake_rows(
        conn,
        ALIGNMENT_DELETE_TABLES,
        "alignment_idx = ?",
        &[&alignment_idx as &dyn duckdb::ToSql],
    )?;

    Ok(serde_json::json!({
        "alignment_idx": alignment_idx,
        "rows_deleted": counts[0],
    }))
}

/// Delete exactly one block's footprint from every `ALIGNMENT_DELETE_TABLES`
/// table: the rows for `alignment_idx` whose `(prep_sample_idx, sequence_idx)`
/// fall in the members' sub-ranges. This is the idempotent-block-replace
/// primitive — the `align` workflow runs it immediately before `register-files`,
/// so a re-run deletes the prior run's rows before writing fresh ones and never
/// double-counts.
///
/// The alignment twin of `delete_read_mask_block`: same exact-by-construction
/// footprint selector (`block_read_where_clause`) scoped further by
/// `alignment_idx = ?`. The per-member OR residual makes it exact — a split member
/// deletes ONLY its own sub-range, so a sibling block's rows for a shared prep_sample
/// survive (independent of tiling order). The selector is on `(prep_sample_idx,
/// sequence_idx)` and is feature_idx-agnostic, so it clears ALL of a read's
/// alignment rows (a read produces multiple rows via cross-shard + PE
/// multiplicity) — exactly what a re-run must replace.
///
/// Mirrors `delete_read_mask_block`; transaction, lockstep and parquet lifecycle
/// are `delete_lake_rows`', count reporting is `delete_alignment`'s. Idempotent:
/// a fresh block (no rows yet) deletes 0. Empty `members` is a control-plane bug
/// (the DoAction arm rejects it before this); guarded here too, returning a
/// zero-count noop. All integers are Ed25519-verified i64s, safe to inline.
fn delete_alignment_block(
    conn: &LakeWriter,
    alignment_idx: i64,
    members: &[auth::BlockReadMember],
) -> Result<serde_json::Value, Status> {
    if members.is_empty() {
        return Ok(serde_json::json!({
            "alignment_idx": alignment_idx,
            "rows_deleted": 0,
        }));
    }

    // Scope the shared footprint selector to this align-config identity. The
    // `read`/`read_mask` blocks key on sequence_idx; every
    // `ALIGNMENT_DELETE_TABLES` table also carries `prep_sample_idx` and
    // `sequence_idx`, so the one clause applies to each. The side table holds one
    // row per read against `alignment`'s one per SAM record, and a member's
    // sub-range selects the same reads either way.
    let where_clause = format!(
        "alignment_idx = {alignment_idx} AND {}",
        block_read_where_clause(members)
    );

    let counts = delete_lake_rows(conn, ALIGNMENT_DELETE_TABLES, &where_clause, &[])?;

    Ok(serde_json::json!({
        "alignment_idx": alignment_idx,
        "rows_deleted": counts[0],
    }))
}

/// Delete one `(alignment_idx, prep_sample_idx)` pair's rows from every
/// `ALIGNMENT_DELETE_TABLES` table — the idempotent-prep_sample-replace primitive: run
/// immediately before `register-files` and a re-run deletes the prior run's rows
/// before writing fresh ones, so it never double-counts.
///
/// The pair is the unit because none of the three mechanisms already here selects
/// it:
///
/// * `delete_alignment` keys on `alignment_idx` alone, so it takes every other
///   prep_sample's rows with it.
/// * `delete_alignment_block` needs a `block_member` cover-map, which a caller
///   holding one prep_sample has none of.
/// * `REPLACE_KEY_TABLES` is matched on the destination TABLE name alone (see
///   `register_files`), so an `alignment` entry keyed on this pair would fire on
///   the block-scoped `align` workflow's registrations too. `tile_partition`
///   splits a straddling prep_sample across consecutive blocks and
///   `replace_key_delete_sql` deletes every lake row whose key tuple appears in
///   the incoming Parquet, so the second block's registration would delete the
///   first's rows for the shared prep_sample — `REPLACE_KEY_TABLES`' condition 1 (the
///   incoming files carry the complete row set for every key they mention)
///   failing.
///
/// Both key columns are in the DDL of both tables
/// (`ducklake::ensure_alignment_tables`), so the one clause applies to each. The
/// predicate carries no `sequence_idx` bound — the prep_sample is the unit — and is
/// feature_idx-agnostic, so ALL of a read's alignment rows go.
///
/// Transaction, lockstep and parquet lifecycle are `delete_lake_rows`', count
/// reporting is `delete_alignment`'s. Idempotent: a prep_sample with no rows yet
/// deletes 0 and still succeeds.
fn delete_alignment_sample(
    conn: &LakeWriter,
    alignment_idx: i64,
    prep_sample_idx: i64,
) -> Result<serde_json::Value, Status> {
    let counts = delete_lake_rows(
        conn,
        ALIGNMENT_DELETE_TABLES,
        "alignment_idx = ? AND prep_sample_idx = ?",
        &[
            &alignment_idx as &dyn duckdb::ToSql,
            &prep_sample_idx as &dyn duckdb::ToSql,
        ],
    )?;

    Ok(serde_json::json!({
        "alignment_idx": alignment_idx,
        "rows_deleted": counts[0],
    }))
}

#[cfg(test)]
#[path = "lake_edit_tests.rs"]
mod tests;
