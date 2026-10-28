//! The `register_files` DoAction: moving staged files into the lake and
//! registering them in one DuckLake transaction.

use std::collections::BTreeMap;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Action;
use sha2::{Digest, Sha256};
use tonic::{Response, Status};

use crate::auth;
use crate::pool::LakeWriter;

use super::QiitaFlightService;

impl QiitaFlightService {
    pub(super) async fn register_files_action(
        &self,
        action: &Action,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let payload =
            self.verify_action_payload(action, auth::verify_action, |p| p.action.as_str())?;

        // Moves files, then runs one DuckLake transaction.
        let data_path = self.pool.data_path().to_string();
        let scratch_root = self.scratch_root.clone();
        let registration = self
            .blocking_lake_write("register_files", move |conn| {
                register_files(conn, &data_path, &scratch_root, &payload)
            })
            .await?;

        Self::action_response(&serde_json::json!({
            "registered": registration.registered,
            "replaced": registration.replaced,
        }))
    }
}

/// Lake tables a registration REPLACES by key rather than appends to, and the
/// key each replaces on.
///
/// `feature_idx` is minted from the canonical sequence hash
/// (`qiita_common.chunking.canonical_sequence_hash_expr`), so identical bytes
/// carry one feature across every producer: two references that share a
/// sequence, or two assemblies that produce the same contig, each emit that
/// feature's rows in full. The producer cannot anti-join them away — the compute
/// job writing the staging Parquet has no DuckLake access — and DuckLake enforces
/// no PK/UNIQUE, so an append leaves N copies and
/// `string_agg(chunk_data, '' ORDER BY chunk_index)` returns the sequence
/// concatenated with itself while `sequence_length_bp` still describes one copy.
/// Replacing on the key is what makes a second load converge instead of
/// accumulate.
///
/// Two conditions admit a table:
///
/// 1. The incoming files carry the COMPLETE row set for every key they mention.
///    True of the `_feature_load` writers, which bin-pack whole features into
///    parts, so no part holds a fragment of a feature.
/// 2. Every row set carrying one key is an acceptable substitute for any other.
///    `sequence_hash` and `sequence_length_bp` are functions of the feature, so
///    those are identical. `chunk_data` is NOT: the canonical hash is
///    `LEAST(md5(seq), md5(revcomp(seq)))`, so a sequence and its reverse
///    complement share one `feature_idx` while differing byte for byte. Replacing
///    therefore lets the newest load's strand win. Case does not vary here — the
///    write side normalizes it (`qiita_common.chunking.normalized_sequence_expr`).
///
/// Without the replace both byte strings persist and a reader gets them
/// concatenated — neither strand, and a length that matches nothing. With it a
/// reader gets one coherent sequence that `sequence_length_bp` describes.
/// Nothing records which chunk arrived in which load, so keeping the older
/// strand instead is not expressible.
///
/// The incoming file is taken whole: it is a single load, self-consistent by
/// construction, so no chunk of one strand lands beside a chunk of another. A
/// rule that picked per `chunk_index` from rows already in the lake could,
/// since nothing there records which load a chunk came from.
///
/// Writers of these tables also SERIALIZE against each other, on
/// `registration_lock`. Why the replace alone does not suffice is at the site
/// that takes it — `register_files`' transaction.
///
/// `assembly_membership` / `bin_quality` are keyed on `(prep_sample_idx,
/// processing_idx)` instead. A second `long-read-assembly` run over a prep_sample
/// resolves to the same `processing_idx` whenever the inputs
/// `runner/_processing.py` hashes are unchanged — an edited workflow file
/// included — and `routes/work_ticket.py` admits the submission. Appending
/// leaves both runs' rows under one identity with nothing on the row to tell
/// them apart.
///
/// Condition 1 holds per run: `assembly_load` derives both files from the job's
/// own workspace (`bin_map` ⋈ `id_map`, and the CheckM/DAS_Tool tables) and
/// never reads the lake back, so each carries the run's whole row set for its
/// one key. Condition 2 is the run identity itself — same hashed inputs, so the
/// later rows stand in for the earlier.
const REPLACE_KEY_TABLES: &[ReplaceKey] = &[
    ReplaceKey::own("reference_sequences", &["feature_idx"]),
    ReplaceKey::own("reference_sequence_chunks", &["feature_idx"]),
    ReplaceKey::own("assembled_sequence", &["feature_idx"]),
    ReplaceKey::own("assembled_sequence_chunks", &["feature_idx"]),
    ReplaceKey::own(
        "assembly_membership",
        &["prep_sample_idx", "processing_idx"],
    ),
    ReplaceKey {
        table: "bin_quality",
        key: &["prep_sample_idx", "processing_idx"],
        key_source: "assembly_membership",
    },
    // amplicon's twin of assembly_membership: same key, same argument. amplicon_load
    // derives the whole run's rows from its own workspace and never reads the lake
    // back, and a re-run resolves to the same processing_idx, so a --force resubmit
    // replaces rather than doubling the counts.
    ReplaceKey::own(
        "amplicon_membership",
        &["prep_sample_idx", "processing_idx"],
    ),
    // amplicon's ASV sequences, twins of the assembled_sequence tables: keyed on
    // feature_idx, replaced (not appended) so a re-load of the same ASV converges.
    ReplaceKey::own("amplicon_sequence", &["feature_idx"]),
    ReplaceKey::own("amplicon_sequence_chunks", &["feature_idx"]),
];

/// One `REPLACE_KEY_TABLES` entry.
struct ReplaceKey {
    /// Lake table whose rows a registration supersedes.
    table: &'static str,
    /// Columns compared together as one key.
    key: &'static [&'static str],
    /// Table in the same registration whose incoming files name the key set to
    /// delete on, unioned with this table's own files.
    ///
    /// `bin_quality` borrows `assembly_membership`'s, because it can be EMPTY
    /// where the load still runs: CheckM scores MAG, LCG and the unbinned residue
    /// in three separate passes, and a run in which none of them produced output —
    /// no refined bin, nothing circular, no residue clearing the length cut —
    /// writes zero rows. (A missing CheckM DB is NOT one of those — that step fails
    /// loud rather than report quality as empty.) Zero rows name no key and so delete
    /// nothing, leaving the previous run's rows joined to a membership set that
    /// was replaced out from under them. `assembly_membership` carries the run's
    /// key on every row and is never empty where the load runs at all
    /// (`assembly_hash` raises `StepNoData` at zero contigs of any kind). Every
    /// other entry is its own source.
    key_source: &'static str,
}

impl ReplaceKey {
    /// An entry whose delete keys come from its own incoming files.
    const fn own(table: &'static str, key: &'static [&'static str]) -> Self {
        Self {
            table,
            key,
            key_source: table,
        }
    }
}

/// The replace-by-key DELETE for one `REPLACE_KEY_TABLES` entry: drop every lake
/// row whose key appears in ANY of the `n_files` Parquets the caller passes.
/// Takes one bound path parameter per file, in order. The files are the ones
/// headed for the entry's table plus, where they differ, the ones headed for its
/// `key_source`.
///
/// A multi-column entry matches on the whole key — the row constructor compares
/// the columns together, so a lake row agreeing on one component and differing
/// on another survives.
///
/// One statement for the whole table, not one per file. A multi-file table
/// arrives as several parts, and deleting part-by-part would let a later part's
/// delete drop rows an earlier part had just added whenever the two share a key;
/// it would also re-scan the lake table once per part.
///
/// The `IN` operand is a subquery, not a literal list: DuckDB plans it as a SEMI
/// hash join and pushes the incoming keys' min/max into the lake scan as a
/// dynamic filter. What the delete reads therefore follows the SPREAD of the
/// incoming key set against the per-file key ranges the catalog holds — not the
/// key's arity, and not the table's size.
///
/// Measured on DuckDB 1.5.4 / ducklake d318a545. Against a catalog holding 1.0M
/// rows over 57 files per table, 16k incoming keys over 4 files:
///
/// * composite `(prep_sample_idx, processing_idx)`, one pair per load: scans
///   17,544 rows of 1,000,008 and opens 1 of the 57 files.
/// * `feature_idx` spread over the identity space: scans 1,003,121 of 1,003,200
///   and opens 57 of 57 — the derived range covers every file, so the scan reads
///   the table.
/// * `feature_idx` confined to one narrow window: scans 17,602 of 1,003,200 and
///   still opens 57 of 57 — all 57 files hold a `feature_idx` below the window's
///   maximum, so the range prunes row groups and no file.
///
/// Against a second catalog, single-column `feature_idx`, 2k incoming keys in
/// one file, table size varied: one contiguous incoming block scans 2,000 rows
/// and opens 1 file at both 40k rows over 20 files and 400k over 200; the same
/// count of keys spread over the identity space scans 39,982 of 40,000 and
/// 399,819 of 400,000, opening every file at both sizes.
///
/// A `WITH … DELETE … USING` rewrite plans the same apart from INNER vs SEMI —
/// same dynamic filters, same scan cardinality, same files read — on each of the
/// four key sets measured on the first catalog (the three above plus one
/// matching no lake row). Over 25 alternating pairs per key set the mean paired
/// difference (this statement minus the rewrite) ran from -0.8 ms to +1.0 ms on
/// statements of 3-37 ms, the widest 95% CI being [-3.2, +2.9] ms.
///
/// `table` / `keys` are interpolated because they are `REPLACE_KEY_TABLES`
/// literals (the caller looks them up there, never using the payload's own
/// string); the file paths are bound parameters, so a basename carrying a quote
/// cannot reach the SQL text.
fn replace_key_delete_sql(table: &str, keys: &[&str], n_files: usize) -> String {
    let key_list = keys.join(", ");
    let incoming_keys = (0..n_files)
        .map(|_| format!("SELECT {key_list} FROM read_parquet(?)"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    // DISTINCT because a chunk table repeats its key once per 64 KB chunk and a
    // run-scoped table repeats its pair on every row, and that whole multiset
    // would otherwise become the semi-join's build side.
    format!(
        "DELETE FROM qiita_lake.{table} WHERE ({key_list}) IN \
         (SELECT DISTINCT {key_list} FROM ({incoming_keys}))"
    )
}

/// How long a lake writer keeps re-running its transaction after a failed COMMIT
/// before giving up.
///
/// A budget rather than an attempt count, because the retries a writer needs is
/// the number of writers ahead of it, which nothing here knows. It is a livelock
/// backstop. Measured against a DuckLake catalog holding 40k rows over 20 files,
/// with a contiguous incoming key set: a whole registration transaction (lock
/// UPDATE + replace-by-key DELETE + one `ducklake_add_data_files`) takes ~14 ms
/// against a lake that already holds the incoming keys, ~7 ms when it does not.
/// Since every writer queues behind one row, that per-transaction cost IS the
/// queue rate, and this budget covers a queue far longer than a deploy produces.
/// Those times were taken at the pool's threads, not at the one thread a lake
/// write runs on (`pool::LakeWriter`); a 50M-row delete transaction measured
/// 2.00 s at one thread against 1.32 s at six, locally.
/// The DELETE's share of it is set by the spread of the incoming key set rather
/// than by the table's size — `replace_key_delete_sql` carries what each key set
/// scans, including the sets where it reads the whole table. Exceeding the
/// budget means something other than contention is wrong, and the error says so.
///
/// A registration cannot be retried from the top — its staging files were already
/// moved — so an exhausted budget loses the load.
const LAKE_COMMIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

/// Ceiling on the backoff between retries. Without a cap the doubling below would
/// soon sleep away the whole budget in one wait.
const LAKE_COMMIT_BACKOFF_CAP: std::time::Duration = std::time::Duration::from_millis(500);

/// Starting backoff, doubled per attempt up to `LAKE_COMMIT_BACKOFF_CAP`.
const LAKE_COMMIT_BACKOFF_BASE: std::time::Duration = std::time::Duration::from_millis(10);

/// Backoff before re-running a conflicted lake transaction.
///
/// Doubles per attempt to a cap, offset by a caller-supplied `salt` so writers
/// that conflicted together are less likely to wake together. The salt is an
/// identifier already in hand (the work ticket, the reference) rather than an
/// RNG, which keeps the data plane's write path deterministic; two callers whose
/// salts happen to be congruent modulo the current spread still collide, which
/// costs an attempt and not correctness.
fn lake_commit_backoff(attempt: u32, salt: i64) -> std::time::Duration {
    let base = LAKE_COMMIT_BACKOFF_BASE
        .saturating_mul(1u32 << attempt.min(6))
        .min(LAKE_COMMIT_BACKOFF_CAP);
    let spread = base.as_millis() as u64;
    let offset = if spread == 0 {
        0
    } else {
        salt.unsigned_abs() % spread
    };
    base + std::time::Duration::from_millis(offset)
}

/// Take the lock that serializes writers of the replace-keyed tables. Call
/// inside an open transaction; see `register_files` for why.
pub(super) fn take_registration_lock(conn: &LakeWriter) -> Result<(), Status> {
    let locked = conn
        .execute(
            "UPDATE qiita_lake.registration_lock SET epoch = epoch + 1",
            [],
        )
        .map_err(|e| Status::internal(format!("failed to take the registration lock: {e}")))?;
    // An UPDATE matching no row succeeds and locks nothing, so the serialization
    // would be silently absent and the symptom would be the duplication it exists
    // to prevent. `ensure_registration_lock` seeds the row at boot.
    if locked == 0 {
        return Err(Status::internal(
            "qiita_lake.registration_lock holds no row, so concurrent lake writers \
             would not serialize",
        ));
    }
    Ok(())
}

/// Run `body` inside a DuckLake transaction, re-running the whole thing when the
/// COMMIT fails, until `LAKE_COMMIT_BUDGET` is spent.
///
/// Conflicts surface at COMMIT, not at the statement: measured with 16 concurrent
/// writers contending on `registration_lock`, with the retry disabled, every
/// failure was the COMMIT and none was the lock UPDATE or the replace-by-key
/// DELETE. So an error out of `body` is not contention, will not resolve on a
/// retry, and is surfaced immediately.
///
/// `body` must therefore be idempotent across attempts — each one re-reads a
/// fresh snapshot after DuckLake rolled the last one back. `salt` only spreads
/// the backoff (see `lake_commit_backoff`).
///
/// A COMMIT that reports an error may still have landed: when the catalog's
/// connection drops after Postgres committed, before its answer arrives, the
/// COMMIT errors and the rows are there, and a re-run of an add registers them
/// again (`ducklake_commit_error_after_the_catalog_committed`). So before any
/// re-run, `landed` reads the catalog: given the attempt's value it says whether
/// that attempt's commit is there, and if it is the value is returned. When
/// `landed` cannot tell, the call fails with both errors rather than re-running.
pub(super) fn transact_with_retry<T>(
    conn: &LakeWriter,
    what: &str,
    salt: i64,
    mut body: impl FnMut() -> Result<T, Status>,
    mut landed: impl FnMut(&T) -> Result<bool, Status>,
) -> Result<T, Status> {
    let deadline = std::time::Instant::now() + LAKE_COMMIT_BUDGET;
    let mut attempt: u32 = 0;
    loop {
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(|e| Status::internal(format!("failed to begin transaction: {e}")))?;

        let value = match body() {
            Ok(value) => value,
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(e);
            }
        };

        let commit_error = match conn.execute_batch("COMMIT") {
            Ok(()) => return Ok(value),
            Err(e) => e,
        };
        match landed(&value) {
            Ok(true) => return Ok(value),
            Ok(false) => {}
            Err(e) => {
                return Err(Status::internal(format!(
                    "failed to commit {what}: {commit_error}; whether the commit \
                     landed could not be read: {}",
                    e.message()
                )))
            }
        }

        if std::time::Instant::now() >= deadline {
            return Err(Status::internal(format!(
                "failed to commit {what} within {}s ({} attempts): {commit_error}",
                LAKE_COMMIT_BUDGET.as_secs(),
                attempt.saturating_add(1),
            )));
        }
        std::thread::sleep(lake_commit_backoff(attempt, salt));
        attempt = attempt.saturating_add(1);
    }
}

/// What one `register_files` call did.
#[derive(Debug)]
struct Registration {
    /// Permanent lake paths registered, in payload iteration order.
    registered: Vec<String>,
    /// Rows the replace passes removed, per table — non-zero entries only. Empty
    /// when no `REPLACE_KEY_TABLES` key this call carried was already in the lake
    /// and this ticket had registered none of its `read` prep_samples before.
    ///
    /// Rides back to the control plane in the DoAction body, which logs it: a
    /// delete nothing recorded is the one thing an operator reconciling row
    /// counts cannot reconstruct.
    replaced: BTreeMap<&'static str, usize>,
}

/// Move Parquet files from staging to permanent storage and register in DuckLake.
///
/// Validates all requested files exist in staging, moves them to permanent
/// locations under `data_path/{table_name}/`, and registers them.
///
/// Each file is placed by `move_file`, which copies where it cannot rename or
/// link (e.g., SLURM local scratch → shared NFS).
///
/// Some tables are REPLACED on their key rather than appended to — see
/// `REPLACE_KEY_TABLES` for which, and why. The `read` rows a ticket registered
/// before for a staged prep_sample are replaced too; the comment at that lookup
/// says why.
///
/// Note: the action token is scoped to staging_dir + files, not to a specific
/// reference_idx. The control plane is responsible for issuing tokens only for
/// valid references in the correct state.
fn register_files(
    conn: &LakeWriter,
    data_path: &str,
    scratch_root: &std::path::Path,
    payload: &auth::ActionPayload,
) -> Result<Registration, Status> {
    let staging = std::path::Path::new(&payload.staging_dir);
    let perm_root = std::path::Path::new(data_path);

    // Validate every filename is a safe relative path under
    // staging_dir. Filenames may carry a subdir prefix
    // (e.g. "reference_sequence_chunks/part_00000.parquet") to register
    // multiple parts under one DuckLake table, but must not contain
    // `..` or absolute components. Although `payload.files` is
    // Ed25519-signed by the control plane and so already trusted, this
    // defense-in-depth check keeps the data plane's filesystem
    // contract independent of CP correctness.
    for filename in payload.files.keys() {
        let candidate = std::path::Path::new(filename);
        if candidate.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        }) {
            return Err(Status::invalid_argument(format!(
                "filename must be a relative path with no '..' components: {filename}"
            )));
        }
    }

    // One scope key for the whole registration; it does not vary per file.
    let scope = staging_scope(&payload.staging_dir, scratch_root);

    // Where each file is placed in the lake. Every file must be in staging, or
    // at the destination an earlier call with this payload moved it to.
    let mut placed: Vec<PlacedFile> = Vec::new();
    for (filename, table) in &payload.files {
        let src = staging.join(filename);
        // Multi-file tables carry a subdir prefix in `filename`
        // (e.g. "reference_sequence_chunks/part_00000.parquet"). Use
        // only the basename when placing into the per-table dir —
        // otherwise we'd nest the staging subdir inside it.
        let basename = std::path::Path::new(filename)
            .file_name()
            .and_then(|b| b.to_str())
            .ok_or_else(|| {
                Status::invalid_argument(format!("filename has no UTF-8 basename: {filename}"))
            })?;
        let dest = perm_root.join(table).join(lake_dest_filename(
            payload.work_ticket_idx,
            &scope,
            basename,
        ));
        if !src.exists() && !dest.exists() {
            return Err(Status::not_found(format!(
                "staging file not found: {}",
                src.display()
            )));
        }
        // Both SQL passes below name the destination as a string, so resolve it
        // once here rather than re-deriving (and re-erroring on) it twice.
        let dest_str = dest
            .to_str()
            .ok_or_else(|| Status::internal(format!("non-UTF-8 path: {}", dest.display())))?
            .to_string();
        placed.push(PlacedFile {
            table: table.clone(),
            src,
            dest,
            dest_str,
        });
    }

    // Tables are ensured at startup in main.rs.

    // A file already at its lake destination is a replay of this payload: the
    // caller lost the reply, or died before recording it, and called again. The destination name
    // is deterministic for the (ticket, staging dir, basename), so the replay
    // finds where the earlier call moved each file. When every file was moved and
    // is a data file of its table, the earlier call committed, and its
    // registration is returned without writing: `ducklake_add_data_files`
    // accepts a path it already holds and adds its rows again. Any other state
    // fails closed, because the earlier call may still be registering the files.
    // Judged by the destination, not the source: `move_file` keeps a placed
    // file when removing its staging source fails.
    let moved_before = placed.iter().filter(|f| f.dest.exists()).count();
    if moved_before > 0 {
        let in_catalog = registered_data_files(conn, &placed)?;
        if moved_before == placed.len() && in_catalog.len() == placed.len() {
            return Ok(Registration {
                registered: placed.into_iter().map(|p| p.dest_str).collect(),
                replaced: BTreeMap::new(),
            });
        }
        return Err(Status::failed_precondition(format!(
            "an earlier register_files call for work_ticket {} from {} moved {} of {} \
             files into the lake and registered {} of them. Either it is still \
             running (or the data plane stopped mid-call), or it failed and kept \
             the files at their lake paths, which its error names (the catalog \
             listed some of them, could not be read, or a move back failed). \
             Redrive again once a running call has had time to finish; if this \
             persists, the files need an operator",
            payload.work_ticket_idx,
            payload.staging_dir,
            moved_before,
            placed.len(),
            in_catalog.len()
        )));
    }

    // `read` is not replace-keyed, and DuckLake enforces no uniqueness. A ticket
    // re-runs a step in a new attempt dir under its own workspace, so its second
    // registration of a prep_sample's reads arrives from a new staging dir:
    // `lake_dest_filename` mints a new name and `move_file` has nothing to refuse.
    // The rows this ticket registered before for the staged prep_samples are
    // deleted in the registration transaction below. Only files named for this
    // ticket are deleted from, so rows another ticket registered are left as they
    // are.
    let staged_read = staged_read_prep_samples(conn, staging, &payload.files)?;
    let replace_read_files =
        ticket_read_files_in_lake(conn, payload.work_ticket_idx, &staged_read)?;
    let replace_read_prep_samples: Vec<i64> = staged_read.into_iter().collect();

    // Move all files to permanent storage. `move_file` refuses a destination
    // that exists, so a call racing this one for the same payload fails closed.
    // (DuckLake table, permanent path). The path is carried as a String because
    // every consumer below binds it into SQL or reports it.
    let mut moved: Vec<(String, String)> = Vec::new();
    for (i, file) in placed.iter().enumerate() {
        let dest_dir = file.dest.parent().ok_or_else(|| {
            Status::internal(format!("lake path has no parent: {}", file.dest.display()))
        })?;
        let placed_one = std::fs::create_dir_all(dest_dir)
            .map_err(|e| {
                Status::internal(format!(
                    "failed to create directory {}: {e}",
                    dest_dir.display()
                ))
            })
            .and_then(|()| move_file(&file.src, &file.dest));
        if let Err(e) = placed_one {
            // `move_file` removes a destination of its own when it fails (unless
            // that removal fails too, which it logs), so only the files before
            // this one are this call's to move back.
            return Err(return_to_staging(conn, &placed[..i], e));
        }
        moved.push((file.table.clone(), file.dest_str.clone()));
    }

    // Replace-by-key the `REPLACE_KEY_TABLES` targets, then register every moved
    // file, in ONE DuckLake transaction so the catalog update is all-or-nothing
    // (mirrors delete_reference / delete_mask / delete_pool_reads). A failure
    // part-way through rolls back every prior delete and
    // ducklake_add_data_files call rather than leaving the reference
    // half-registered in the catalog.
    //
    // Atomicity here is CATALOG-LEVEL ONLY: the filesystem moves above are not
    // part of the transaction. When it fails, `return_to_staging` moves the files
    // back, so the call can be made again with the same payload.
    //
    // Registrations that touch a replace-keyed table SERIALIZE against each
    // other, and retry when they lose. Both halves are needed:
    //
    // DuckLake detects a conflict only where two transactions touch the same
    // EXISTING row, so the replace-by-key DELETE serializes writers of a feature
    // the lake already holds — but NOT writers of a feature that is new, whose
    // deletes match nothing. Measured with 4 concurrent writers of one feature:
    // 1 row when it already existed, 4 when it did not, and 4 again for two bare
    // `ducklake_add_data_files` with no delete at all. Bumping
    // `registration_lock` gives every such registration a row to contend for, so
    // the new-feature case conflicts too (measured: back to 1 row).
    //
    // The loser's work is fully re-runnable — its files are already at the paths
    // `lake_dest_filename` minted and the delete+add is idempotent against whatever
    // snapshot it re-reads — so it retries here rather than failing the ticket.
    //
    // Registrations touching none of those tables (read_mask, alignment) skip the
    // lock and so never contend.
    // Which tables this registration replaces by key, and the files whose keys
    // each delete reads — its own, plus its `key_source`'s where the two differ
    // and that table is in this registration too. Loop-invariant, so it is built
    // once outside the retry.
    let mut files_for: BTreeMap<&'static str, Vec<&str>> = BTreeMap::new();
    for (table, dest) in &moved {
        if let Some(entry) = REPLACE_KEY_TABLES
            .iter()
            .find(|candidate| candidate.table == table.as_str())
        {
            files_for
                .entry(entry.table)
                .or_default()
                .push(dest.as_str());
        }
    }
    let incoming: Vec<(&'static ReplaceKey, Vec<&str>)> = REPLACE_KEY_TABLES
        .iter()
        .filter_map(|entry| {
            let mut dests = files_for.get(entry.table)?.clone();
            if entry.key_source != entry.table {
                if let Some(source_dests) = files_for.get(entry.key_source) {
                    dests.extend(source_dests.iter().copied());
                }
            }
            Some((entry, dests))
        })
        .collect();
    let takes_lock = !incoming.is_empty();
    // The `read` rows this ticket registered before (the lookup above the moves
    // says why). The delete names the files that lookup found rather than matching
    // a name pattern in SQL, so `lake_file_ticket` stays the one reader of a ticket
    // out of a file name. Loop-invariant, so built outside the retry; the file
    // paths are bound.
    let read_replace_sql = (!replace_read_files.is_empty()).then(|| {
        format!(
            "DELETE FROM qiita_lake.read WHERE prep_sample_idx IN ({}) AND filename IN ({})",
            sql_i64_list(&replace_read_prep_samples),
            vec!["?"; replace_read_files.len()].join(", ")
        )
    });

    let registration = transact_with_retry(
        conn,
        "the registration transaction",
        payload.work_ticket_idx,
        || {
            if takes_lock {
                take_registration_lock(conn)?;
            }

            // Replace-by-key runs as its OWN pass, ahead of every add: one statement
            // per target table over all the files headed for it, so no delete can
            // touch a row this same registration already added.
            let mut replaced: BTreeMap<&'static str, usize> = BTreeMap::new();
            for (entry, dests) in &incoming {
                let sql = replace_key_delete_sql(entry.table, entry.key, dests.len());
                let params: Vec<&dyn duckdb::ToSql> =
                    dests.iter().map(|d| d as &dyn duckdb::ToSql).collect();
                let deleted = conn.execute(&sql, params.as_slice()).map_err(|e| {
                    let table = entry.table;
                    Status::internal(format!("replace-by-key delete failed for {table}: {e}"))
                })?;
                // Zero is the ordinary case (a first load, or a run whose features
                // are all new) and says nothing; only a real supersede earns an entry.
                if deleted > 0 {
                    replaced.insert(entry.table, deleted);
                }
            }
            if let Some(sql) = &read_replace_sql {
                let params: Vec<&dyn duckdb::ToSql> = replace_read_files
                    .iter()
                    .map(|file| file as &dyn duckdb::ToSql)
                    .collect();
                let deleted = conn.execute(sql, params.as_slice()).map_err(|e| {
                    Status::internal(format!("replace delete failed for read: {e}"))
                })?;
                if deleted > 0 {
                    replaced.insert("read", deleted);
                }
            }

            let mut registered = Vec::new();
            for (table, dest) in &moved {
                conn.execute(
                    "CALL ducklake_add_data_files('qiita_lake', ?, ?)",
                    duckdb::params![table, dest],
                )
                .map_err(|e| {
                    Status::internal(format!(
                        "ducklake_add_data_files failed for {table}/{dest}: {e}"
                    ))
                })?;
                registered.push(dest.clone());
            }
            Ok(Registration {
                registered,
                replaced,
            })
        },
        // One transaction adds every file, so the catalog lists all of them or
        // none; a count in between is a registration this one did not make.
        |_| {
            let listed = registered_data_files(conn, &placed)?.len();
            match listed {
                0 => Ok(false),
                n if n == placed.len() => Ok(true),
                n => Err(Status::internal(format!(
                    "the catalog lists {n} of the {} files",
                    placed.len()
                ))),
            }
        },
    );

    registration.map_err(|e| return_to_staging(conn, &placed, e))
}

/// Move `files` back from the lake to staging after a registration failed past
/// its moves, so a later call with the same payload starts from staging again,
/// and return `failure` with what became of the files appended.
///
/// A COMMIT that reported an error may still have landed — the catalog's
/// connection can drop after the write — so the files go back only when the
/// catalog confirms it holds none of them. Otherwise, or when a move back fails,
/// they stay at their lake paths and the message says so: a replay then fails
/// closed rather than registering a payload twice.
fn return_to_staging(conn: &duckdb::Connection, files: &[PlacedFile], failure: Status) -> Status {
    let moved: Vec<&PlacedFile> = files.iter().filter(|f| f.dest.exists()).collect();
    if moved.is_empty() {
        return failure;
    }
    let count = moved.len();
    let kept = |why: String| {
        Status::new(
            failure.code(),
            format!(
                "{}; {count} file(s) stay at their lake paths unregistered: {why}",
                failure.message()
            ),
        )
    };
    match registered_data_files(conn, files) {
        Ok(listed) if listed.is_empty() => {}
        Ok(listed) => {
            return kept(format!(
                "the catalog lists {} of them, so the registration may have committed",
                listed.len()
            ))
        }
        Err(e) => {
            return kept(format!(
                "the catalog could not be read to confirm it holds none of them ({})",
                e.message()
            ))
        }
    }
    let mut stuck = Vec::new();
    for file in moved {
        // `move_file` keeps a placed file when removing its staging source
        // fails; the lake copy is then the one to remove.
        let back = if file.src.exists() {
            std::fs::remove_file(&file.dest)
                .map_err(|e| Status::internal(format!("remove {}: {e}", file.dest.display())))
        } else {
            move_file(&file.dest, &file.src)
        };
        if let Err(e) = back {
            stuck.push(format!("{}: {}", file.dest.display(), e.message()));
        }
    }
    if stuck.is_empty() {
        Status::new(
            failure.code(),
            format!(
                "{}; the files were moved back to staging",
                failure.message()
            ),
        )
    } else {
        kept(format!("moving them back failed: {}", stuck.join("; ")))
    }
}

/// One file of a registration: its table, its staging source, and its lake
/// destination (with that destination as the string the SQL binds).
struct PlacedFile {
    table: String,
    src: std::path::PathBuf,
    dest: std::path::PathBuf,
    dest_str: String,
}

/// The destinations of `placed` that are already data files of their table in
/// the catalog.
fn registered_data_files(
    conn: &duckdb::Connection,
    placed: &[PlacedFile],
) -> Result<Vec<String>, Status> {
    let lookup_failed =
        |e: duckdb::Error| Status::internal(format!("failed to list lake data files: {e}"));
    let mut by_table: BTreeMap<&str, std::collections::BTreeSet<String>> = BTreeMap::new();
    for file in placed {
        if by_table.contains_key(file.table.as_str()) {
            continue;
        }
        let mut stmt = conn
            .prepare("SELECT data_file FROM ducklake_list_files('qiita_lake', ?)")
            .map_err(lookup_failed)?;
        let files = stmt
            .query_map(duckdb::params![file.table], |r| r.get::<_, String>(0))
            .and_then(|rows| rows.collect::<Result<_, _>>())
            .map_err(lookup_failed)?;
        by_table.insert(file.table.as_str(), files);
    }
    Ok(placed
        .iter()
        .filter(|file| by_table[file.table.as_str()].contains(&file.dest_str))
        .map(|file| file.dest_str.clone())
        .collect())
}

/// The prep_samples the staged `read` files hold. Files headed for other tables
/// are not opened. A row with a NULL `prep_sample_idx` is refused: the replace
/// could never match it.
fn staged_read_prep_samples(
    conn: &duckdb::Connection,
    staging: &std::path::Path,
    files: &std::collections::HashMap<String, String>,
) -> Result<std::collections::BTreeSet<i64>, Status> {
    let mut prep_samples = std::collections::BTreeSet::new();
    for (filename, table) in files {
        if table != "read" {
            continue;
        }
        let path = staging.join(filename);
        let path = path
            .to_str()
            .ok_or_else(|| Status::invalid_argument(format!("non-UTF-8 path: {filename}")))?;
        let read_failed =
            |e: duckdb::Error| Status::internal(format!("failed to read {filename}: {e}"));
        let mut stmt = conn
            .prepare("SELECT DISTINCT prep_sample_idx FROM read_parquet(?)")
            .map_err(read_failed)?;
        let found = stmt
            .query_map(duckdb::params![path], |r| r.get::<_, Option<i64>>(0))
            .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
            .map_err(read_failed)?;
        for prep_sample in found {
            let prep_sample = prep_sample.ok_or_else(|| {
                Status::invalid_argument(format!(
                    "read file {filename} has rows with a NULL prep_sample_idx"
                ))
            })?;
            prep_samples.insert(prep_sample);
        }
    }
    Ok(prep_samples)
}

/// The lake files under `read` that `work_ticket_idx` registered and that hold
/// rows of any of `prep_samples`.
fn ticket_read_files_in_lake(
    conn: &duckdb::Connection,
    work_ticket_idx: i64,
    prep_samples: &std::collections::BTreeSet<i64>,
) -> Result<Vec<String>, Status> {
    if prep_samples.is_empty() {
        return Ok(Vec::new());
    }
    let wanted: Vec<i64> = prep_samples.iter().copied().collect();
    let lookup_failed =
        |e: duckdb::Error| Status::internal(format!("failed to look up read files: {e}"));
    // `filename` is the data file each row is read from.
    let mut stmt = conn
        .prepare(&format!(
            "SELECT DISTINCT filename FROM qiita_lake.read WHERE prep_sample_idx IN ({})",
            sql_i64_list(&wanted)
        ))
        .map_err(lookup_failed)?;
    let files = stmt
        .query_map([], |r| r.get::<_, Option<String>>(0))
        .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
        .map_err(lookup_failed)?;
    Ok(files
        .into_iter()
        .flatten()
        .filter(|file| lake_file_ticket(file) == Some(work_ticket_idx))
        .collect())
}

/// Comma-separated `i64` literals for a SQL `IN (…)` list. The values are
/// integers, so inlining them carries no injection surface.
pub(super) fn sql_i64_list(values: &[i64]) -> String {
    values
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The work ticket a lake data file was registered under, read from the
/// [`LAKE_FILE_TICKET_PREFIX`]`<work_ticket_idx>-` start that `lake_dest_filename`
/// gives it. `None` for a file not named that way.
fn lake_file_ticket(path: &str) -> Option<i64> {
    let name = std::path::Path::new(path).file_name()?.to_str()?;
    let (digits, _) = name
        .strip_prefix(LAKE_FILE_TICKET_PREFIX)?
        .split_once('-')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// The start of every lake file name [`lake_dest_filename`] mints, ahead of the
/// work ticket id.
const LAKE_FILE_TICKET_PREFIX: &str = "wt";

/// Mint a unique, ticket-traceable lake-storage filename for a registered
/// Parquet.
///
/// The producer (the reference-load job) reuses fixed basenames
/// (`part_00000.parquet`, `reference_<table>.parquet`) on every load, so the
/// bare basename is NOT unique within a per-table lake dir: two registrations
/// into the same table would target the same path and the second would clobber
/// the first's live, catalog-registered file. The basename — part index or
/// table name — still distinguishes files within one registration; the two
/// components below separate one registration from another:
///
/// * `wt{work_ticket_idx}` ([`LAKE_FILE_TICKET_PREFIX`]) traces the file back to
///   the ticket that wrote it. `lake_file_ticket` reads the ticket back out, and
///   `register_files` decides a `read` re-registration on it.
/// * A digest of the registration's staging dir separates two loads from ONE
///   ticket. A ticket can load twice: a redrive replays its storage tail, and
///   the ticket alone yields the byte-identical path the first load already
///   registered, which [`move_file`] refuses.
///
///   The scope of that: `staging_dir` is the PRODUCER step's output directory,
///   so it separates the two loads only where the producer itself re-ran — the
///   case a redrive that drops the producer's `qiita.work_ticket_step` row
///   produces. A redrive that leaves the producer fast-forwarded rebuilds its
///   outputs under the original attempt, yielding the same `staging_dir`, and so
///   the same names: `register_files` then finds the files it placed before.
///
/// Deterministic for a given (ticket, staging dir), so a replayed DoAction
/// recomputes the same name, and `register_files` recognises a replay from it:
/// it returns the earlier registration when every file is already a data file of
/// its table, and fails closed otherwise. A random name would hide the replay
/// from it, and the tables outside `REPLACE_KEY_TABLES` (e.g.
/// `reference_membership`), which have no replace-by-key to absorb a second
/// registration, would be registered twice.
///
/// DuckLake names its own INSERT-written data files uniquely for the same
/// reason; this is the equivalent for our "register an existing file" path.
///
/// `scope` comes from [`staging_scope`], not from the raw `staging_dir`.
fn lake_dest_filename(work_ticket_idx: i64, scope: &str, basename: &str) -> String {
    let digest = Sha256::digest(scope.as_bytes());
    // 48 bits, enough to separate the attempts one ticket can have
    // (`max_retries` bounds them). A collision lands on move_file's
    // AlreadyExists, not a silent clobber.
    let hex: String = digest[..6].iter().map(|b| format!("{b:02x}")).collect();
    format!("{LAKE_FILE_TICKET_PREFIX}{work_ticket_idx}-{hex}-{basename}")
}

/// The part of a registration's staging dir that identifies WHICH registration
/// it is, independent of where scratch happens to be mounted.
///
/// `PATH_SCRATCH` is host configuration: a migration, a remount, or a differently
/// laid-out replacement host changes it without changing which registration a
/// given staging dir denotes. Keying [`lake_dest_filename`] on the absolute path
/// would make the digest — and so the destination name — move with it, which
/// would break the determinism that guard depends on. Keying on the path
/// RELATIVE to the scratch root does not.
///
/// A staging dir outside the scratch root falls back to the full path. That is
/// still correct (it only has to be stable and distinct), just not stable across
/// a move of whatever holds it.
fn staging_scope(staging_dir: &str, scratch_root: &std::path::Path) -> String {
    std::path::Path::new(staging_dir)
        .strip_prefix(scratch_root)
        .map(|rel| rel.to_string_lossy().into_owned())
        .unwrap_or_else(|_| staging_dir.to_string())
}

/// Move a file: rename it, or hard-link it and remove the source, or copy it and
/// remove the source, whichever the filesystem allows first.
///
/// Refuses to overwrite an existing destination. Lake data files are
/// registered in the DuckLake catalog by absolute path and written read-only
/// (mode 0440); clobbering one corrupts the lake (or, because of the read-only
/// bit, fails mid-copy with a cryptic EACCES). Callers mint unique destination
/// names ([`lake_dest_filename`]), so a pre-existing dest signals a genuine
/// double-registration — surface it loudly as `AlreadyExists` rather than
/// touching the file.
///
/// The refusal is made by the call that creates the destination (a no-replace
/// rename, a hard link, or a `create_new` copy), never by a check before it, so
/// two calls placing one destination cannot both succeed. On failure the
/// destination this call created is removed; if that removal fails too, a
/// warning names the partial file. If the move succeeds but
/// removing the staging source fails, the dest file is kept (it's the correct
/// data) and a warning names the orphaned source.
fn move_file(src: &std::path::Path, dest: &std::path::Path) -> Result<(), Status> {
    let refused = || {
        Status::already_exists(format!(
            "refusing to overwrite existing lake file {}",
            dest.display()
        ))
    };
    match rename_no_replace(src, dest) {
        Ok(true) => return Ok(()),
        Ok(false) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Err(refused()),
        Err(e) => {
            return Err(Status::internal(format!(
                "rename failed {} → {}: {e}",
                src.display(),
                dest.display()
            )))
        }
    }
    let link_error = match std::fs::hard_link(src, dest) {
        Ok(()) => {
            remove_moved_source(src, dest);
            return Ok(());
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Err(refused()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Status::internal(format!(
                "move failed {} → {}: {e}",
                src.display(),
                dest.display()
            )))
        }
        // Another filesystem, or a link the kernel refuses (Linux's
        // `fs.protected_hardlinks` refuses one to a file the caller does not own
        // and cannot write, and staged files are 0o440): copy instead.
        Err(e) => e,
    };
    copy_no_replace(src, dest).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            refused()
        } else {
            Status::internal(format!(
                "copy failed {} → {} (the hard link was refused: {link_error}): {e}",
                src.display(),
                dest.display()
            ))
        }
    })?;
    remove_moved_source(src, dest);
    Ok(())
}

/// Rename `src` to `dest` unless `dest` exists, checked by the kernel in the same
/// call, so two callers placing one destination cannot both succeed. `Ok(false)`
/// when this platform or filesystem has no such rename, or the two paths are on
/// different filesystems.
fn rename_no_replace(src: &std::path::Path, dest: &std::path::Path) -> std::io::Result<bool> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use std::os::unix::ffi::OsStrExt;
        let c = |p: &std::path::Path| {
            std::ffi::CString::new(p.as_os_str().as_bytes())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
        };
        let (from, to) = (c(src)?, c(dest)?);
        // The raw syscall rather than glibc's wrapper, which only glibc 2.28 and
        // later has. SAFETY: both paths are NUL-terminated and outlive the call.
        #[cfg(target_os = "linux")]
        let rc = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        // SAFETY: as above.
        #[cfg(target_os = "macos")]
        let rc = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
        if rc == 0 {
            return Ok(true);
        }
        let e = std::io::Error::last_os_error();
        // Another filesystem, or one without the flag. ENOTSUP and EOPNOTSUPP
        // are one value on Linux and two on macOS.
        let unsupported = [
            libc::EXDEV,
            libc::EINVAL,
            libc::ENOSYS,
            libc::ENOTSUP,
            libc::EOPNOTSUPP,
        ];
        match e.raw_os_error() {
            Some(n) if unsupported.contains(&n) => Ok(false),
            _ => Err(e),
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (src, dest);
        Ok(false)
    }
}

/// Copy `src` to a `dest` this call creates, refused when `dest` exists. A copy
/// that fails partway removes the `dest` it created; if that removal fails too, a
/// warning names the partial file.
fn copy_no_replace(src: &std::path::Path, dest: &std::path::Path) -> std::io::Result<()> {
    let mut from = std::fs::File::open(src)?;
    let permissions = from.metadata()?.permissions();
    // Created writable; the source's mode is applied once the bytes are written.
    let mut to = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)?;
    let copied =
        std::io::copy(&mut from, &mut to).and_then(|_| std::fs::set_permissions(dest, permissions));
    if let Err(e) = copied {
        drop(to);
        if let Err(cleanup) = std::fs::remove_file(dest) {
            eprintln!(
                "warning: failed to remove partial copy {}: {cleanup}",
                dest.display()
            );
        }
        return Err(e);
    }
    Ok(())
}

/// Remove the staging source of a move that placed `dest`. A failure leaves a
/// stale staging file and is not the move's failure: `dest` has the data.
fn remove_moved_source(src: &std::path::Path, dest: &std::path::Path) {
    if let Err(e) = std::fs::remove_file(src) {
        eprintln!(
            "warning: cleanup of moved file {} failed (dest {} is valid): {e}",
            src.display(),
            dest.display()
        );
    }
}

#[cfg(test)]
#[path = "registration_tests.rs"]
mod tests;
