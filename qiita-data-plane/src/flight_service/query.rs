//! DoGet query building: the table and filter allowlists, projection, and the
//! SQL each ticket surface turns into.

use tonic::Status;

use crate::auth;

/// Allowed table names for DoGet queries. Reject anything else.
///
/// PRIVACY: the bare `read` and `read_mask` tables are deliberately absent, and
/// must stay absent. A whole-table name here would make an unscoped raw-read
/// SELECT representable. `read_masked` is the only broadly-reachable read
/// surface, and it excludes host/human and QC-failed rows by construction (an
/// unconditional `reason = 'pass'`). It is no longer unrestricted either: it is a
/// table MACRO, not a relation, and what its required arguments foreclose is
/// documented where they are declared (`ducklake.rs`).
///
/// `alignment_origin_spanning` is absent too. It carries `feature_idx`, so
/// exposing it would need the same `reference_exclusion` anti-join
/// `alignment_visible` has — otherwise a blocked genome reaches a consumer
/// through the side table while the view refuses it. Nothing reads it over
/// Flight today, so no view is built.
///
/// `read_block` is the one path to raw `read` rows over Flight, and it is
/// admissible only because it CANNOT express an unscoped read: it is not a table
/// name, it is a block-read *selector* form that `build_query` rejects unless the
/// ticket carries a non-empty `members` list (see `BLOCK_READ_SOURCES`). The
/// bytes it streams are the block's own reads, delivered to one authenticated
/// compute job rather than written as a human-readable Parquet onto a shared
/// filesystem — the narrower of the two transports. Direct DB tooling on the host
/// remains the path for anything unscoped.
pub(super) const ALLOWED_TABLES: &[&str] = &[
    "reference_sequences",
    "reference_sequence_chunks",
    // The exclusion-aware taxonomy view (`reference_taxonomy` ANTI JOIN the
    // resolved blocklist). Reads go through the VIEW, never the raw base table
    // — like `read_masked` over `read`, so a curated exclusion can't be bypassed
    // by any consumer. The raw `reference_taxonomy` is deliberately absent (it is
    // still the register-files write target, but not Flight-readable).
    "reference_taxonomy_visible",
    "reference_phylogeny",
    "reference_placements",
    "reference_annotation",
    "read_masked",
    // Block-read selectors — members-scoped by construction (BLOCK_READ_SOURCES).
    "read_block",
    "read_masked_block",
    // The alignment sink's read-side, for the feature-table (OGU) consumer, as
    // the exclusion-aware VIEW (`alignment` ANTI JOIN the resolved blocklist) —
    // raw `alignment` is deliberately absent so a blocked feature can't reach an
    // OGU rollup. It holds host-depleted, derived per-read alignments (not raw
    // human reads), so — unlike read_masked — it is not the human-read privacy
    // surface. Reads are projected to the ticket's signed column list — required
    // here, unlike every other table — and always scoped by alignment_idx +
    // prep_sample_idx (see build_query / ALIGNMENT_PROJECTION_COLUMNS).
    "alignment_visible",
    // One assembly run's contigs — prep_sample-derived sequence, where everything
    // above is reference data or per-read derived output. Neither table has a
    // prep_sample_idx column; a ticket names the run by `(prep_sample_idx,
    // processing_idx)` and `build_assembly_run_query` resolves it through
    // `qiita_lake.assembly_membership`, the same shape as the `reference_idx`
    // resolution the MEMBERSHIP_JOIN_TABLES get. A `feature_idx` filter is NOT
    // accepted on either, and neither is an empty one.
    "assembled_sequence",
    "assembled_sequence_chunks",
    // Per-subject CheckM quality for one assembly run, read by the feature-table
    // resolver to attach completeness/contamination to the genomes it stages, and
    // by a human through the control plane's assembly run mint.
    //
    // PRIVACY: it holds no sequence and no read — one row per assembled subject
    // carrying marker lineage, completeness, contamination, strain heterogeneity,
    // genome size, contig count and DAS_Tool provenance, over sequence a consumer
    // can already reach through `assembled_sequence`. It carries no `feature_idx`,
    // so nothing it streams joins the shared feature space without the Postgres
    // bridge, and `build_bin_quality_query` refuses any filter that is not exactly
    // one assembly run — there is no unscoped form.
    //
    // Two things this is NOT. It is not aggregated past the contig for every kind:
    // `bin_id` is CheckM's Bin Id column, a refined bin's FASTA stem for a MAG but
    // the ASSEMBLER'S CONTIG ID for an LCG or an UNBINNED row (the orchestrator's
    // `assembly_load` job states this), so for those kinds a row does name a contig
    // — in the assembler's id namespace, not the minted `feature_idx` one, and a
    // human who can mint this for a run can also mint that run's contigs. Its
    // reach has two forms: the human mint signs one prep_sample per ticket behind
    // the same per-principal gate as `assembled_sequence`, and the feature-table
    // resolver signs a whole cohort in-process with no route in front of it.
    //
    // `assembly_membership` stays out regardless: it maps a run to its contigs in
    // the MINTED namespace, which is the join into the shared feature space this
    // table cannot make.
    "bin_quality",
];

/// Allowed column names for filter clauses. All identifier columns that can
/// appear in a signed ticket's filter. Whitelist prevents information leakage
/// via error messages for non-existent columns.
const ALLOWED_FILTER_COLUMNS: &[&str] = &[
    "feature_idx",
    "parent_feature_idx",
    "annotation_idx",
    "reference_idx",
    "node_index",
    "mask_idx",
    "prep_sample_idx",
    // Scopes an alignment DoGet to a single alignment run (feature-table consumer).
    "alignment_idx",
    // With prep_sample_idx, names the assembly RUN an assembly DoGet resolves
    // through `assembly_membership` (`build_assembly_run_query`).
    "processing_idx",
];

/// Columns a signed ticket may ask the alignment DoGet to project: every column
/// of `qiita_lake.alignment`, which `alignment_visible` mirrors (`SELECT a.*`,
/// see `ducklake::ensure_exclusion_tables`). Keep in step with
/// `ensure_alignment_tables`' DDL.
///
/// This is the Rust half of a CP-mirrored pair — the control plane validates the
/// same set at mint time, so an unknown column is refused before it is ever
/// signed. Both halves exist on purpose: the CP's copy turns a consumer's typo
/// into a 422 with a useful message, and this one is the defense-in-depth that
/// keeps a signed name out of interpolated SQL.
///
/// The allowlist is per-table (see `projection_allowlist`) and today only the
/// alignment surface has one; every other DoGet table streams `SELECT *` and
/// refuses a column list outright. Why the asymmetry: `docs/architecture/flight.md`.
const ALIGNMENT_PROJECTION_COLUMNS: &[&str] = &[
    "alignment_idx",
    "prep_sample_idx",
    "sequence_idx",
    "feature_idx",
    "mate_feature_idx",
    "flags",
    "position",
    "stop_position",
    "mapq",
    "cigar",
    "mate_position",
    "template_length",
    "tag_as",
    "tag_xs",
    "tag_ys",
    "tag_xn",
    "tag_xm",
    "tag_xo",
    "tag_xg",
    "tag_nm",
    "tag_yt",
    "tag_md",
    "tag_sa",
];

/// Tables that resolve a `reference_idx` filter through a JOIN against
/// `reference_membership` — they have no `reference_idx` column of their own.
///
/// Named rather than inlined at the one `if` that needs them because the JOIN and
/// the projection do not compose (see `build_query`), and an invariant nothing can
/// name is an invariant nothing can check —
/// `no_membership_join_table_has_a_projection_allowlist` does.
const MEMBERSHIP_JOIN_TABLES: &[&str] = &["reference_sequences", "reference_sequence_chunks"];

/// The projection allowlist for `table`, or `None` when the table takes no
/// column list at all (it streams `SELECT *`, and a list is a control-plane bug).
fn projection_allowlist(table: &str) -> Option<&'static [&'static str]> {
    is_alignment_doget_surface(table).then_some(ALIGNMENT_PROJECTION_COLUMNS)
}

/// The SQL select list for `table`, given the ticket's (possibly empty) column
/// list: the signed columns in the ticket's own order, or `*` for a table that
/// takes no projection.
///
/// **Having an allowlist and requiring a list are the same property.** A table
/// only gets an allowlist because serving it unprojected is the wrong default,
/// so the four cases below are total and there is no "projection is optional
/// here" state to reason about. Splitting them is a one-line change if that ever
/// becomes something we want.
///
/// Every rejection is a control-plane bug rather than client input — the CP
/// validates the same set before signing — so failing loudly is the point: the
/// alternative is quietly serving a different set of columns than was signed.
fn select_list_for(table: &str, columns: &[String]) -> Result<String, Status> {
    match (projection_allowlist(table), columns.is_empty()) {
        (None, true) => Ok("*".to_string()),
        // No server-side default to fall back to, deliberately: the consumer is
        // the only component that knows which columns it binds, and a fallback
        // here would be a second answer to that question, free to drift wider
        // than what was asked for. A ticket minted before this shipped and
        // redeemed after lands here — loudly, inside its 300 s TTL — rather than
        // being silently widened.
        (Some(_), true) => Err(Status::invalid_argument(format!(
            "{table} requires an explicit projection column list"
        ))),
        // Ignoring the list would serve wider rows than the ticket asked for,
        // which is the silent widening this whole mechanism exists to prevent.
        (None, false) => Err(Status::invalid_argument(format!(
            "table {table:?} does not accept a projection column list"
        ))),
        (Some(allowed), false) => {
            check_projection_columns(allowed, columns)?;
            Ok(columns.join(", "))
        }
    }
}

/// Reject a projection column that is not on `allowed`, or named twice.
///
/// Names are whitelisted even though the ticket is signature-verified, because
/// they are interpolated into SQL — the same defense-in-depth argument
/// `ALLOWED_FILTER_COLUMNS` makes. A repeated name is refused rather than
/// deduped: it produces two identically-named Arrow fields, which consumers
/// collapse or reject inconsistently, and picking a behaviour for them would be
/// guessing.
fn check_projection_columns(allowed: &[&str], columns: &[String]) -> Result<(), Status> {
    let mut seen: Vec<&str> = Vec::with_capacity(columns.len());
    for col in columns {
        if !allowed.contains(&col.as_str()) {
            return Err(Status::invalid_argument(format!(
                "unknown projection column: {col:?}"
            )));
        }
        if seen.contains(&col.as_str()) {
            return Err(Status::invalid_argument(format!(
                "duplicate projection column: {col:?}"
            )));
        }
        seen.push(col);
    }
    Ok(())
}

/// The block-read DoGet selectors, mapped to the DuckLake relation each streams.
///
/// These are ticket `table` values, not table names: a block is a set of
/// `(prep_sample_idx, sequence_idx sub-range)` members, which the flat
/// column/value `TicketFilter` cannot express, so the ticket carries `members`
/// and `build_query` translates them through the shared `block_read_where_clause`
/// — the same selector the block DELETE actions use, so a block's read footprint
/// and its delete footprint can never drift.
///
/// * `read_block` → raw `read` rows: the reads a read-mask block masks.
/// * `read_masked_block` → `read_masked` rows (trimmed, host/QC-`pass`-filtered),
///   additionally scoped to one `mask_idx`: the reads an align block aligns.
///
/// Both REQUIRE a non-empty `members` list; `read_masked_block` additionally
/// requires exactly one `mask_idx`. That requirement is what makes `read_block`
/// safe to expose at all (see the PRIVACY note on `ALLOWED_TABLES`): there is no
/// ticket shape that reaches raw reads without naming the exact sub-ranges.
const BLOCK_READ_SOURCES: &[(&str, &str)] =
    &[("read_block", "read"), ("read_masked_block", "read_masked")];

/// The DuckLake relation a block-read selector streams, or `None` if `table` is
/// not one. Single lookup point for both the guard and the query build.
fn block_read_source(table: &str) -> Option<&'static str> {
    BLOCK_READ_SOURCES
        .iter()
        .find(|(name, _)| *name == table)
        .map(|(_, source)| *source)
}

/// The read projection, in `read` / `read_masked` table order. Shared by the
/// per-prep_sample `export_read` DoAction (from `qiita_lake.read`) and by BOTH
/// block-read DoGet selectors (`read_block` from `qiita_lake.read`,
/// `read_masked_block` from the `read_masked` MACRO), so every read payload the
/// data plane hands a compute job has the identical column shape — the shape
/// `align_sharded.reads` / the read-mask jobs bind. `read_masked` exposes exactly
/// these columns (plus `mask_idx`), already trimmed and `pass`-filtered.
pub(super) const EXPORT_READ_COLUMNS: &str =
    "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2";

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
fn i64_list_filter(filter: &auth::TicketFilter, col: &str) -> Result<Vec<i64>, Status> {
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
fn block_member_preps(members: &[auth::BlockReadMember]) -> Vec<i64> {
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
fn read_masked_relation(mask_idx: i64, preps: &[i64]) -> String {
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

/// The alignment DoGet surface: the exclusion-aware view `alignment_visible`, and
/// ONLY that — never the raw `alignment` base table (which is out of
/// `ALLOWED_TABLES`, so unreachable via `do_get`). `build_query` gives this name
/// the mandatory projection column list and the mandatory (non-empty, single
/// `alignment_idx`) scoping. Deliberately NOT recognizing the raw name: if
/// `"alignment"` were ever re-added to `ALLOWED_TABLES` by mistake, it would fall
/// through to a bare `SELECT *`, producing an obviously-malformed, unscoped result
/// rather than a clean-looking one that silently bypasses exclusion — the failure
/// stays loud.
fn is_alignment_doget_surface(table: &str) -> bool {
    table == "alignment_visible"
}

/// Tables `build_query` refuses to serve on an empty filter.
///
/// The `reference_*` tables are broadly readable by design (an unfiltered SELECT
/// there mirrors the anonymous REST `GET /reference/{idx}`), so the refusal is
/// per-table rather than global. What is listed is the one table an empty filter
/// would turn into the whole alignment sink. The assembly surfaces are not
/// listed and do not need to be: like `read_masked`, their scope IS their query
/// shape (`build_assembly_run_query`), which has no empty form to refuse.
fn requires_scoped_filter(table: &str) -> bool {
    is_alignment_doget_surface(table)
}

/// The DoGet surfaces whose scope is one assembly RUN.
///
/// Neither carries `prep_sample_idx` — a contig is stored once, keyed by the
/// content-deduped `feature_idx` it shares with every other run that produced
/// the same bytes — so which contigs are "this run's" is a fact held by
/// `assembly_membership`, and `build_assembly_run_query` reads it there.
fn is_assembly_run_surface(table: &str) -> bool {
    matches!(table, "assembled_sequence" | "assembled_sequence_chunks")
}

/// The per-subject quality surface, scoped to one assembly run over a cohort.
///
/// Separate from `is_assembly_run_surface` because the scope is a different shape,
/// not a different table: `bin_quality` carries `prep_sample_idx` and
/// `processing_idx` as real columns, so its run scope is a WHERE clause on itself
/// and needs none of the `assembly_membership` semi join those two require.
fn is_bin_quality_surface(table: &str) -> bool {
    table == "bin_quality"
}

/// Build a SQL query for the given table and filter.
///
/// SQL injection defense model:
/// - Table name: whitelist (`ALLOWED_TABLES`) — only known-safe values
/// - Column names: whitelist (`ALLOWED_FILTER_COLUMNS`) — only known identifier columns
/// - Values: parsed as i64 then stringified — no string data reaches SQL
/// - All inputs are also signature-verified (set by the control plane, not the client)
///
/// DuckDB does not support parameterized identifiers (table/column names), so
/// whitelisting is the correct defense. Values could be parameterized but are
/// already safe as parsed integers.
pub(super) fn build_query(
    table: &str,
    filter: &auth::TicketFilter,
    members: &[auth::BlockReadMember],
    columns: &[String],
) -> Result<(String, String), Status> {
    // Resolve the projection FIRST, before any early return below can build SQL
    // on its own path — otherwise the block-read and `read_masked` selectors
    // would silently ignore a column list rather than refuse it, which is the
    // silent-widening failure this mechanism exists to prevent.
    let select_list = select_list_for(table, columns)?;

    // Block-read selectors resolve to a different relation than their ticket name
    // and are scoped by `members`, not by a column filter — handle them first, and
    // reject `members` on any other table so a stray selector can never silently
    // widen a normal ticket into an unscoped read.
    if let Some(source) = block_read_source(table) {
        return build_block_read_query(table, source, filter, members);
    }
    if !members.is_empty() {
        return Err(Status::invalid_argument(format!(
            "table {table:?} does not accept a block `members` selector"
        )));
    }

    let full_table = format!("qiita_lake.{table}");

    // `read_masked` is a table macro whose (mask, prep_samples) scope IS its argument
    // list, so it cannot be assembled by the generic WHERE-clause path below.
    if table == "read_masked" {
        return build_read_masked_query(filter);
    }

    // An assembly surface is scoped by a run, which is a fact in another table
    // rather than a column of this one — also not a generic WHERE clause.
    if is_assembly_run_surface(table) {
        return build_assembly_run_query(table, filter);
    }

    // Scoped by a run it carries itself, and refused outright without one.
    if is_bin_quality_surface(table) {
        return build_bin_quality_query(filter);
    }

    if filter.is_empty() {
        // Defense-in-depth against a full-table read leak. The human-read surface
        // no longer reaches here at all — `read_masked` is a macro that cannot be
        // called without a scope (above), which retires the "requiring
        // prep_sample_idx for read_masked" half of the follow-up this comment used
        // to track. `alignment_visible` is still guarded here: the CP always
        // scopes it to (alignment_idx, prep_sample_idx), and an empty filter would
        // dump the whole sink (and bypass the projection). This rejects only the
        // *empty* case, not every under-scoped one: a non-empty but non-scoping
        // filter (e.g. feature_idx alone) still passes today. Making an unfiltered
        // read opt-in via an allowlist is still a tracked durability follow-up.
        // Which tables are refused, and why the reference_* ones are not, is at
        // `requires_scoped_filter`.
        if requires_scoped_filter(table) {
            return Err(Status::invalid_argument(format!(
                "{table} requires a non-empty filter (refusing full-table read)"
            )));
        }
        return Ok((
            format!("SELECT {select_list} FROM {full_table}"),
            full_table,
        ));
    }

    // A feature-table DoGet builds a table for exactly ONE alignment run, and a
    // consumer typically leaves alignment_idx out of its projection (every row
    // shares it), so require it present and single-valued. Otherwise a ticket
    // could omit the scope, or pass several alignment_idx values and blend rows
    // from heterogeneous runs into one indistinguishable stream. Fail loud.
    if is_alignment_doget_surface(table) {
        match filter.get("alignment_idx") {
            Some(values) if values.len() == 1 => {}
            _ => {
                return Err(Status::invalid_argument(
                    "alignment DoGet requires exactly one alignment_idx value",
                ));
            }
        }
    }

    // The MEMBERSHIP_JOIN_TABLES have no reference_idx column of their own. When
    // the filter includes reference_idx, resolve it via a JOIN with the membership
    // table.
    let needs_membership_join =
        MEMBERSHIP_JOIN_TABLES.contains(&table) && filter.contains_key("reference_idx");

    let mut where_clauses = Vec::new();
    for (col, values) in filter {
        // Whitelist column names — all SQL is constructed from known-safe identifiers.
        // Input is signature-verified (set by control plane), but we validate anyway for
        // defense-in-depth.
        if !ALLOWED_FILTER_COLUMNS.contains(&col.as_str()) {
            return Err(Status::invalid_argument(format!(
                "unknown filter column: {col:?}"
            )));
        }
        if values.is_empty() {
            return Err(Status::invalid_argument(format!(
                "filter column {col:?} has empty values list"
            )));
        }
        // Build IN clause with integer values only
        let int_values: Vec<i64> = values
            .iter()
            .map(|v| {
                v.as_i64().ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "filter values for {col:?} must be integers, got {v}"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        let csv = int_values
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",");

        if needs_membership_join && col == "reference_idx" {
            // Applied as a WHERE on the joined membership table alias.
            where_clauses.push(format!("m.reference_idx IN ({csv})"));
        } else if needs_membership_join {
            // Under the membership JOIN, feature_idx exists on BOTH the base
            // table (t) and the membership table (m), so an unqualified
            // reference is ambiguous — a combined {reference_idx, feature_idx}
            // filter (what the CP's feature_idx-scoped DoGet ticket mints)
            // would otherwise fail to bind. Qualify with the base alias.
            where_clauses.push(format!("t.{col} IN ({csv})"));
        } else {
            where_clauses.push(format!("{col} IN ({csv})"));
        }
    }

    let where_str = where_clauses.join(" AND ");
    let sql = if needs_membership_join {
        // This arm hardcodes `SELECT t.*` and does NOT use `select_list`, so a
        // projection reaching it would be silently dropped and the stream would
        // carry wider rows than the ticket signed. That is the one failure the
        // projection mechanism exists to prevent, so refuse instead.
        //
        // Unreachable twice over today: no MEMBERSHIP_JOIN_TABLES entry has a
        // projection allowlist (pinned by
        // `no_membership_join_table_has_a_projection_allowlist`), and
        // `select_list_for` at the top of this function already rejects a list for
        // an allowlist-less table. The day one of them gains an allowlist, this is
        // what stands between that and a silently widened stream — the two
        // features compose badly (under the JOIN a bare column name is ambiguous,
        // since both sides carry feature_idx, so a projection here would need
        // `t.`-qualifying), so it must be a decision, not a default.
        if !columns.is_empty() {
            return Err(Status::internal(format!(
                "projection column list is not supported on {table:?} (membership JOIN)"
            )));
        }
        format!(
            "SELECT t.* FROM {full_table} t \
             JOIN qiita_lake.reference_membership m ON t.feature_idx = m.feature_idx \
             WHERE {where_str}"
        )
    } else {
        format!("SELECT {select_list} FROM {full_table} WHERE {where_str}")
    };
    Ok((sql, full_table))
}

/// Build the `read_masked` DoGet: a call to the table macro, not a filtered SELECT.
///
/// The ticket must name exactly the macro's scope — one `mask_idx` and a non-empty
/// `prep_sample_idx` set — which is what every control-plane signing site produces
/// (deliberately not enumerated: the list drifts, and `grep 'read_masked'` on the
/// control plane is exact). Any other column is refused rather than appended as an
/// outer filter: on the human-read surface an unrecognised scope column is a
/// control-plane bug, and quietly reading "the macro's scope, plus whatever else"
/// is how an under-scoped read would pass.
fn build_read_masked_query(filter: &auth::TicketFilter) -> Result<(String, String), Status> {
    let mask_idx = single_i64_filter(filter, "mask_idx")?;
    let preps = i64_list_filter(filter, "prep_sample_idx")?;
    if filter.len() != 2 {
        return Err(Status::invalid_argument(format!(
            "read_masked accepts only mask_idx and prep_sample_idx, got {} columns",
            filter.len()
        )));
    }
    Ok((
        format!("SELECT * FROM {}", read_masked_relation(mask_idx, &preps)),
        "qiita_lake.read_masked".to_string(),
    ))
}

/// Build the assembly DoGet: one run's contigs, selected by a semi join against
/// the lake's own `assembly_membership` rather than by a roster the ticket
/// carries.
///
/// The ticket names the run and nothing else — exactly one `prep_sample_idx`,
/// exactly one `processing_idx`, no third column. Several values, or an extra
/// column, would blend contigs from heterogeneous runs into one
/// indistinguishable stream, the same failure the single-`alignment_idx` guard
/// prevents; `feature_idx` in particular is refused, so no ticket can name
/// contigs directly on these tables.
///
/// `IN (subquery)`, not a literal list: DuckDB plans it as a SEMI hash join and
/// pushes the resolved keys' min/max and a Bloom filter into the lake scan as
/// dynamic filters. Measured on DuckDB 1.5.4 / ducklake d318a545, a catalog of
/// 3.6M chunk rows over 200 files, a 26,129-contig run: this form's scan emits
/// 245,457 rows in 140 ms, while the same roster as 26,129 literals is rewritten
/// into a MARK join above an unfiltered scan — 3,600,000 rows, 1,793 ms — with
/// both forms opening the same 200 files and returning the same 235,161 rows.
/// Where per-file `feature_idx` ranges are narrow enough to prune at all, the two
/// prune identically (1 file of 200 on a contiguous run). Semi-join semantics
/// also make the DISTINCT implicit: a contig that two `(kind, bin_id)` rows claim
/// is one output row, not two.
///
/// `assembly_membership` stays out of `ALLOWED_TABLES` — it is readable here as
/// the scope resolver, never as a stream: no column of it reaches the output.
fn build_assembly_run_query(
    table: &str,
    filter: &auth::TicketFilter,
) -> Result<(String, String), Status> {
    let prep_sample_idx = single_i64_filter(filter, "prep_sample_idx")?;
    let processing_idx = single_i64_filter(filter, "processing_idx")?;
    if filter.len() != 2 {
        return Err(Status::invalid_argument(format!(
            "{table} accepts only prep_sample_idx and processing_idx, got {} columns",
            filter.len()
        )));
    }
    let full_table = format!("qiita_lake.{table}");
    Ok((
        format!(
            "SELECT * FROM {full_table} WHERE feature_idx IN (\
             SELECT feature_idx FROM qiita_lake.assembly_membership \
             WHERE prep_sample_idx = {prep_sample_idx} AND processing_idx = {processing_idx})"
        ),
        full_table,
    ))
}

/// Build the run-scoped SELECT for `bin_quality`.
///
/// One assembly RUN, over a cohort: exactly one `processing_idx`, a non-empty
/// `prep_sample_idx` set, and nothing else. Both halves are required because
/// either alone widens past the run — `processing_idx` alone is every prep_sample that
/// run touched, `prep_sample_idx` alone is every run those prep_samples ever had — and
/// the `filter.len()` check is what stops a third column from riding along
/// unnoticed.
///
/// A cohort where `build_assembly_run_query` takes one prep_sample: this table's rows
/// are per subject rather than per contig, so a whole cohort answers in one
/// stream. The rows carry `prep_sample_idx`, so a prep_sample that scored nothing is
/// absent from the result rather than merged into a neighbour's rows.
fn build_bin_quality_query(filter: &auth::TicketFilter) -> Result<(String, String), Status> {
    let processing_idx = single_i64_filter(filter, "processing_idx")?;
    let prep_sample_idx = i64_list_filter(filter, "prep_sample_idx")?;
    if filter.len() != 2 {
        return Err(Status::invalid_argument(format!(
            "bin_quality accepts only prep_sample_idx and processing_idx, got {} columns",
            filter.len()
        )));
    }
    let full_table = "qiita_lake.bin_quality".to_string();
    let preps = prep_sample_idx
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Ok((
        format!(
            "SELECT * FROM {full_table} \
             WHERE processing_idx = {processing_idx} AND prep_sample_idx IN ({preps})"
        ),
        full_table,
    ))
}

/// Build the SELECT for a block-read DoGet (`read_block` / `read_masked_block`).
///
/// Source relation, `block_read_where_clause` selector and `EXPORT_READ_COLUMNS`
/// projection are all shared with the block DELETE path — deliberately: a block's read footprint and its delete
/// footprint are the same footprint, and one translator means they cannot drift.
///
/// Guards, all fail-loud (a violation is a control-plane bug, and the ticket is
/// signature-verified, so these are defense in depth):
///
/// * `members` must be non-empty. This is the invariant that makes `read_block`
///   admissible at all — an empty selector must never degrade to "all reads"
///   (see the PRIVACY note on `ALLOWED_TABLES`).
/// * `read_masked_block` must carry exactly one `mask_idx` and nothing else;
///   `read_block` must carry no filter at all. A masked block without its mask
///   scope would stream every mask's rows for those ranges, blending pass-sets
///   from different filtering identities into one indistinguishable stream — the
///   same class of error the single-`alignment_idx` guard prevents.
fn build_block_read_query(
    table: &str,
    source: &str,
    filter: &auth::TicketFilter,
    members: &[auth::BlockReadMember],
) -> Result<(String, String), Status> {
    if members.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{table} requires a non-empty `members` selector (refusing an unscoped read)"
        )));
    }
    let full_table = format!("qiita_lake.{source}");
    let member_clause = block_read_where_clause(members);

    // `read_masked` is a macro (scope-as-arguments); `read` is a plain relation.
    // The member clause stays an outer filter either way — it carries the
    // per-prep_sample sequence sub-ranges the macro's prep_sample scope does not express,
    // and it is the SAME selector the block DELETE path uses, so a block's read
    // footprint and its delete footprint cannot drift.
    let (relation, where_str) = if table == "read_masked_block" {
        let mask_idx = single_i64_filter(filter, "mask_idx")?;
        if filter.len() != 1 {
            return Err(Status::invalid_argument(format!(
                "{table} accepts only a mask_idx filter, got {} columns",
                filter.len()
            )));
        }
        // `mask_idx` moves into the macro call; the members' prep_samples scope its
        // `read`/`read_mask` inputs so DuckLake prunes to their files rather than
        // scanning the lake (see the measurements on the macro in ducklake.rs).
        let relation = read_masked_relation(mask_idx, &block_member_preps(members));
        (relation, member_clause)
    } else {
        if !filter.is_empty() {
            return Err(Status::invalid_argument(format!(
                "{table} accepts no filter columns (scope it with `members`), got {} columns",
                filter.len()
            )));
        }
        (full_table.clone(), member_clause)
    };

    Ok((
        format!("SELECT {EXPORT_READ_COLUMNS} FROM {relation} WHERE {where_str}"),
        full_table,
    ))
}

#[cfg(test)]
#[path = "query_tests.rs"]
mod tests;
