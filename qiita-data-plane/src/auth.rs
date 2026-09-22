//! Flight ticket Ed25519 verification.
//!
//! Used by the Arrow Flight service to verify signed tickets on do_get,
//! do_action, and do_put.
//!
//! Wire format (all multi-byte integers are big-endian):
//!
//!     <1B version><4B payload_len><payload_len B payload><64B Ed25519 signature><8B expiry_epoch>
//!
//! The signature covers (version || payload_len || payload || expiry). Signing
//! is asymmetric: the control plane holds the private key and signs; this
//! (publicly reachable) data plane holds only the public key and verifies, so a
//! data-plane compromise cannot forge tickets. The version byte is 2 (v1 was
//! HMAC-SHA256 with a 32-byte tag; only v2 is accepted).

use ed25519_dalek::{Signature, VerifyingKey};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

const TICKET_VERSION: u8 = 2;
const SIGNATURE_SIZE: usize = 64;
const EXPIRY_SIZE: usize = 8;
/// Clock skew tolerance in seconds between signing and verifying hosts.
const CLOCK_SKEW_TOLERANCE: u64 = 5;
/// Maximum allowed ticket lifetime from now. Tickets with expiry further out
/// than this are rejected — prevents indefinitely valid tickets from a
/// compromised signing path.
const MAX_TICKET_LIFETIME: u64 = 3600;

/// Typed filter for ticket payloads. Maps column names to sets of allowed values.
/// E.g., {"feature_idx": [1, 2, 3]} restricts DoGet to those feature_idx values.
pub type TicketFilter = HashMap<String, Vec<serde_json::Value>>;

/// Parsed ticket payload after verification.
///
/// Two scoping mechanisms, and a ticket uses exactly one of them (`build_query`
/// enforces which tables accept which):
///
/// * `filter` — the column/value whitelist form (`{"feature_idx": [1, 2, 3]}`),
///   used by every reference table, `read_masked`, and `alignment`.
/// * `members` — the block-read selector: `(prep_sample_idx, sequence_idx
///   sub-range)` tuples, which a flat column filter cannot express. Only the
///   block-read tables (`read_block` / `read_masked_block`) accept it, and they
///   REQUIRE it — see `BLOCK_READ_SOURCES` in `flight_service`.
///
/// Both default to empty so each ticket carries only the shape it uses; the
/// per-table guards in `build_query` reject an under-scoped combination rather
/// than letting an empty scope mean "everything".
///
/// `columns` is orthogonal to both: it narrows what each returned row carries,
/// not which rows are returned. It defaults to empty for the same reason — a
/// ticket that has no opinion omits the field entirely.
///
/// `deny_unknown_fields`, like most structs in this file, and here it guards a
/// specific rollout hazard rather than tidiness. A control plane newer than this
/// data plane will sign fields this build has never heard of; without the guard
/// serde drops them and the request proceeds under an older, WIDER
/// interpretation of a ticket the signer believed it had narrowed. That is
/// silent under-enforcement of a scope, so an unknown field fails the ticket
/// instead — a mixed-version deploy breaks loudly, in the direction that cannot
/// leak.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TicketPayload {
    pub table: String,
    #[serde(default)]
    pub filter: TicketFilter,
    #[serde(default)]
    pub members: Vec<BlockReadMember>,
    #[serde(default)]
    pub columns: Vec<String>,
}

/// Errors from ticket verification.
#[derive(Debug, PartialEq)]
pub enum AuthError {
    TooShort,
    UnsupportedVersion(u8),
    BadLength { expected: usize, actual: usize },
    InvalidSignature,
    Expired,
    ExpiryTooFar,
    MalformedPayload(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::TooShort => write!(f, "ticket too short"),
            AuthError::UnsupportedVersion(v) => write!(f, "unsupported ticket version: {v}"),
            AuthError::BadLength { expected, actual } => {
                write!(
                    f,
                    "ticket length mismatch: expected {expected}, got {actual}"
                )
            }
            AuthError::InvalidSignature => write!(f, "invalid signature"),
            AuthError::Expired => write!(f, "ticket expired"),
            AuthError::ExpiryTooFar => write!(f, "ticket expiry too far in the future"),
            AuthError::MalformedPayload(msg) => write!(f, "malformed payload: {msg}"),
        }
    }
}

/// Verify a signed ticket's Ed25519 signature and expiry, return the raw payload bytes.
///
/// Checks (in order): version, payload length, signature, expiry, max lifetime.
/// The ordering ensures timing information only leaks for structural issues (not
/// for the signature or payload content).
///
/// Use `verify_ticket` for DoGet (parses into `TicketPayload`) or deserialize
/// the returned bytes into an action-specific type for DoAction.
pub fn verify_ticket_raw(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<Vec<u8>, AuthError> {
    // Minimum size: 1 (version) + 4 (payload_len) + 0 (payload) + 64 (sig) + 8 (expiry)
    if ticket.len() < 1 + 4 + SIGNATURE_SIZE + EXPIRY_SIZE {
        return Err(AuthError::TooShort);
    }

    // Version
    let version = ticket[0];
    if version != TICKET_VERSION {
        return Err(AuthError::UnsupportedVersion(version));
    }

    // Payload length
    let payload_len = u32::from_be_bytes([ticket[1], ticket[2], ticket[3], ticket[4]]) as usize;
    let expected_total = 1 + 4 + payload_len + SIGNATURE_SIZE + EXPIRY_SIZE;
    if ticket.len() != expected_total {
        return Err(AuthError::BadLength {
            expected: expected_total,
            actual: ticket.len(),
        });
    }

    let payload_start = 5;
    let payload_end = payload_start + payload_len;
    let sig_start = payload_end;
    let sig_end = sig_start + SIGNATURE_SIZE;
    let expiry_start = sig_end;

    let payload_bytes = &ticket[payload_start..payload_end];
    let signature_bytes = &ticket[sig_start..sig_end];
    let expiry_bytes = &ticket[expiry_start..expiry_start + EXPIRY_SIZE];

    // Verify the Ed25519 signature — covers version + payload_len + payload + expiry
    let signed_input = [
        &ticket[0..1], // version
        &ticket[1..5], // payload_len
        payload_bytes, // payload
        expiry_bytes,  // expiry
    ]
    .concat();

    let sig_array: [u8; SIGNATURE_SIZE] = signature_bytes
        .try_into()
        .map_err(|_| AuthError::InvalidSignature)?;
    let signature = Signature::from_bytes(&sig_array);
    verifying_key
        .verify_strict(&signed_input, &signature)
        .map_err(|_| AuthError::InvalidSignature)?;

    // Check expiry (saturating_add to avoid u64 overflow on crafted input)
    let expiry = u64::from_be_bytes([
        expiry_bytes[0],
        expiry_bytes[1],
        expiry_bytes[2],
        expiry_bytes[3],
        expiry_bytes[4],
        expiry_bytes[5],
        expiry_bytes[6],
        expiry_bytes[7],
    ]);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_secs();
    if now > expiry.saturating_add(CLOCK_SKEW_TOLERANCE) {
        return Err(AuthError::Expired);
    }
    if expiry
        > now
            .saturating_add(MAX_TICKET_LIFETIME)
            .saturating_add(CLOCK_SKEW_TOLERANCE)
    {
        return Err(AuthError::ExpiryTooFar);
    }

    Ok(payload_bytes.to_vec())
}

/// Verify a DoGet ticket and return the parsed payload.
pub fn verify_ticket(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<TicketPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed action payload for DoAction requests.
#[derive(Debug, serde::Deserialize)]
pub struct ActionPayload {
    /// Action type, e.g., "register_files".
    pub action: String,
    /// Staging directory containing the Parquet files to register. Also one of
    /// the two inputs to each placed lake file's name — see
    /// `flight_service::lake_dest_filename` for why both are needed.
    pub staging_dir: String,
    /// Map of {filename: ducklake_table_name}.
    pub files: HashMap<String, String>,
    /// Originating work ticket. Composed with `staging_dir` into each placed
    /// lake file's name (`flight_service::lake_dest_filename`), so the file
    /// traces back to the ticket that wrote it. Required: pinned by
    /// `qiita_control_plane.actions.library.register_files`.
    pub work_ticket_idx: i64,
}

/// Verify a DoAction token and return the parsed action payload.
pub fn verify_action(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<ActionPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `delete_reference` DoAction.
///
/// Wire shape pinned by `qiita_control_plane.actions.library.delete_reference_data`:
/// `{"action": "delete_reference", "reference_idx": N}`. `deny_unknown_fields`
/// keeps the contract tight — the data plane needs only the identifier and
/// computes which features to drop from its own DuckLake `reference_membership`
/// table, so any extra field on the ticket is a design slip surfaced loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteReferencePayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "delete_reference".
    pub action: String,
    /// `i64`, matching the Postgres `reference.reference_idx BIGINT` source of
    /// truth and the `BIGINT` reference_idx columns in the DuckLake tables.
    pub reference_idx: i64,
}

/// Verify a `delete_reference` DoAction token and return its parsed payload.
pub fn verify_delete_reference(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<DeleteReferencePayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `delete_mask` DoAction.
///
/// Wire shape pinned by `qiita_control_plane.actions.library.delete_mask_data`:
/// `{"action": "delete_mask", "mask_idx": N}`. `deny_unknown_fields` keeps the
/// contract tight — the data plane needs only the identifier and drops every
/// row that carries it from its own DuckLake `read_mask` table, so any extra
/// field on the ticket is a design slip surfaced loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteMaskPayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "delete_mask".
    pub action: String,
    /// `i64`, matching the Postgres `mask_definition.idx BIGINT` source of
    /// truth and the `read_mask.mask_idx BIGINT` column in the DuckLake table.
    pub mask_idx: i64,
}

/// Verify a `delete_mask` DoAction token and return its parsed payload.
pub fn verify_delete_mask(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<DeleteMaskPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `delete_pool_reads` DoAction.
///
/// Wire shape pinned by `qiita_control_plane.actions.library.delete_pool_reads_data`:
/// `{"action": "delete_pool_reads", "prep_sample_idxs": [N, ...]}`. The control
/// plane expands a deleted sequenced_pool to its prep_sample set (the `read` /
/// `read_mask` tables carry no run/pool column — the data plane stays "dumb" and
/// deletes only the identifiers it is handed). `deny_unknown_fields` keeps the
/// contract tight — any extra field on the ticket is a design slip surfaced
/// loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletePoolReadsPayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "delete_pool_reads".
    pub action: String,
    /// `i64` set, matching the Postgres `prep_sample` identifier source of truth
    /// and the `prep_sample_idx BIGINT` columns in the DuckLake `read` /
    /// `read_mask` tables. May be empty — the handler then deletes nothing.
    pub prep_sample_idxs: Vec<i64>,
}

/// Verify a `delete_pool_reads` DoAction token and return its parsed payload.
pub fn verify_delete_pool_reads(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<DeletePoolReadsPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `export_read` DoAction.
///
/// Wire shape pinned by `qiita_control_plane.runner._resolve_staged_reads`:
/// `{"action": "export_read", "dest": "<abs path>", "prep_sample_idx": N}`.
/// The data plane re-materializes one prep_sample's reads from its DuckLake `read`
/// table to `dest` on the shared filesystem (a per-ticket `reads.parquet` a
/// read-mask job then consumes) — so the bulk read bytes never transit the
/// control plane. No `work_ticket_idx`: the data plane keys nothing off it
/// (the `dest` path the CP builds already carries the ticket), so carrying it
/// would be a dead field. `deny_unknown_fields` keeps the contract tight: any
/// extra field is a design slip surfaced loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportReadPayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "export_read".
    pub action: String,
    /// `i64`, matching the Postgres `prep_sample` identifier source of truth
    /// and the `read.prep_sample_idx BIGINT` column in the DuckLake table.
    pub prep_sample_idx: i64,
    /// Absolute destination path for the materialized Parquet. The handler
    /// re-validates it (`validate_export_dest`) before writing — under the
    /// data plane's scratch root, no `..`, no single quote — even though the
    /// token is Ed25519-signed by the control plane (defense in depth).
    pub dest: String,
}

/// Verify an `export_read` DoAction token and return its parsed payload.
pub fn verify_export_read(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<ExportReadPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// One member of a block-read selector: a prep_sample and the inclusive
/// `sequence_idx` sub-range of it this block covers. A whole prep_sample is
/// `[start, stop]` == its `qiita.sequence_range`; a prep_sample split across blocks
/// contributes a sub-range to each. `deny_unknown_fields` pins the shape to
/// exactly these three columns.
///
/// Shared by every block-footprint payload — the block-read DoGet tickets
/// (`read_block` / `read_masked_block`) and the `delete_read_mask_block` /
/// `delete_alignment_block` actions — so one selector shape describes a block
/// whether we are reading it or deleting it. `block_read_where_clause` in
/// `flight_service` is the single translator from these members to SQL.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockReadMember {
    /// `i64`, matching the Postgres `prep_sample` identifier source of truth
    /// and the `read.prep_sample_idx BIGINT` column in the DuckLake table.
    pub prep_sample_idx: i64,
    /// Inclusive lower `sequence_idx` bound of this member's sub-range.
    pub sequence_idx_start: i64,
    /// Inclusive upper `sequence_idx` bound of this member's sub-range.
    pub sequence_idx_stop: i64,
}

/// Parsed payload for the `delete_read_mask_block` DoAction — the idempotent
/// block-replace sibling of the `read_block` DoGet selector.
///
/// Wire shape pinned by
/// `qiita_control_plane.actions.library.delete_read_mask_block_data`:
/// `{"action": "delete_read_mask_block", "mask_idx": N,
///   "members": [{"prep_sample_idx": N, "sequence_idx_start": a,
///                "sequence_idx_stop": b}, ...]}`.
/// The data plane deletes exactly this block's footprint from the DuckLake
/// `read_mask` table — the rows for `mask_idx` whose `(prep_sample_idx,
/// sequence_idx)` fall in the members' sub-ranges — so a block re-run can
/// delete-then-re-register without double-counting or clobbering a sibling
/// block's rows for a shared prep_sample. The footprint is the SAME
/// `(prep_sample_idx, sub-range)` member list the `read_block` DoGet selector carries
/// (reusing `BlockReadMember`); it is exact by construction (per-member
/// OR residual), so a split member never deletes a sibling block's tail. The
/// extra `mask_idx` scopes the delete to this filtering identity — the `read`
/// export needs no such column, `read_mask` does. `deny_unknown_fields` keeps
/// the contract tight: any extra field is a design slip surfaced loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteReadMaskBlockPayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "delete_read_mask_block".
    pub action: String,
    /// `i64`, matching the Postgres `mask_definition.idx BIGINT` source of truth
    /// and the `read_mask.mask_idx BIGINT` column in the DuckLake table.
    pub mask_idx: i64,
    /// The block's `(prep_sample_idx, sub-range)` members. The handler rejects
    /// an empty list (an empty block is a control-plane bug, not a valid ask).
    pub members: Vec<BlockReadMember>,
}

/// Verify a `delete_read_mask_block` DoAction token and return its parsed payload.
pub fn verify_delete_read_mask_block(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<DeleteReadMaskBlockPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `delete_alignment_block` DoAction — the idempotent
/// block-replace primitive of the `align` workflow, the alignment twin of
/// `delete_read_mask_block`.
///
/// Wire shape pinned by
/// `qiita_control_plane.actions.library.delete_alignment_block_data`:
/// `{"action": "delete_alignment_block", "alignment_idx": N,
///   "members": [{"prep_sample_idx": N, "sequence_idx_start": a,
///                "sequence_idx_stop": b}, ...]}`.
/// The data plane deletes exactly this block's footprint from the DuckLake
/// `alignment` table — the rows for `alignment_idx` whose `(prep_sample_idx,
/// sequence_idx)` fall in the members' sub-ranges — so a block re-run can
/// delete-then-re-register without double-counting or clobbering a sibling
/// block's rows for a shared prep_sample. The footprint is the SAME
/// `(prep_sample_idx, sub-range)` member list the `read_masked_block` DoGet selector carries
/// (reusing `BlockReadMember`); it is exact by construction (per-member OR
/// residual) and feature_idx-agnostic (all of a read's alignment rows go, since a
/// read produces multiple rows via cross-shard + PE multiplicity). The extra
/// `alignment_idx` scopes the delete to this align-config identity — the raw
/// `read` export needs no such column, the `alignment` sink does.
/// `deny_unknown_fields` keeps the contract tight: any extra field is a design
/// slip surfaced loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteAlignmentBlockPayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "delete_alignment_block".
    pub action: String,
    /// `i64`, matching the Postgres `alignment_definition.alignment_idx BIGINT`
    /// source of truth and the `alignment.alignment_idx BIGINT` DuckLake column.
    pub alignment_idx: i64,
    /// The block's `(prep_sample_idx, sub-range)` members. The handler rejects
    /// an empty list (an empty block is a control-plane bug, not a valid ask).
    pub members: Vec<BlockReadMember>,
}

/// Verify a `delete_alignment_block` DoAction token and return its parsed payload.
pub fn verify_delete_alignment_block(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<DeleteAlignmentBlockPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `delete_alignment` DoAction — the whole-alignment
/// purge, the alignment twin of `delete_mask`.
///
/// Wire shape pinned by `qiita_control_plane.actions.library.delete_alignment_data`:
/// `{"action": "delete_alignment", "alignment_idx": N}`. The data plane deletes
/// every `alignment` row for `alignment_idx` in one DuckLake transaction — the
/// minimal DELETE path the disallow-without-delete resubmission rule requires (a
/// completed `alignment_sample` must be cleared before re-aligning). Idempotent:
/// an alignment whose rows never registered deletes zero rows. `deny_unknown_fields`
/// keeps the contract tight: any extra field is a design slip surfaced loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteAlignmentPayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "delete_alignment".
    pub action: String,
    /// `i64`, matching the Postgres `alignment_definition.alignment_idx BIGINT`
    /// source of truth and the `alignment.alignment_idx BIGINT` DuckLake column.
    pub alignment_idx: i64,
}

/// Verify a `delete_alignment` DoAction token and return its parsed payload.
pub fn verify_delete_alignment(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<DeleteAlignmentPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `delete_alignment_sample` DoAction — the per-prep_sample
/// idempotent replace.
///
/// Wire shape pinned by
/// `qiita_control_plane.actions.library.delete_alignment_sample_data`:
/// `{"action": "delete_alignment_sample", "alignment_idx": N,
///   "prep_sample_idx": M}`. What the pair selects, and why the prep_sample rather
/// than the block or the whole alignment is the unit, is on
/// `flight_service::delete_alignment_sample`. `deny_unknown_fields` rejects any
/// extra field rather than dropping it — a `members` list in particular, which
/// a caller reaching for the block scope would send and which this delete would
/// otherwise ignore while taking the whole prep_sample.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteAlignmentSamplePayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "delete_alignment_sample".
    pub action: String,
    /// `i64`, as `DeleteAlignmentPayload::alignment_idx`.
    pub alignment_idx: i64,
    /// `i64`, matching the `alignment.prep_sample_idx BIGINT` DuckLake column.
    pub prep_sample_idx: i64,
}

/// Verify a `delete_alignment_sample` DoAction token and return its parsed payload.
pub fn verify_delete_alignment_sample(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<DeleteAlignmentSamplePayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `mask_metrics` DoAction.
///
/// Wire shape pinned by `qiita_control_plane.actions.library.mask_metrics_data`:
/// `{"action": "mask_metrics", "mask_idx": N, "prep_sample_idx": M}`. Unlike
/// `count_masked` (which reuses a `read_masked` DoGet ticket because the CLI
/// already holds one), the block reconcile primitive runs control-plane-side and
/// has no such ticket, so this is a first-class action token the CP signs. The
/// data plane aggregates the prep_sample's `read_mask` rows for the mask across ALL
/// its blocks — the per-`(prep_sample, mask)` rollup a per-prep_sample read-mask would
/// have written from its single local parquet, now derived from the persisted
/// DuckLake table because a block-masked prep_sample's rows arrive from several blocks.
/// `deny_unknown_fields` keeps the contract tight: any extra field is a design
/// slip surfaced loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaskMetricsPayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "mask_metrics".
    pub action: String,
    /// `i64`, matching the Postgres `mask_definition.idx BIGINT` source of truth
    /// and the `read_mask.mask_idx BIGINT` column in the DuckLake table.
    pub mask_idx: i64,
    /// `i64`, matching the Postgres `prep_sample` identifier source of truth and
    /// the `read_mask.prep_sample_idx BIGINT` column in the DuckLake table.
    pub prep_sample_idx: i64,
}

/// Verify a `mask_metrics` DoAction token and return its parsed payload.
pub fn verify_mask_metrics(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<MaskMetricsPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed DoPut ticket payload.
///
/// Wire shape pinned by `qiita_control_plane.auth.tickets.sign_doput`:
/// `{"action": "doput", "upload_idx": N}`. `deny_unknown_fields` keeps the
/// upload domain generic — any future per-consumer field on the ticket
/// (reference_idx, study_idx, etc.) would couple this domain to a consumer
/// and trip the deserializer here, surfacing the design slip loudly.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DoPutPayload {
    /// Action discriminator. The gRPC handler also rejects payloads whose
    /// action field is not "doput" — that check lives there because the
    /// handler is the only consumer of this payload today and bundling the
    /// check keeps the auth module shape-only.
    pub action: String,
    /// `i64`, matching the Postgres source of truth
    /// (`qiita.upload.upload_idx BIGINT GENERATED ALWAYS AS IDENTITY`) and
    /// the runner's `::bigint[]` cast in `_resolve_upload_handles`. The
    /// IDENTITY column never reaches `i64::MAX` in practice; using `u64`
    /// here would let a CP-signed ticket carry a value past `i64::MAX`
    /// that `staging_path_for` would still happily turn into a directory
    /// name, diverging from what the Postgres row could ever hold.
    pub upload_idx: i64,
}

/// Verify a DoPut ticket and return the parsed payload.
pub fn verify_doput(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<DoPutPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `sync_reference_exclusion` DoAction.
///
/// Wire shape pinned by
/// `qiita_control_plane.actions.library.sync_reference_exclusion_data`:
/// `{"action": "sync_reference_exclusion", "dest": "<abs path>"}`. The control
/// plane resolves its authoritative blocklist to the excluded `feature_idx` set,
/// writes that set to a single-column Parquet at `dest` on the shared scratch
/// tree, and the data plane REPLACES its `reference_exclusion` mirror wholesale
/// from that file (full-replace ⇒ idempotent / replay-safe). No `reference_idx`
/// or other scoping field: the blocklist is global and the mirror is a flat
/// `feature_idx` set. `deny_unknown_fields` keeps the contract tight: any extra
/// field is a design slip surfaced loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncReferenceExclusionPayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "sync_reference_exclusion".
    pub action: String,
    /// Absolute path to the single-column (`feature_idx`) Parquet the control
    /// plane wrote. The handler re-validates it (`validate_export_dest`) before
    /// inlining it into a `read_parquet(...)` literal — under the data plane's
    /// scratch root, no `..`, no single quote — even though the token is
    /// Ed25519-signed by the control plane (defense in depth). An empty
    /// blocklist still ships a valid zero-row Parquet, so the replace clears the
    /// mirror table.
    pub dest: String,
}

/// Verify a `sync_reference_exclusion` DoAction token and return its payload.
pub fn verify_sync_reference_exclusion(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<SyncReferenceExclusionPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;
