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
#[derive(Debug, serde::Deserialize)]
pub struct TicketPayload {
    pub table: String,
    pub filter: TicketFilter,
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
    /// Staging directory containing the Parquet files to register.
    pub staging_dir: String,
    /// Map of {filename: ducklake_table_name}.
    pub files: HashMap<String, String>,
    /// Originating work ticket. The data plane prefixes each placed lake file
    /// with `wt{work_ticket_idx}-` so destination names are unique across
    /// loads — the producer reuses fixed basenames (e.g. `part_00000.parquet`)
    /// — and trace back to the ticket that wrote them. Required: pinned by
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
/// The data plane re-materializes one sample's reads from its DuckLake `read`
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

/// One member of an `export_read_block` selector: a prep_sample and the
/// inclusive `sequence_idx` sub-range of it this block covers. A whole sample
/// is `[start, stop]` == its `qiita.sequence_range`; a sample split across
/// blocks contributes a sub-range to each. `deny_unknown_fields` pins the shape
/// to exactly these three columns.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportReadBlockMember {
    /// `i64`, matching the Postgres `prep_sample` identifier source of truth
    /// and the `read.prep_sample_idx BIGINT` column in the DuckLake table.
    pub prep_sample_idx: i64,
    /// Inclusive lower `sequence_idx` bound of this member's sub-range.
    pub sequence_idx_start: i64,
    /// Inclusive upper `sequence_idx` bound of this member's sub-range.
    pub sequence_idx_stop: i64,
}

/// Parsed payload for the `export_read_block` DoAction — the block-compute
/// sibling of `export_read`.
///
/// Wire shape pinned by `qiita_control_plane.runner._resolve_staged_reads_block`:
/// `{"action": "export_read_block", "dest": "<abs path>",
///   "members": [{"prep_sample_idx": N, "sequence_idx_start": a,
///                "sequence_idx_stop": b}, ...]}`.
/// The data plane re-materializes the union of the members' `read` sub-ranges
/// from its DuckLake `read` table to `dest` (a per-ticket `reads.parquet` a
/// read-mask *block* job then consumes) — the bulk read bytes never transit the
/// control plane. Constraining on `prep_sample_idx` (not `sequence_idx` alone)
/// keeps the selector correct even where the inner index is only locally unique
/// (the reusable block-compute case). No `work_ticket_idx`: the `dest` path the
/// CP builds already carries the ticket. `deny_unknown_fields` keeps the
/// contract tight: any extra field is a design slip surfaced loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportReadBlockPayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "export_read_block".
    pub action: String,
    /// Absolute destination path for the materialized Parquet. The handler
    /// re-validates it (`validate_export_dest`) before writing — under the
    /// data plane's scratch root, no `..`, no single quote — even though the
    /// token is Ed25519-signed by the control plane (defense in depth).
    pub dest: String,
    /// The block's `(prep_sample_idx, sub-range)` members. The handler rejects
    /// an empty list (an empty block is a control-plane bug, not a valid ask).
    pub members: Vec<ExportReadBlockMember>,
}

/// Verify an `export_read_block` DoAction token and return its parsed payload.
pub fn verify_export_read_block(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<ExportReadBlockPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `export_read_masked_block` DoAction — the MASKED-reads
/// sibling of `export_read_block`.
///
/// Wire shape pinned by
/// `qiita_control_plane.runner._resolve_staged_masked_reads_block`:
/// `{"action": "export_read_masked_block", "dest": "<abs path>", "mask_idx": N,
///   "members": [{"prep_sample_idx": N, "sequence_idx_start": a,
///                "sequence_idx_stop": b}, ...]}`.
/// The data plane re-materializes the union of the members' sub-ranges from its
/// DuckLake `read_masked` VIEW (filtered `mask_idx = ?`, so already trimmed and
/// host/QC-`pass`-filtered) to `dest` — a per-ticket `reads.parquet` the sharded
/// `align_sharded` job then consumes, in the SAME column shape `export_read_block`
/// writes. It is `export_read_block` (dest + members, reusing
/// `ExportReadBlockMember`) plus the `mask_idx` scope — the raw `read` export
/// needs no mask column, a masked export does. `deny_unknown_fields` keeps the
/// contract tight: any extra field is a design slip surfaced loudly here.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportReadMaskedBlockPayload {
    /// Action discriminator; the gRPC handler also rejects a payload whose
    /// `action` is not "export_read_masked_block".
    pub action: String,
    /// Absolute destination path for the materialized Parquet. The handler
    /// re-validates it (`validate_export_dest`) before writing — under the
    /// data plane's scratch root, no `..`, no single quote — even though the
    /// token is Ed25519-signed by the control plane (defense in depth).
    pub dest: String,
    /// `i64`, matching the Postgres `alignment_definition` mask scope and the
    /// `read_mask.mask_idx BIGINT` column the `read_masked` view filters on.
    pub mask_idx: i64,
    /// The block's `(prep_sample_idx, sub-range)` members. The handler rejects
    /// an empty list (an empty block is a control-plane bug, not a valid ask).
    pub members: Vec<ExportReadBlockMember>,
}

/// Verify an `export_read_masked_block` DoAction token and return its payload.
pub fn verify_export_read_masked_block(
    ticket: &[u8],
    verifying_key: &VerifyingKey,
) -> Result<ExportReadMaskedBlockPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, verifying_key)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

/// Parsed payload for the `delete_read_mask_block` DoAction — the idempotent
/// block-replace sibling of `export_read_block`.
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
/// block's rows for a shared sample. The footprint is the SAME
/// `(prep_sample_idx, sub-range)` member list `export_read_block` carries
/// (reusing `ExportReadBlockMember`); it is exact by construction (per-member
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
    pub members: Vec<ExportReadBlockMember>,
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
/// block's rows for a shared sample. The footprint is the SAME
/// `(prep_sample_idx, sub-range)` member list `export_read_masked_block` carries
/// (reusing `ExportReadBlockMember`); it is exact by construction (per-member OR
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
    pub members: Vec<ExportReadBlockMember>,
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

/// Parsed payload for the `mask_metrics` DoAction.
///
/// Wire shape pinned by `qiita_control_plane.actions.library.mask_metrics_data`:
/// `{"action": "mask_metrics", "mask_idx": N, "prep_sample_idx": M}`. Unlike
/// `count_masked` (which reuses a `read_masked` DoGet ticket because the CLI
/// already holds one), the block reconcile primitive runs control-plane-side and
/// has no such ticket, so this is a first-class action token the CP signs. The
/// data plane aggregates the sample's `read_mask` rows for the mask across ALL
/// its blocks — the per-`(prep_sample, mask)` rollup a per-sample read-mask would
/// have written from its single local parquet, now derived from the persisted
/// DuckLake table because a block-masked sample's rows arrive from several blocks.
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

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    // Fixed test keypair. Any 32 bytes is a valid Ed25519 seed; the control
    // plane's cross-language vector (verify_python_signed_ticket) is signed with
    // this same seed, so the derived public key matches Python's byte-for-byte.
    fn test_signing_key() -> SigningKey {
        SigningKey::from_bytes(&[7u8; 32])
    }
    fn test_vk() -> VerifyingKey {
        test_signing_key().verifying_key()
    }

    /// Build a v2 (Ed25519) ticket over an arbitrary payload, signed by `key`.
    fn build_ticket(payload: &[u8], key: &SigningKey, expiry: u64) -> Vec<u8> {
        let version: u8 = TICKET_VERSION;
        let payload_len = (payload.len() as u32).to_be_bytes();
        let expiry_bytes = expiry.to_be_bytes();
        let signed_input = [&[version][..], &payload_len[..], payload, &expiry_bytes[..]].concat();
        let sig = key.sign(&signed_input).to_bytes();
        let mut ticket = Vec::new();
        ticket.push(version);
        ticket.extend_from_slice(&payload_len);
        ticket.extend_from_slice(payload);
        ticket.extend_from_slice(&sig);
        ticket.extend_from_slice(&expiry_bytes);
        ticket
    }

    fn future_expiry(secs_from_now: u64) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + secs_from_now
    }

    const DOGET_PAYLOAD: &[u8] =
        br#"{"filter":{"feature_idx":[1,2,3]},"table":"reference_sequences"}"#;

    #[test]
    fn verify_valid_ticket() {
        let ticket = build_ticket(DOGET_PAYLOAD, &test_signing_key(), future_expiry(300));
        let payload = verify_ticket(&ticket, &test_vk()).expect("valid ticket should verify");
        assert_eq!(payload.table, "reference_sequences");
        assert!(payload.filter.contains_key("feature_idx"));
    }

    #[test]
    fn reject_tampered_payload() {
        let mut ticket = build_ticket(DOGET_PAYLOAD, &test_signing_key(), future_expiry(300));
        ticket[10] ^= 0xFF;
        assert_eq!(
            verify_ticket(&ticket, &test_vk()).unwrap_err(),
            AuthError::InvalidSignature
        );
    }

    #[test]
    fn reject_wrong_key() {
        // A ticket signed by our key must not verify under a DIFFERENT public key
        // — the whole point of asymmetric signing.
        let ticket = build_ticket(DOGET_PAYLOAD, &test_signing_key(), future_expiry(300));
        let other = SigningKey::from_bytes(&[9u8; 32]).verifying_key();
        assert_eq!(
            verify_ticket(&ticket, &other).unwrap_err(),
            AuthError::InvalidSignature
        );
    }

    #[test]
    fn reject_expired_ticket() {
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 100;
        let ticket = build_ticket(DOGET_PAYLOAD, &test_signing_key(), expiry);
        assert_eq!(
            verify_ticket(&ticket, &test_vk()).unwrap_err(),
            AuthError::Expired
        );
    }

    #[test]
    fn reject_expiry_too_far_in_future() {
        let ticket = build_ticket(
            DOGET_PAYLOAD,
            &test_signing_key(),
            future_expiry(100 * 365 * 86400),
        );
        assert_eq!(
            verify_ticket(&ticket, &test_vk()).unwrap_err(),
            AuthError::ExpiryTooFar
        );
    }

    #[test]
    fn reject_truncated_ticket() {
        assert_eq!(
            verify_ticket(&[2, 0, 0, 0], &test_vk()).unwrap_err(),
            AuthError::TooShort
        );
    }

    #[test]
    fn reject_trailing_bytes() {
        let mut ticket = build_ticket(DOGET_PAYLOAD, &test_signing_key(), future_expiry(300));
        ticket.push(0xFF);
        match verify_ticket(&ticket, &test_vk()).unwrap_err() {
            AuthError::BadLength { .. } => {}
            other => panic!("expected BadLength, got {other:?}"),
        }
    }

    #[test]
    fn reject_unsupported_version() {
        // Version is checked before the signature, so a v1 (or garbage) version
        // byte is rejected as UnsupportedVersion even though the sig won't match.
        let mut ticket = build_ticket(DOGET_PAYLOAD, &test_signing_key(), future_expiry(300));
        ticket[0] = 1;
        assert_eq!(
            verify_ticket(&ticket, &test_vk()).unwrap_err(),
            AuthError::UnsupportedVersion(1)
        );
    }

    // -------------------- DoPut --------------------

    #[test]
    fn verify_doput_round_trip() {
        let payload = br#"{"action":"doput","upload_idx":42}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        let parsed = verify_doput(&ticket, &test_vk()).expect("valid ticket should verify");
        assert_eq!(parsed.action, "doput");
        assert_eq!(parsed.upload_idx, 42);
    }

    #[test]
    fn verify_doput_rejects_bad_signature() {
        let payload = br#"{"action":"doput","upload_idx":7}"#;
        let mut ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        ticket[10] ^= 0xFF;
        assert_eq!(
            verify_doput(&ticket, &test_vk()).unwrap_err(),
            AuthError::InvalidSignature
        );
    }

    #[test]
    fn verify_doput_rejects_extra_fields() {
        // deny_unknown_fields: a smuggled field is a contract slip surfaced here.
        let payload = br#"{"action":"doput","reference_idx":99,"upload_idx":1}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        match verify_doput(&ticket, &test_vk()).unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    // -------------------- export_read --------------------

    #[test]
    fn verify_export_read_round_trip() {
        let payload = br#"{"action":"export_read","dest":"/scratch/ticket/804/reads.parquet","prep_sample_idx":26154}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        let parsed = verify_export_read(&ticket, &test_vk()).expect("valid token should verify");
        assert_eq!(parsed.action, "export_read");
        assert_eq!(parsed.prep_sample_idx, 26154);
        assert_eq!(parsed.dest, "/scratch/ticket/804/reads.parquet");
    }

    #[test]
    fn verify_export_read_rejects_extra_fields() {
        let payload =
            br#"{"action":"export_read","dest":"/scratch/x","prep_sample_idx":1,"smuggled":9}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        match verify_export_read(&ticket, &test_vk()).unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    // -------------------- export_read_block --------------------

    #[test]
    fn verify_export_read_block_round_trip() {
        let payload = br#"{"action":"export_read_block","dest":"/scratch/ticket/900/reads.parquet","members":[{"prep_sample_idx":101,"sequence_idx_start":100,"sequence_idx_stop":109},{"prep_sample_idx":103,"sequence_idx_start":300,"sequence_idx_stop":309}]}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        let parsed =
            verify_export_read_block(&ticket, &test_vk()).expect("valid token should verify");
        assert_eq!(parsed.action, "export_read_block");
        assert_eq!(parsed.members.len(), 2);
        assert_eq!(parsed.members[0].prep_sample_idx, 101);
        assert_eq!(parsed.members[1].sequence_idx_stop, 309);
    }

    #[test]
    fn verify_export_read_block_rejects_member_extra_fields() {
        let payload = br#"{"action":"export_read_block","dest":"/scratch/x","members":[{"prep_sample_idx":1,"sequence_idx_start":1,"sequence_idx_stop":2,"smuggled":9}]}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        match verify_export_read_block(&ticket, &test_vk()).unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    #[test]
    fn verify_export_read_block_rejects_extra_fields() {
        // Top-level deny_unknown_fields (distinct from the member-level guard above).
        let payload = br#"{"action":"export_read_block","dest":"/scratch/x","members":[{"prep_sample_idx":1,"sequence_idx_start":1,"sequence_idx_stop":2}],"smuggled":9}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        match verify_export_read_block(&ticket, &test_vk()).unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    // --------------------------------------------------------------------
    // export_read_masked_block action token variant
    // --------------------------------------------------------------------

    fn make_export_read_masked_block_ticket(
        dest: &str,
        mask_idx: i64,
        members: &str,
        key: &SigningKey,
        expiry: u64,
    ) -> Vec<u8> {
        // Canonical JSON: sorted keys, no whitespace. Top-level keys sorted:
        // action, dest, mask_idx, members.
        let payload = format!(
            r#"{{"action":"export_read_masked_block","dest":"{dest}","mask_idx":{mask_idx},"members":{members}}}"#
        );
        build_ticket(payload.as_bytes(), key, expiry)
    }

    #[test]
    fn verify_export_read_masked_block_round_trip() {
        let members =
            r#"[{"prep_sample_idx":101,"sequence_idx_start":100,"sequence_idx_stop":109}]"#;
        let ticket = make_export_read_masked_block_ticket(
            "/scratch/ticket/900/reads.parquet",
            7,
            members,
            &test_signing_key(),
            future_expiry(300),
        );
        let payload = verify_export_read_masked_block(&ticket, &test_vk())
            .expect("valid token should verify");
        assert_eq!(payload.action, "export_read_masked_block");
        assert_eq!(payload.dest, "/scratch/ticket/900/reads.parquet");
        assert_eq!(payload.mask_idx, 7);
        assert_eq!(payload.members.len(), 1);
        assert_eq!(payload.members[0].prep_sample_idx, 101);
        assert_eq!(payload.members[0].sequence_idx_start, 100);
        assert_eq!(payload.members[0].sequence_idx_stop, 109);
    }

    #[test]
    fn verify_export_read_masked_block_rejects_bad_signature() {
        let members = r#"[{"prep_sample_idx":1,"sequence_idx_start":1,"sequence_idx_stop":2}]"#;
        let mut ticket = make_export_read_masked_block_ticket(
            "/scratch/ticket/1/reads.parquet",
            3,
            members,
            &test_signing_key(),
            future_expiry(300),
        );
        ticket[12] ^= 0xFF;
        assert_eq!(
            verify_export_read_masked_block(&ticket, &test_vk()).unwrap_err(),
            AuthError::InvalidSignature
        );
    }

    #[test]
    fn verify_export_read_masked_block_rejects_extra_fields() {
        // deny_unknown_fields: a smuggled top-level field (or a missing mask_idx)
        // is a contract slip surfaced here.
        let payload = br#"{"action":"export_read_masked_block","dest":"/scratch/x","mask_idx":1,"members":[],"smuggled":9}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        match verify_export_read_masked_block(&ticket, &test_vk()).unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    // -------------------- mask_metrics --------------------

    #[test]
    fn verify_mask_metrics_round_trip() {
        let payload = br#"{"action":"mask_metrics","mask_idx":42,"prep_sample_idx":7}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        let parsed = verify_mask_metrics(&ticket, &test_vk()).expect("valid token should verify");
        assert_eq!(parsed.mask_idx, 42);
        assert_eq!(parsed.prep_sample_idx, 7);
    }

    #[test]
    fn verify_mask_metrics_rejects_extra_fields() {
        let payload =
            br#"{"action":"mask_metrics","mask_idx":42,"prep_sample_idx":7,"smuggled":9}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        match verify_mask_metrics(&ticket, &test_vk()).unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    // -------------------- delete_read_mask_block --------------------

    #[test]
    fn verify_delete_read_mask_block_round_trip() {
        let payload = br#"{"action":"delete_read_mask_block","mask_idx":42,"members":[{"prep_sample_idx":101,"sequence_idx_start":100,"sequence_idx_stop":109}]}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        let parsed =
            verify_delete_read_mask_block(&ticket, &test_vk()).expect("valid token should verify");
        assert_eq!(parsed.mask_idx, 42);
        assert_eq!(parsed.members.len(), 1);
    }

    #[test]
    fn verify_delete_read_mask_block_rejects_extra_fields() {
        let payload = br#"{"action":"delete_read_mask_block","mask_idx":42,"members":[{"prep_sample_idx":101,"sequence_idx_start":100,"sequence_idx_stop":109}],"smuggled":9}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        match verify_delete_read_mask_block(&ticket, &test_vk()).unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    // --------------------------------------------------------------------
    // delete_alignment / delete_alignment_block action token variants
    // --------------------------------------------------------------------

    #[test]
    fn verify_delete_alignment_round_trip() {
        let payload = br#"{"action":"delete_alignment","alignment_idx":77}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        let parsed =
            verify_delete_alignment(&ticket, &test_vk()).expect("valid token should verify");
        assert_eq!(parsed.action, "delete_alignment");
        assert_eq!(parsed.alignment_idx, 77);
    }

    #[test]
    fn verify_delete_alignment_rejects_bad_signature() {
        let payload = br#"{"action":"delete_alignment","alignment_idx":1}"#;
        let mut ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        ticket[10] ^= 0xFF;
        assert_eq!(
            verify_delete_alignment(&ticket, &test_vk()).unwrap_err(),
            AuthError::InvalidSignature
        );
    }

    #[test]
    fn verify_delete_alignment_rejects_extra_fields() {
        // deny_unknown_fields: a smuggled field is a contract slip surfaced here.
        let payload = br#"{"action":"delete_alignment","alignment_idx":1,"mask_idx":9}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        match verify_delete_alignment(&ticket, &test_vk()).unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    #[test]
    fn verify_delete_alignment_block_round_trip() {
        let payload = br#"{"action":"delete_alignment_block","alignment_idx":42,"members":[{"prep_sample_idx":101,"sequence_idx_start":100,"sequence_idx_stop":109},{"prep_sample_idx":103,"sequence_idx_start":300,"sequence_idx_stop":309}]}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        let parsed =
            verify_delete_alignment_block(&ticket, &test_vk()).expect("valid token should verify");
        assert_eq!(parsed.action, "delete_alignment_block");
        assert_eq!(parsed.alignment_idx, 42);
        assert_eq!(parsed.members.len(), 2);
        assert_eq!(parsed.members[0].prep_sample_idx, 101);
        assert_eq!(parsed.members[0].sequence_idx_start, 100);
        assert_eq!(parsed.members[0].sequence_idx_stop, 109);
        assert_eq!(parsed.members[1].prep_sample_idx, 103);
    }

    #[test]
    fn verify_delete_alignment_block_rejects_bad_signature() {
        let payload = br#"{"action":"delete_alignment_block","alignment_idx":1,"members":[{"prep_sample_idx":1,"sequence_idx_start":1,"sequence_idx_stop":2}]}"#;
        let mut ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        ticket[12] ^= 0xFF;
        assert_eq!(
            verify_delete_alignment_block(&ticket, &test_vk()).unwrap_err(),
            AuthError::InvalidSignature
        );
    }

    #[test]
    fn verify_delete_alignment_block_rejects_extra_fields() {
        // deny_unknown_fields: a smuggled top-level field is a contract slip.
        let payload =
            br#"{"action":"delete_alignment_block","alignment_idx":1,"members":[],"smuggled":9}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        match verify_delete_alignment_block(&ticket, &test_vk()).unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    // -------------------- delete_pool_reads --------------------

    #[test]
    fn verify_delete_pool_reads_round_trip() {
        let payload = br#"{"action":"delete_pool_reads","prep_sample_idxs":[10,11,12]}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        let parsed =
            verify_delete_pool_reads(&ticket, &test_vk()).expect("valid token should verify");
        assert_eq!(parsed.prep_sample_idxs, vec![10, 11, 12]);
    }

    #[test]
    fn verify_delete_pool_reads_accepts_empty_set() {
        let payload = br#"{"action":"delete_pool_reads","prep_sample_idxs":[]}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        let parsed = verify_delete_pool_reads(&ticket, &test_vk()).expect("should verify");
        assert!(parsed.prep_sample_idxs.is_empty());
    }

    #[test]
    fn verify_delete_pool_reads_rejects_extra_fields() {
        let payload =
            br#"{"action":"delete_pool_reads","prep_sample_idxs":[10,11,12],"smuggled":9}"#;
        let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
        match verify_delete_pool_reads(&ticket, &test_vk()).unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    /// Cross-language interop: this ticket was signed by the Python control plane
    /// (`qiita_control_plane.auth.tickets.sign_ticket`) with the fixed test seed
    /// `[7u8; 32]` — the same seed `test_signing_key()` uses — so the public key
    /// derived here verifies Python's Ed25519 signature byte-for-byte.
    ///
    /// Regenerate from the repo root:
    /// ```bash
    /// cd qiita-control-plane && uv run python3 -c "
    /// from qiita_control_plane.auth.tickets import sign_ticket
    /// t = sign_ticket(table='reference_sequences', filter={'feature_idx':[1,2,3]},
    ///                 secret=bytes([7]*32), expiry_epoch=4102444800)
    /// print(', '.join(str(b) for b in t))"
    /// ```
    ///
    /// The vector's expiry is in 2100 (beyond MAX_TICKET_LIFETIME), so we verify
    /// the signature + structure directly rather than through verify_ticket.
    #[test]
    fn verify_python_signed_ticket() {
        #[rustfmt::skip]
        const PYTHON_SIGNED_TICKET: &[u8] = &[
            2, 0, 0, 0, 64, 123, 34, 102, 105, 108, 116, 101, 114, 34, 58, 123, 34, 102, 101, 97,
            116, 117, 114, 101, 95, 105, 100, 120, 34, 58, 91, 49, 44, 50, 44, 51, 93, 125, 44, 34,
            116, 97, 98, 108, 101, 34, 58, 34, 114, 101, 102, 101, 114, 101, 110, 99, 101, 95, 115,
            101, 113, 117, 101, 110, 99, 101, 115, 34, 125, 140, 118, 190, 90, 173, 150, 129, 253,
            206, 242, 111, 248, 36, 170, 8, 139, 141, 12, 204, 198, 124, 220, 121, 254, 16, 14, 40,
            171, 121, 191, 119, 57, 121, 236, 207, 243, 67, 83, 89, 150, 194, 158, 42, 202, 82, 75,
            75, 0, 10, 226, 1, 82, 95, 204, 7, 243, 146, 239, 225, 79, 83, 203, 20, 7, 0, 0, 0, 0,
            244, 134, 87, 0,
        ];

        let ticket = PYTHON_SIGNED_TICKET;
        let payload_len = u32::from_be_bytes([ticket[1], ticket[2], ticket[3], ticket[4]]) as usize;
        let payload_bytes = &ticket[5..5 + payload_len];
        let sig_start = 5 + payload_len;
        let sig_bytes = &ticket[sig_start..sig_start + SIGNATURE_SIZE];
        let expiry_bytes = &ticket[sig_start + SIGNATURE_SIZE..];

        let signed_input = [&ticket[0..1], &ticket[1..5], payload_bytes, expiry_bytes].concat();
        let sig_array: [u8; SIGNATURE_SIZE] = sig_bytes.try_into().unwrap();
        test_vk()
            .verify_strict(&signed_input, &Signature::from_bytes(&sig_array))
            .expect("Python-signed Ed25519 ticket must verify in Rust");

        let payload: TicketPayload =
            serde_json::from_slice(payload_bytes).expect("payload should parse");
        assert_eq!(payload.table, "reference_sequences");
        assert_eq!(payload.filter.get("feature_idx").unwrap().len(), 3);
    }
}
