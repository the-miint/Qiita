//! Flight ticket HMAC-SHA256 verification.
//!
//! Used by the Arrow Flight service to verify signed tickets on do_get,
//! do_action, and do_put.
//!
//! Wire format (all multi-byte integers are big-endian):
//!
//!     <1B version><4B payload_len><payload_len B payload><32B HMAC-SHA256><8B expiry_epoch>
//!
//! The HMAC covers (version || payload_len || payload || expiry).
//! The version byte is always 1 for now.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

const TICKET_VERSION: u8 = 1;
const HMAC_SIZE: usize = 32;
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
    InvalidHmac,
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
            AuthError::InvalidHmac => write!(f, "invalid HMAC signature"),
            AuthError::Expired => write!(f, "ticket expired"),
            AuthError::ExpiryTooFar => write!(f, "ticket expiry too far in the future"),
            AuthError::MalformedPayload(msg) => write!(f, "malformed payload: {msg}"),
        }
    }
}

/// Verify a signed ticket's HMAC and expiry, return the raw payload bytes.
///
/// Checks (in order): version, payload length, HMAC signature, expiry, max
/// lifetime. HMAC verification is constant-time. The ordering ensures timing
/// information only leaks for structural issues (not for HMAC or payload content).
///
/// Use `verify_ticket` for DoGet (parses into `TicketPayload`) or deserialize
/// the returned bytes into an action-specific type for DoAction.
pub fn verify_ticket_raw(ticket: &[u8], secret: &[u8]) -> Result<Vec<u8>, AuthError> {
    // Minimum size: 1 (version) + 4 (payload_len) + 0 (payload) + 32 (hmac) + 8 (expiry)
    if ticket.len() < 1 + 4 + HMAC_SIZE + EXPIRY_SIZE {
        return Err(AuthError::TooShort);
    }

    // Version
    let version = ticket[0];
    if version != TICKET_VERSION {
        return Err(AuthError::UnsupportedVersion(version));
    }

    // Payload length
    let payload_len = u32::from_be_bytes([ticket[1], ticket[2], ticket[3], ticket[4]]) as usize;
    let expected_total = 1 + 4 + payload_len + HMAC_SIZE + EXPIRY_SIZE;
    if ticket.len() != expected_total {
        return Err(AuthError::BadLength {
            expected: expected_total,
            actual: ticket.len(),
        });
    }

    let payload_start = 5;
    let payload_end = payload_start + payload_len;
    let hmac_start = payload_end;
    let hmac_end = hmac_start + HMAC_SIZE;
    let expiry_start = hmac_end;

    let payload_bytes = &ticket[payload_start..payload_end];
    let received_hmac = &ticket[hmac_start..hmac_end];
    let expiry_bytes = &ticket[expiry_start..expiry_start + EXPIRY_SIZE];

    // Verify HMAC (constant-time) — covers version + payload_len + payload + expiry
    let mac_input = [
        &ticket[0..1], // version
        &ticket[1..5], // payload_len
        payload_bytes, // payload
        expiry_bytes,  // expiry
    ]
    .concat();

    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC can take keys of any size");
    mac.update(&mac_input);
    mac.verify_slice(received_hmac)
        .map_err(|_| AuthError::InvalidHmac)?;

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
pub fn verify_ticket(ticket: &[u8], secret: &[u8]) -> Result<TicketPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, secret)?;
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
pub fn verify_action(ticket: &[u8], secret: &[u8]) -> Result<ActionPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, secret)?;
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
    secret: &[u8],
) -> Result<DeleteReferencePayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, secret)?;
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
pub fn verify_delete_mask(ticket: &[u8], secret: &[u8]) -> Result<DeleteMaskPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, secret)?;
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
    secret: &[u8],
) -> Result<DeletePoolReadsPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, secret)?;
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
    /// token is HMAC-signed by the control plane (defense in depth).
    pub dest: String,
}

/// Verify an `export_read` DoAction token and return its parsed payload.
pub fn verify_export_read(ticket: &[u8], secret: &[u8]) -> Result<ExportReadPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, secret)?;
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
pub fn verify_doput(ticket: &[u8], secret: &[u8]) -> Result<DoPutPayload, AuthError> {
    let payload_bytes = verify_ticket_raw(ticket, secret)?;
    serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::MalformedPayload(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_ticket(secret: &[u8], expiry: u64) -> Vec<u8> {
        // Reproduce the Python wire format for cross-language testing
        let payload = br#"{"filter":{"feature_idx":[1,2,3]},"table":"reference_sequences"}"#;
        let version: u8 = 1;
        let payload_len = (payload.len() as u32).to_be_bytes();
        let expiry_bytes = expiry.to_be_bytes();

        let mac_input = [
            &[version][..],
            &payload_len[..],
            &payload[..],
            &expiry_bytes[..],
        ]
        .concat();

        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(&mac_input);
        let hmac_result = mac.finalize().into_bytes();

        let mut ticket = Vec::new();
        ticket.push(version);
        ticket.extend_from_slice(&payload_len);
        ticket.extend_from_slice(payload);
        ticket.extend_from_slice(&hmac_result);
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

    #[test]
    fn verify_valid_ticket() {
        let ticket = make_test_ticket(b"dev-secret", future_expiry(300));
        let payload = verify_ticket(&ticket, b"dev-secret").expect("valid ticket should verify");
        assert_eq!(payload.table, "reference_sequences");
        assert!(payload.filter.contains_key("feature_idx"));
    }

    #[test]
    fn reject_tampered_payload() {
        let mut ticket = make_test_ticket(b"dev-secret", future_expiry(300));
        ticket[10] ^= 0xFF;
        assert_eq!(
            verify_ticket(&ticket, b"dev-secret").unwrap_err(),
            AuthError::InvalidHmac
        );
    }

    #[test]
    fn reject_wrong_secret() {
        let ticket = make_test_ticket(b"dev-secret", future_expiry(300));
        assert_eq!(
            verify_ticket(&ticket, b"wrong-secret").unwrap_err(),
            AuthError::InvalidHmac
        );
    }

    #[test]
    fn reject_expired_ticket() {
        // Expired 100 seconds ago (well past the 5s clock skew tolerance)
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 100;
        let ticket = make_test_ticket(b"dev-secret", expiry);
        assert_eq!(
            verify_ticket(&ticket, b"dev-secret").unwrap_err(),
            AuthError::Expired
        );
    }

    #[test]
    fn reject_expiry_too_far_in_future() {
        // Expiry 100 years from now — beyond MAX_TICKET_LIFETIME
        let ticket = make_test_ticket(b"dev-secret", future_expiry(100 * 365 * 86400));
        assert_eq!(
            verify_ticket(&ticket, b"dev-secret").unwrap_err(),
            AuthError::ExpiryTooFar
        );
    }

    #[test]
    fn reject_truncated_ticket() {
        assert_eq!(
            verify_ticket(&[1, 0, 0, 0], b"secret").unwrap_err(),
            AuthError::TooShort
        );
    }

    #[test]
    fn reject_trailing_bytes() {
        let mut ticket = make_test_ticket(b"dev-secret", future_expiry(300));
        ticket.push(0xFF); // append garbage
        let err = verify_ticket(&ticket, b"dev-secret").unwrap_err();
        match err {
            AuthError::BadLength { .. } => {} // expected
            other => panic!("expected BadLength, got {other:?}"),
        }
    }

    #[test]
    fn reject_unsupported_version() {
        let mut ticket = make_test_ticket(b"dev-secret", future_expiry(300));
        ticket[0] = 99;
        assert_eq!(
            verify_ticket(&ticket, b"dev-secret").unwrap_err(),
            AuthError::UnsupportedVersion(99)
        );
    }

    // --------------------------------------------------------------------
    // DoPut ticket variant
    // --------------------------------------------------------------------

    /// Build a signed DoPut ticket with an arbitrary payload — lets tests
    /// drive both the happy path and shape-violation paths.
    fn make_doput_ticket_raw(payload_json: &[u8], secret: &[u8], expiry: u64) -> Vec<u8> {
        let version: u8 = 1;
        let payload_len = (payload_json.len() as u32).to_be_bytes();
        let expiry_bytes = expiry.to_be_bytes();

        let mac_input = [
            &[version][..],
            &payload_len[..],
            payload_json,
            &expiry_bytes[..],
        ]
        .concat();
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(&mac_input);
        let hmac_result = mac.finalize().into_bytes();

        let mut ticket = Vec::new();
        ticket.push(version);
        ticket.extend_from_slice(&payload_len);
        ticket.extend_from_slice(payload_json);
        ticket.extend_from_slice(&hmac_result);
        ticket.extend_from_slice(&expiry_bytes);
        ticket
    }

    fn make_doput_ticket(upload_idx: i64, secret: &[u8], expiry: u64) -> Vec<u8> {
        // Canonical JSON: sorted keys, no whitespace — matches sign_doput.
        let payload = format!(r#"{{"action":"doput","upload_idx":{upload_idx}}}"#);
        make_doput_ticket_raw(payload.as_bytes(), secret, expiry)
    }

    #[test]
    fn verify_doput_round_trip() {
        let ticket = make_doput_ticket(42, b"dev-secret", future_expiry(300));
        let payload = verify_doput(&ticket, b"dev-secret").expect("valid ticket should verify");
        assert_eq!(payload.action, "doput");
        assert_eq!(payload.upload_idx, 42);
    }

    #[test]
    fn verify_doput_rejects_bad_hmac() {
        let mut ticket = make_doput_ticket(7, b"dev-secret", future_expiry(300));
        ticket[10] ^= 0xFF;
        assert_eq!(
            verify_doput(&ticket, b"dev-secret").unwrap_err(),
            AuthError::InvalidHmac
        );
    }

    #[test]
    fn verify_doput_rejects_expired() {
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 100;
        let ticket = make_doput_ticket(1, b"dev-secret", expiry);
        assert_eq!(
            verify_doput(&ticket, b"dev-secret").unwrap_err(),
            AuthError::Expired
        );
    }

    #[test]
    fn verify_doput_rejects_extra_fields() {
        // A future signer that accidentally smuggled a reference_idx onto the
        // ticket would couple the upload domain to that consumer — the
        // deserializer's deny_unknown_fields catches the slip.
        let payload = br#"{"action":"doput","upload_idx":1,"reference_idx":99}"#;
        let ticket = make_doput_ticket_raw(payload, b"dev-secret", future_expiry(300));
        let err = verify_doput(&ticket, b"dev-secret").unwrap_err();
        match err {
            AuthError::MalformedPayload(_) => {} // expected
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    #[test]
    fn verify_doput_passes_action_string_through() {
        // The auth layer is shape-only — `action` is just a string here.
        // It still has to be present and parseable; the gRPC handler is
        // what enforces action == "doput". Locking the parse-only
        // behaviour: a payload with a different action string verifies
        // but carries the verbatim value through.
        let payload = br#"{"action":"register_files","upload_idx":1}"#;
        let ticket = make_doput_ticket_raw(payload, b"dev-secret", future_expiry(300));
        let parsed = verify_doput(&ticket, b"dev-secret").expect("verify should succeed");
        assert_eq!(parsed.action, "register_files");
        assert_eq!(parsed.upload_idx, 1);
    }

    // --------------------------------------------------------------------
    // export_read action token variant
    // --------------------------------------------------------------------

    fn make_export_read_ticket(
        prep_sample_idx: i64,
        dest: &str,
        secret: &[u8],
        expiry: u64,
    ) -> Vec<u8> {
        // Canonical JSON: sorted keys, no whitespace — matches sign_action.
        let payload = format!(
            r#"{{"action":"export_read","dest":"{dest}","prep_sample_idx":{prep_sample_idx}}}"#
        );
        make_doput_ticket_raw(payload.as_bytes(), secret, expiry)
    }

    #[test]
    fn verify_export_read_round_trip() {
        let ticket = make_export_read_ticket(
            26154,
            "/scratch/ticket/804/reads.parquet",
            b"dev-secret",
            future_expiry(300),
        );
        let payload =
            verify_export_read(&ticket, b"dev-secret").expect("valid token should verify");
        assert_eq!(payload.action, "export_read");
        assert_eq!(payload.prep_sample_idx, 26154);
        assert_eq!(payload.dest, "/scratch/ticket/804/reads.parquet");
    }

    #[test]
    fn verify_export_read_rejects_bad_hmac() {
        let mut ticket = make_export_read_ticket(
            1,
            "/scratch/ticket/1/reads.parquet",
            b"dev-secret",
            future_expiry(300),
        );
        ticket[10] ^= 0xFF;
        assert_eq!(
            verify_export_read(&ticket, b"dev-secret").unwrap_err(),
            AuthError::InvalidHmac
        );
    }

    #[test]
    fn verify_export_read_rejects_extra_fields() {
        // deny_unknown_fields: a smuggled field is a contract slip surfaced here.
        let payload =
            br#"{"action":"export_read","dest":"/scratch/x","prep_sample_idx":1,"smuggled":9}"#;
        let ticket = make_doput_ticket_raw(payload, b"dev-secret", future_expiry(300));
        match verify_export_read(&ticket, b"dev-secret").unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    // --------------------------------------------------------------------
    // delete_pool_reads action token variant
    // --------------------------------------------------------------------

    #[test]
    fn verify_delete_pool_reads_round_trip() {
        // Canonical JSON: sorted keys, no whitespace — matches sign_action.
        let payload = br#"{"action":"delete_pool_reads","prep_sample_idxs":[10,11,12]}"#;
        let ticket = make_doput_ticket_raw(payload, b"dev-secret", future_expiry(300));
        let parsed =
            verify_delete_pool_reads(&ticket, b"dev-secret").expect("valid token should verify");
        assert_eq!(parsed.action, "delete_pool_reads");
        assert_eq!(parsed.prep_sample_idxs, vec![10, 11, 12]);
    }

    #[test]
    fn verify_delete_pool_reads_accepts_empty_set() {
        // An empty pool (no prep_samples) signs an empty list; it must verify.
        let payload = br#"{"action":"delete_pool_reads","prep_sample_idxs":[]}"#;
        let ticket = make_doput_ticket_raw(payload, b"dev-secret", future_expiry(300));
        let parsed = verify_delete_pool_reads(&ticket, b"dev-secret").expect("should verify");
        assert!(parsed.prep_sample_idxs.is_empty());
    }

    #[test]
    fn verify_delete_pool_reads_rejects_bad_hmac() {
        let payload = br#"{"action":"delete_pool_reads","prep_sample_idxs":[1]}"#;
        let mut ticket = make_doput_ticket_raw(payload, b"dev-secret", future_expiry(300));
        ticket[10] ^= 0xFF;
        assert_eq!(
            verify_delete_pool_reads(&ticket, b"dev-secret").unwrap_err(),
            AuthError::InvalidHmac
        );
    }

    #[test]
    fn verify_delete_pool_reads_rejects_extra_fields() {
        // deny_unknown_fields: a smuggled field is a contract slip surfaced here.
        let payload =
            br#"{"action":"delete_pool_reads","prep_sample_idxs":[1],"sequenced_pool_idx":9}"#;
        let ticket = make_doput_ticket_raw(payload, b"dev-secret", future_expiry(300));
        match verify_delete_pool_reads(&ticket, b"dev-secret").unwrap_err() {
            AuthError::MalformedPayload(_) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    /// Cross-language interop test: this ticket was signed by the Python
    /// implementation (qiita_control_plane.auth.tickets.sign_ticket).
    ///
    /// To regenerate, run from the repo root:
    ///
    /// ```bash
    /// cd qiita-control-plane && uv run python3 -c "
    /// from qiita_control_plane.auth.tickets import sign_ticket
    /// ticket = sign_ticket(
    ///     table='reference_sequences',
    ///     filter={'feature_idx': [1, 2, 3]},
    ///     secret=b'dev-secret',
    ///     expiry_epoch=4102444800,  # 2100-01-01T00:00:00Z
    /// )
    /// print(', '.join(str(b) for b in ticket))
    /// "
    /// ```
    ///
    /// Parameters:
    /// - secret: b"dev-secret" (raw bytes, NOT base64 — test-only shortcut)
    /// - expiry_epoch: 4102444800 (2100-01-01T00:00:00Z)
    /// - payload: {"filter":{"feature_idx":[1,2,3]},"table":"reference_sequences"}
    ///
    /// NOTE: This test uses verify_ticket_for_test which skips MAX_TICKET_LIFETIME
    /// check since the test vector has a far-future expiry by design.
    #[test]
    fn verify_python_signed_ticket() {
        #[rustfmt::skip]
        const PYTHON_SIGNED_TICKET: &[u8] = &[
            1, 0, 0, 0, 64, 123, 34, 102, 105, 108, 116, 101, 114, 34, 58,
            123, 34, 102, 101, 97, 116, 117, 114, 101, 95, 105, 100, 120, 34,
            58, 91, 49, 44, 50, 44, 51, 93, 125, 44, 34, 116, 97, 98, 108,
            101, 34, 58, 34, 114, 101, 102, 101, 114, 101, 110, 99, 101, 95,
            115, 101, 113, 117, 101, 110, 99, 101, 115, 34, 125, 11, 122, 0,
            121, 96, 221, 245, 9, 167, 96, 146, 32, 71, 203, 67, 72, 241,
            158, 190, 111, 214, 244, 166, 238, 152, 162, 233, 150, 194, 188,
            91, 68, 0, 0, 0, 0, 244, 134, 87, 0,
        ];

        // The cross-language test vector has expiry in 2100, which exceeds
        // MAX_TICKET_LIFETIME. We verify HMAC + structure directly rather than
        // going through verify_ticket which would reject the expiry.
        // This tests the critical interop property: Python's HMAC output
        // matches Rust's HMAC verification byte-for-byte.
        let secret = b"dev-secret";
        let ticket = PYTHON_SIGNED_TICKET;

        // Parse wire format manually
        let payload_len = u32::from_be_bytes([ticket[1], ticket[2], ticket[3], ticket[4]]) as usize;
        let payload_bytes = &ticket[5..5 + payload_len];
        let hmac_start = 5 + payload_len;
        let received_hmac = &ticket[hmac_start..hmac_start + 32];
        let expiry_bytes = &ticket[hmac_start + 32..];

        // Verify HMAC matches
        let mac_input = [&ticket[0..1], &ticket[1..5], payload_bytes, expiry_bytes].concat();
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(&mac_input);
        mac.verify_slice(received_hmac)
            .expect("Python-signed HMAC must verify in Rust");

        // Verify payload deserializes correctly into typed filter
        let payload: TicketPayload =
            serde_json::from_slice(payload_bytes).expect("payload should parse");
        assert_eq!(payload.table, "reference_sequences");
        let feature_idx = payload
            .filter
            .get("feature_idx")
            .expect("filter should have feature_idx");
        assert_eq!(feature_idx.len(), 3);
    }
}
