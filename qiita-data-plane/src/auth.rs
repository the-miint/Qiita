//! Flight ticket HMAC-SHA256 verification.
//!
//! Used by the Arrow Flight service (Phase 8) to verify signed tickets.
//! Currently only exercised by tests; will be wired into do_get/do_action.
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

/// Verify a Flight ticket and return the parsed payload.
///
/// Checks (in order): version, payload length, HMAC signature, expiry, max
/// lifetime, and JSON structure. HMAC verification is constant-time. The
/// ordering ensures timing information only leaks for structural issues (not
/// for HMAC or payload content).
pub fn verify_ticket(ticket: &[u8], secret: &[u8]) -> Result<TicketPayload, AuthError> {
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

    // Parse payload JSON
    let payload: TicketPayload = serde_json::from_slice(payload_bytes)
        .map_err(|e| AuthError::MalformedPayload(e.to_string()))?;

    Ok(payload)
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
