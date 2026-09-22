//! Unit tests for [`super`]. Split out of `auth.rs`, which was 1143 lines
//! with 44% of them tests; the module is still a child of
//! `auth` via `#[path]`, so it reaches private items through `use super::*`.

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

const DOGET_PAYLOAD: &[u8] = br#"{"filter":{"feature_idx":[1,2,3]},"table":"reference_sequences"}"#;

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

// -------------------- sync_reference_exclusion --------------------

#[test]
fn verify_sync_reference_exclusion_round_trip() {
    let payload = br#"{"action":"sync_reference_exclusion","dest":"/scratch/exclusion/reference_exclusion.parquet"}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    let parsed =
        verify_sync_reference_exclusion(&ticket, &test_vk()).expect("valid token should verify");
    assert_eq!(parsed.action, "sync_reference_exclusion");
    assert_eq!(
        parsed.dest,
        "/scratch/exclusion/reference_exclusion.parquet"
    );
}

#[test]
fn verify_sync_reference_exclusion_rejects_extra_fields() {
    // A smuggled scoping field (e.g. reference_idx) is a design slip — the
    // blocklist is global and the mirror is a flat feature_idx set.
    let payload = br#"{"action":"sync_reference_exclusion","dest":"/scratch/x","reference_idx":9}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    match verify_sync_reference_exclusion(&ticket, &test_vk()).unwrap_err() {
        AuthError::MalformedPayload(_) => {}
        other => panic!("expected MalformedPayload, got {other:?}"),
    }
}

// -------------------- block-read DoGet ticket members --------------------
//
// Block-scoped reads are a DoGet ticket carrying `members`. A member is
// exactly three fields and a smuggled field is a hard error — guarantees that
// live on `BlockReadMember`, which the DoGet payload embeds.

#[test]
fn verify_ticket_parses_block_read_members() {
    let payload = br#"{"filter":{},"members":[{"prep_sample_idx":101,"sequence_idx_start":100,"sequence_idx_stop":109},{"prep_sample_idx":103,"sequence_idx_start":300,"sequence_idx_stop":309}],"table":"read_block"}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    let parsed = verify_ticket(&ticket, &test_vk()).expect("valid ticket should verify");
    assert_eq!(parsed.table, "read_block");
    assert_eq!(parsed.members.len(), 2);
    assert_eq!(parsed.members[0].prep_sample_idx, 101);
    assert_eq!(parsed.members[1].sequence_idx_stop, 309);
    assert!(
        parsed.filter.is_empty(),
        "read_block scopes by members alone"
    );
}

#[test]
fn verify_ticket_parses_masked_block_members_with_mask_scope() {
    let payload = br#"{"filter":{"mask_idx":[7]},"members":[{"prep_sample_idx":101,"sequence_idx_start":100,"sequence_idx_stop":109}],"table":"read_masked_block"}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    let parsed = verify_ticket(&ticket, &test_vk()).expect("valid ticket should verify");
    assert_eq!(parsed.table, "read_masked_block");
    assert_eq!(parsed.members.len(), 1);
    assert_eq!(parsed.filter.get("mask_idx").unwrap()[0].as_i64(), Some(7));
}

#[test]
fn verify_ticket_rejects_member_extra_fields() {
    // BlockReadMember is deny_unknown_fields: a member is exactly the three
    // columns, so a smuggled field is a malformed ticket, not an ignored one.
    let payload = br#"{"filter":{},"members":[{"prep_sample_idx":1,"sequence_idx_start":1,"sequence_idx_stop":2,"smuggled":9}],"table":"read_block"}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    match verify_ticket(&ticket, &test_vk()).unwrap_err() {
        AuthError::MalformedPayload(_) => {}
        other => panic!("expected MalformedPayload, got {other:?}"),
    }
}

#[test]
fn verify_ticket_rejects_an_unknown_top_level_field() {
    // The rollout direction that would otherwise be silent: a control plane
    // newer than this data plane signs a field this build has never heard of.
    // Ignoring it means proceeding under a WIDER reading of a ticket the signer
    // believed it had narrowed, so the ticket fails instead.
    let payload = br#"{"filter":{"feature_idx":[1]},"row_limit":10,"table":"reference_sequences"}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    match verify_ticket(&ticket, &test_vk()).unwrap_err() {
        AuthError::MalformedPayload(_) => {}
        other => panic!("expected MalformedPayload, got {other:?}"),
    }
}

#[test]
fn verify_ticket_defaults_members_when_absent() {
    // Every pre-existing (non-block) ticket omits `members` entirely; it must
    // still parse, with an empty selector.
    let payload = br#"{"filter":{"feature_idx":[1]},"table":"reference_sequences"}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    let parsed = verify_ticket(&ticket, &test_vk()).expect("valid ticket should verify");
    assert!(parsed.members.is_empty());
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
    let payload = br#"{"action":"mask_metrics","mask_idx":42,"prep_sample_idx":7,"smuggled":9}"#;
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
// delete_alignment / delete_alignment_block / delete_alignment_sample
// action token variants
// --------------------------------------------------------------------

#[test]
fn verify_delete_alignment_round_trip() {
    let payload = br#"{"action":"delete_alignment","alignment_idx":77}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    let parsed = verify_delete_alignment(&ticket, &test_vk()).expect("valid token should verify");
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

#[test]
fn verify_delete_alignment_sample_round_trip() {
    let payload =
        br#"{"action":"delete_alignment_sample","alignment_idx":42,"prep_sample_idx":101}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    let parsed =
        verify_delete_alignment_sample(&ticket, &test_vk()).expect("valid token should verify");
    assert_eq!(parsed.action, "delete_alignment_sample");
    assert_eq!(parsed.alignment_idx, 42);
    assert_eq!(parsed.prep_sample_idx, 101);
}

#[test]
fn verify_delete_alignment_sample_rejects_bad_signature() {
    let payload = br#"{"action":"delete_alignment_sample","alignment_idx":1,"prep_sample_idx":2}"#;
    let mut ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    ticket[14] ^= 0xFF;
    assert_eq!(
        verify_delete_alignment_sample(&ticket, &test_vk()).unwrap_err(),
        AuthError::InvalidSignature
    );
}

#[test]
fn verify_delete_alignment_sample_rejects_extra_fields() {
    // A `members` list: a caller reaching for the block scope.
    let payload = br#"{"action":"delete_alignment_sample","alignment_idx":1,"prep_sample_idx":2,"members":[]}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    match verify_delete_alignment_sample(&ticket, &test_vk()).unwrap_err() {
        AuthError::MalformedPayload(_) => {}
        other => panic!("expected MalformedPayload, got {other:?}"),
    }
}

// -------------------- delete_pool_reads --------------------

#[test]
fn verify_delete_pool_reads_round_trip() {
    let payload = br#"{"action":"delete_pool_reads","prep_sample_idxs":[10,11,12]}"#;
    let ticket = build_ticket(payload, &test_signing_key(), future_expiry(300));
    let parsed = verify_delete_pool_reads(&ticket, &test_vk()).expect("valid token should verify");
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
    let payload = br#"{"action":"delete_pool_reads","prep_sample_idxs":[10,11,12],"smuggled":9}"#;
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
