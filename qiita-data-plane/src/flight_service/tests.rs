//! Unit tests for [`super`]: the `do_action` dispatcher.

use super::*;
use crate::flight_service::test_support::{future_expiry_secs, make_service, sign_raw, TEST_SEED};

// --- do_action dispatch trust checks (pure; no DuckDB) ---

/// An action whose `Action.type` header disagrees with the signed
/// `payload.action` is rejected. `verify_action` succeeds (signature + shape are
/// valid), then the handler's discriminator check catches the mismatch — the
/// two must agree so a token minted for one action can't be replayed under a
/// different action header.
#[tokio::test]
async fn do_action_rejects_type_payload_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    // Validly-signed register_files-shaped payload, but its action field
    // says delete_reference — sent under the register_files header.
    let payload =
        br#"{"action":"delete_reference","staging_dir":"/unused","files":{},"work_ticket_idx":1}"#;
    let body = sign_raw(payload, &TEST_SEED, future_expiry_secs(300));
    let action = Action {
        r#type: "register_files".to_string(),
        body: body.into(),
    };
    // The success type (a boxed Stream) is not Debug, so `expect_err` won't
    // compile — match instead.
    let err = match service.do_action(Request::new(action)).await {
        Ok(_) => panic!("action-type/payload mismatch must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("mismatch"),
        "error should name the mismatch: {}",
        err.message()
    );
}

/// An unrecognized `Action.type` is rejected as invalid rather than silently
/// ignored or dispatched — the dispatcher only ever runs known handlers.
#[tokio::test]
async fn do_action_rejects_unknown_action_type() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());
    let action = Action {
        r#type: "definitely_not_a_real_action".to_string(),
        body: Vec::<u8>::new().into(),
    };
    let err = match service.do_action(Request::new(action)).await {
        Ok(_) => panic!("unknown action type must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// The replay-safe registry and the do_action dispatcher must stay in
/// lockstep. Every `REPLAY_SAFE_ACTIONS` entry reaches a real handler — it
/// then fails verifying the empty token body (`Unauthenticated`), NOT the
/// replay guard (`InvalidArgument`) — and an action outside the registry is
/// rejected by the guard. So a new match arm added without a registry entry
/// is unreachable and surfaces the moment it is exercised, forcing a
/// conscious replay classification (see the `# replay:` note in do_action).
#[tokio::test]
async fn replay_safe_actions_matches_dispatcher() {
    let tmp = tempfile::tempdir().unwrap();
    let service = make_service(tmp.path().to_path_buf());

    for name in REPLAY_SAFE_ACTIONS {
        let action = Action {
            r#type: name.to_string(),
            body: Vec::<u8>::new().into(),
        };
        let err = match service.do_action(Request::new(action)).await {
            Ok(_) => panic!("empty-body action {name:?} must fail"),
            Err(e) => e,
        };
        assert_eq!(
            err.code(),
            tonic::Code::Unauthenticated,
            "classified action {name:?} must be dispatched to a handler \
             (fail on token verification), not rejected as unknown"
        );
    }

    // An action absent from the registry is turned away by the replay guard.
    let bogus = Action {
        r#type: "definitely_not_a_real_action".to_string(),
        body: Vec::<u8>::new().into(),
    };
    let err = match service.do_action(Request::new(bogus)).await {
        Ok(_) => panic!("unclassified action must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
