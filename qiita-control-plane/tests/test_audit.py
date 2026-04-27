"""Unit tests for the audit-event leak-guard (no DB)."""

import pytest


def test_record_event_rejects_forbidden_key_at_top_level():
    from qiita_control_plane.auth.audit import _check_for_leaks

    with pytest.raises(ValueError, match="forbidden key"):
        _check_for_leaks({"token": "anything"})


def test_record_event_rejects_forbidden_key_nested():
    from qiita_control_plane.auth.audit import _check_for_leaks

    with pytest.raises(ValueError, match="forbidden key"):
        _check_for_leaks({"outer": {"plaintext": "..."}})


def test_record_event_rejects_forbidden_key_case_insensitive():
    from qiita_control_plane.auth.audit import _check_for_leaks

    with pytest.raises(ValueError, match="forbidden key"):
        _check_for_leaks({"BEARER": "..."})


def test_record_event_rejects_qk_prefix_in_string_value():
    from qiita_control_plane.auth.audit import _check_for_leaks

    with pytest.raises(ValueError, match="qk_"):
        _check_for_leaks(
            {"reason": "revoking qk_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
        )


def test_record_event_rejects_jwt_shape_in_string_value():
    from qiita_control_plane.auth.audit import _check_for_leaks

    with pytest.raises(ValueError, match="JWT"):
        _check_for_leaks(
            {"note": "received eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.signaturepart"}
        )


def test_record_event_rejects_in_list_values():
    from qiita_control_plane.auth.audit import _check_for_leaks

    with pytest.raises(ValueError, match="qk_"):
        _check_for_leaks(
            {"items": ["safe", "qk_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"]}
        )


def test_record_event_accepts_benign_detail():
    from qiita_control_plane.auth.audit import _check_for_leaks

    # Should not raise.
    _check_for_leaks(
        {
            "ip": "10.0.0.1",
            "user_agent": "curl/8.0",
            "scopes": ["self:profile", "references:read"],
            "outcome": "updated",
            "from": "old@example.com",
            "to": "new@example.com",
            "attempted_email_sha256": "deadbeef" * 8,
        }
    )


def test_sha256_hex_helper():
    from qiita_control_plane.auth.audit import sha256_hex

    h = sha256_hex("alice@example.com")
    assert len(h) == 64
    assert h == sha256_hex("alice@example.com")  # deterministic
    assert h != sha256_hex("Alice@example.com")  # case-sensitive (we don't lowercase)
