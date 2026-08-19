"""Tests for `auth.handoff` — cookie sign/verify + OT-code helpers + URL builder.

Pure-function tests; no I/O, no DB access. The route layer (tests/integration)
exercises the helpers in context against a real handoff flow.
"""

import secrets

import pytest
from qiita_common.api_paths import URL_AUTH_HANDOFF

from qiita_control_plane.auth.handoff import (
    LOGIN_COOKIE_NAME,
    CookieInvalid,
    build_authrocket_login_url,
    generate_ot_code,
    hash_ot_code,
    sign_login_cookie,
    verify_login_cookie,
)

# ---------------------------------------------------------------------------
# Cookie sign / verify
# ---------------------------------------------------------------------------


def _secret() -> bytes:
    """A reasonable-strength HMAC key for tests."""
    return secrets.token_bytes(32)


def test_cookie_roundtrip_yields_original_payload():
    secret = _secret()
    payload = {"state": "abc123", "timestamp_ms": 1_700_000_000_000, "cli": True, "port": 9999}
    cookie = sign_login_cookie(payload, secret)
    assert "." in cookie  # body.sig shape

    out = verify_login_cookie(cookie, secret, max_age_seconds=300, now_ms=1_700_000_000_500)
    assert out == payload


def test_cookie_rejects_tampered_payload():
    secret = _secret()
    cookie = sign_login_cookie({"state": "abc", "timestamp_ms": 1_700_000_000_000}, secret)
    # Flip a character in the body half. Even a single-char change must
    # break the HMAC; this is the headline tamper defense.
    body, sig = cookie.split(".")
    tampered = body[:-1] + ("A" if body[-1] != "A" else "B") + "." + sig

    with pytest.raises(CookieInvalid):
        verify_login_cookie(tampered, secret, max_age_seconds=300)


def test_cookie_rejects_wrong_secret():
    cookie = sign_login_cookie({"state": "abc", "timestamp_ms": 1_700_000_000_000}, _secret())
    with pytest.raises(CookieInvalid):
        verify_login_cookie(cookie, _secret(), max_age_seconds=300)


def test_cookie_rejects_expired():
    secret = _secret()
    cookie = sign_login_cookie({"state": "abc", "timestamp_ms": 1_700_000_000_000}, secret)
    # 5 minutes + 1 ms past the timestamp → outside a 300s window.
    too_late = 1_700_000_000_000 + 300_001

    with pytest.raises(CookieInvalid, match="stale"):
        verify_login_cookie(cookie, secret, max_age_seconds=300, now_ms=too_late)


def test_cookie_rejects_future_dated():
    """Negative ages are treated as invalid — either clock skew or a forged
    cookie. Without a symmetric guard, an attacker who could write cookies
    with a future timestamp could indefinitely extend the freshness window."""
    secret = _secret()
    cookie = sign_login_cookie({"state": "abc", "timestamp_ms": 2_000_000_000_000}, secret)

    with pytest.raises(CookieInvalid, match="future"):
        verify_login_cookie(cookie, secret, max_age_seconds=300, now_ms=1_000_000_000_000)


def test_cookie_rejects_malformed():
    secret = _secret()
    with pytest.raises(CookieInvalid):
        verify_login_cookie("not-a-cookie", secret, max_age_seconds=300)
    with pytest.raises(CookieInvalid):
        verify_login_cookie("a.b.c", secret, max_age_seconds=300)
    with pytest.raises(CookieInvalid):
        verify_login_cookie("a.!!", secret, max_age_seconds=300)


def test_sign_rejects_empty_secret():
    """An empty login-cookie secret must never sign. `hmac.new(b"", ...)` would
    otherwise sign happily under an empty key, defeating the point of a
    dedicated cookie key whose leak must not forge cookies. The guard lives in
    the helper so it holds regardless of how Settings was constructed."""
    with pytest.raises(ValueError, match="empty"):
        sign_login_cookie({"state": "abc", "timestamp_ms": 1_700_000_000_000}, b"")


def test_verify_rejects_empty_secret():
    """Symmetric with signing: an empty secret must never validate a
    signature."""
    cookie = sign_login_cookie({"state": "abc", "timestamp_ms": 1_700_000_000_000}, _secret())
    with pytest.raises(ValueError, match="empty"):
        verify_login_cookie(cookie, b"", max_age_seconds=300)


def test_cookie_rejects_payload_without_timestamp():
    secret = _secret()
    # Use sign helper to produce a valid signature over a payload that
    # nonetheless lacks `timestamp_ms`. Verify should reject after sig
    # passes (so we know it isn't accepting because of sig leniency).
    cookie = sign_login_cookie({"state": "abc"}, secret)
    with pytest.raises(CookieInvalid, match="timestamp_ms"):
        verify_login_cookie(cookie, secret, max_age_seconds=300)


def test_cookie_rejects_non_dict_payload():
    secret = _secret()
    # Need to construct a signed cookie whose payload is a JSON list, not
    # a dict. We do this by hand because sign_login_cookie's signature
    # requires a dict.
    import base64
    import hashlib
    import hmac
    import json

    body = json.dumps([1, 2, 3]).encode()
    body_b64 = base64.urlsafe_b64encode(body).rstrip(b"=").decode()
    sig = hmac.new(secret, body_b64.encode(), hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=").decode()

    with pytest.raises(CookieInvalid, match="object"):
        verify_login_cookie(f"{body_b64}.{sig_b64}", secret, max_age_seconds=300)


def test_cookie_name_is_stable():
    """LOGIN_COOKIE_NAME is part of qiita's public client contract — the
    route layer sets it, the verifier reads it, and operators may filter
    it in nginx logs. Renaming it is a breaking change."""
    assert LOGIN_COOKIE_NAME == "qiita_login_state"


# ---------------------------------------------------------------------------
# One-time codes
# ---------------------------------------------------------------------------


def test_generate_ot_code_returns_plaintext_and_matching_hash():
    plaintext, hash_ = generate_ot_code()
    assert isinstance(plaintext, str)
    assert isinstance(hash_, bytes)
    assert len(hash_) == 32  # SHA-256 is always 32 bytes
    assert hash_ot_code(plaintext) == hash_


def test_generate_ot_code_is_url_safe():
    """The plaintext travels through the browser URL bar to the CLI's
    loopback. Anything that needs URL-encoding would land mangled."""
    import string

    url_safe = set(string.ascii_letters + string.digits + "-_")
    plaintext, _ = generate_ot_code()
    assert all(c in url_safe for c in plaintext)


def test_generate_ot_code_yields_distinct_values():
    """Two consecutive mints must not collide. With 32 bytes of entropy
    this is astronomically unlikely; the test is a tripwire for a future
    refactor that accidentally seeds a deterministic generator."""
    a, _ = generate_ot_code()
    b, _ = generate_ot_code()
    assert a != b


def test_hash_ot_code_matches_sha256_definition():
    """Hash must equal SHA-256 of the plaintext bytes — not of any
    derived form. Anything else would fail to match what the route stores."""
    import hashlib

    plaintext = "abc-123_xyz"
    assert hash_ot_code(plaintext) == hashlib.sha256(plaintext.encode()).digest()


# ---------------------------------------------------------------------------
# AuthRocket login URL builder
# ---------------------------------------------------------------------------


def test_build_authrocket_login_url_appends_prompt_login():
    url = build_authrocket_login_url(
        loginrocket_base_url="https://realm.e2.loginrocket.com",
        redirect_uri=f"https://qiita.example.com{URL_AUTH_HANDOFF}",
    )
    assert url.startswith("https://realm.e2.loginrocket.com/login?")
    assert "prompt=login" in url
    # redirect_uri must be URL-encoded — colon and slash both get %-escaped.
    assert "redirect_uri=https%3A%2F%2Fqiita.example.com%2Fapi%2Fv1%2Fauth%2Fhandoff" in url


def test_build_authrocket_login_url_strips_trailing_slash_on_base():
    """Operators may set AUTHROCKET_LOGINROCKET_URL with or without a
    trailing slash; the result must look identical either way."""
    url1 = build_authrocket_login_url(
        loginrocket_base_url="https://realm.example.com",
        redirect_uri="https://x/handoff",
    )
    url2 = build_authrocket_login_url(
        loginrocket_base_url="https://realm.example.com/",
        redirect_uri="https://x/handoff",
    )
    assert url1 == url2


def test_build_authrocket_login_url_encodes_redirect_with_query_params():
    """A redirect_uri that itself has query params must be encoded so
    AuthRocket sees the full URL as one parameter."""
    url = build_authrocket_login_url(
        loginrocket_base_url="https://r.com",
        redirect_uri="https://qiita.example.com/handoff?env=dev",
    )
    # `?env=dev` must be encoded into the redirect_uri value, not appended
    # as a separate query param to the AuthRocket URL.
    assert "env%3Ddev" in url
    assert url.count("?") == 1  # only AuthRocket's `?` — the inner one is encoded
