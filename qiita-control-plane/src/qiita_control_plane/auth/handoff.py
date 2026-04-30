"""Helpers for the AuthRocket LoginRocket Web → /auth/handoff → CLI flow.

The flow has two pieces of crypto:

  1. **Login cookie** — set by GET /auth/login, read by GET /auth/handoff.
     Carries `{state, timestamp_ms, cli, port}` signed with HMAC_SECRET_KEY.
     Lives ≤ AUTH_HANDOFF_FRESHNESS_SECONDS to bound how long a user has to
     complete the AuthRocket round-trip (longer windows expand replay risk).
     The cookie is qiita's freshness anchor — AuthRocket re-emits the same
     JWT across cached sessions so the JWT's iat/auth_time can't carry
     freshness, and we instead set our own timestamp before the round-trip.

  2. **One-time code** — minted by /auth/handoff in the CLI branch, redeemed
     once by POST /auth/cli-exchange. The plaintext code travels through
     the browser's URL bar to the CLI's loopback server; the SHA-256 hash
     is what's stored in qiita.cli_login_codes (matching the api_tokens
     hash-on-disk convention). Plaintext PAT lives in the same row briefly
     between handoff and exchange.

Pure functions; no I/O, no DB access. Easy to unit-test in isolation. Routes
import these helpers and combine them with asyncpg + FastAPI plumbing.
"""

import base64
import hashlib
import hmac
import json
import secrets
import time
from urllib.parse import quote

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


# Cookie set on /auth/login, read + scrubbed on /auth/handoff. SameSite=Lax
# is correct here — the AuthRocket → /auth/handoff redirect is cross-origin
# and Strict would block the cookie from accompanying it.
LOGIN_COOKIE_NAME = "qiita_login_state"

# A short-lived signed value that proves "qiita just sent this user out the
# door for AuthRocket login." 5 min Max-Age leaves slack for slow login
# UX without expanding the replay window beyond what's reasonable.
LOGIN_COOKIE_MAX_AGE_SECONDS = 300


# ---------------------------------------------------------------------------
# Login cookie sign/verify
# ---------------------------------------------------------------------------


class CookieInvalid(Exception):
    """Raised when a cookie fails verification (tampered, expired, malformed)."""


def sign_login_cookie(payload: dict, secret: bytes) -> str:
    """Encode and sign a payload dict for the login cookie.

    Returns `<base64url(json)>.<base64url(hmac_sha256)>`. The payload should
    contain at least `{"state", "timestamp_ms"}`; routes also include
    `{"cli", "port"}` for the CLI flow.
    """
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    body_b64 = _b64u_encode(body)
    sig = hmac.new(secret, body_b64.encode(), hashlib.sha256).digest()
    sig_b64 = _b64u_encode(sig)
    return f"{body_b64}.{sig_b64}"


def verify_login_cookie(
    cookie: str,
    secret: bytes,
    *,
    max_age_seconds: int,
    now_ms: int | None = None,
) -> dict:
    """Verify a signed cookie and return its payload dict.

    Raises `CookieInvalid` on any failure: malformed, bad signature,
    payload not JSON object, missing `timestamp_ms`, or stale beyond
    `max_age_seconds`.

    `now_ms` is injectable for tests; production passes None.
    """
    parts = cookie.split(".")
    if len(parts) != 2:
        raise CookieInvalid("malformed cookie")
    body_b64, sig_b64 = parts

    expected_sig = hmac.new(secret, body_b64.encode(), hashlib.sha256).digest()
    try:
        provided_sig = _b64u_decode(sig_b64)
    except Exception as exc:
        raise CookieInvalid("malformed signature") from exc
    if not hmac.compare_digest(expected_sig, provided_sig):
        raise CookieInvalid("bad signature")

    try:
        body = json.loads(_b64u_decode(body_b64))
    except Exception as exc:
        raise CookieInvalid("malformed payload") from exc
    if not isinstance(body, dict):
        raise CookieInvalid("payload not an object")

    timestamp_ms = body.get("timestamp_ms")
    if not isinstance(timestamp_ms, int):
        raise CookieInvalid("timestamp_ms missing or wrong type")

    current_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    age_ms = current_ms - timestamp_ms
    # Negative ages indicate clock skew or a forged cookie; treat as invalid.
    if age_ms < 0 or age_ms > max_age_seconds * 1000:
        raise CookieInvalid("cookie stale or future-dated")

    return body


# ---------------------------------------------------------------------------
# One-time codes for CLI handoff
# ---------------------------------------------------------------------------


# 32 bytes of entropy → 43 url-safe base64 chars. Same shape as the api_tokens
# body; deliberate so monitoring/leak-scanners can apply similar rules.
_OT_CODE_BYTES = 32


def generate_ot_code() -> tuple[str, bytes]:
    """Mint a fresh one-time code.

    Returns `(plaintext, hash)` — plaintext is the url-safe base64 string
    that goes through the browser URL bar to the CLI's loopback; hash is
    the 32-byte SHA-256 stored in qiita.cli_login_codes.ot_code.
    """
    plaintext = secrets.token_urlsafe(_OT_CODE_BYTES)
    return plaintext, hash_ot_code(plaintext)


def hash_ot_code(plaintext: str) -> bytes:
    """SHA-256 of a plaintext ot_code. Used for storage and lookup."""
    return hashlib.sha256(plaintext.encode()).digest()


# ---------------------------------------------------------------------------
# AuthRocket login URL builder
# ---------------------------------------------------------------------------


def build_authrocket_login_url(
    *,
    loginrocket_base_url: str,
    redirect_uri: str,
) -> str:
    """Build the AuthRocket LoginRocket Web login URL.

    Appends `&prompt=login` to force interactive re-auth even when
    AuthRocket has a cached browser session. We don't depend on this for
    token freshness (AuthRocket re-emits the same JWT regardless), but it
    blocks "logged-in browser walked away → attacker pivots" by requiring
    a fresh password entry before issuing a new login.

    `redirect_uri` is URL-encoded into the query string. The caller
    is responsible for ensuring it's an https:// URL pointing at qiita's
    /auth/handoff route.
    """
    base = loginrocket_base_url.rstrip("/")
    encoded_redirect = quote(redirect_uri, safe="")
    return f"{base}/login?redirect_uri={encoded_redirect}&prompt=login"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _b64u_encode(data: bytes) -> str:
    """base64url-without-padding encoding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64u_decode(data: str) -> bytes:
    """base64url-without-padding decoding (with pad-on-the-fly)."""
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded)
