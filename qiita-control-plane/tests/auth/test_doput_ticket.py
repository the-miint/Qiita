"""Tests for the DoPut Flight ticket variant.

Mirrors the test surface of `test_auth.py::test_sign_ticket_*` so the doput
variant is held to the same wire-format invariants as DoGet.
"""

import json
import struct
import time

import pytest


def test_sign_doput_importable():
    """sign_doput must be importable."""
    from qiita_control_plane.auth.tickets import sign_doput

    assert callable(sign_doput)


def test_sign_doput_returns_bytes():
    """sign_doput returns the same wire-format bytes shape as sign_ticket."""
    from qiita_control_plane.auth.tickets import sign_doput

    ticket = sign_doput(upload_idx=42, secret=b"dev-secret")
    assert isinstance(ticket, bytes)
    assert len(ticket) > 0


def test_sign_doput_payload_carries_action_and_upload_idx():
    """Payload must be `{"action": "doput", "upload_idx": N}` — no other fields.

    The Rust side will deserialize against this exact shape; any extra fields
    here would couple the upload domain to a specific consumer.
    """
    from qiita_control_plane.auth.tickets import sign_doput

    ticket = sign_doput(upload_idx=42, secret=b"dev-secret", expiry_epoch=1_000_000)
    payload_len = struct.unpack(">I", ticket[1:5])[0]
    payload = json.loads(ticket[5 : 5 + payload_len])
    assert payload == {"action": "doput", "upload_idx": 42}


def test_sign_doput_is_deterministic():
    """Same inputs + same expiry → identical bytes."""
    from qiita_control_plane.auth.tickets import sign_doput

    t1 = sign_doput(upload_idx=7, secret=b"dev-secret", expiry_epoch=1_000_000)
    t2 = sign_doput(upload_idx=7, secret=b"dev-secret", expiry_epoch=1_000_000)
    assert t1 == t2


def test_sign_doput_distinct_upload_idx_distinct_ticket():
    """Different upload_idx → different bytes (sanity check the payload reaches the HMAC)."""
    from qiita_control_plane.auth.tickets import sign_doput

    t1 = sign_doput(upload_idx=1, secret=b"dev-secret", expiry_epoch=1_000_000)
    t2 = sign_doput(upload_idx=2, secret=b"dev-secret", expiry_epoch=1_000_000)
    assert t1 != t2


def test_sign_doput_wire_format():
    """Wire format: 1B version, 4B payload_len, payload, 32B HMAC, 8B expiry."""
    from qiita_control_plane.auth.tickets import sign_doput

    ticket = sign_doput(upload_idx=1, secret=b"dev-secret", expiry_epoch=9_999_999_999)
    assert ticket[0] == 1

    payload_len = struct.unpack(">I", ticket[1:5])[0]
    hmac_start = 5 + payload_len
    assert len(ticket[hmac_start : hmac_start + 32]) == 32
    expiry_start = hmac_start + 32
    expiry = struct.unpack(">Q", ticket[expiry_start : expiry_start + 8])[0]
    assert expiry == 9_999_999_999
    assert len(ticket) == 1 + 4 + payload_len + 32 + 8


def test_sign_doput_default_expiry_in_future():
    """Default TTL puts expiry past now()."""
    from qiita_control_plane.auth.tickets import sign_doput

    ticket = sign_doput(upload_idx=1, secret=b"dev-secret")
    payload_len = struct.unpack(">I", ticket[1:5])[0]
    expiry_start = 1 + 4 + payload_len + 32
    expiry = struct.unpack(">Q", ticket[expiry_start : expiry_start + 8])[0]
    assert expiry > time.time()


def test_sign_doput_rejects_nonpositive_ttl():
    """ttl_seconds <= 0 raises ValueError — same rule as sign_ticket."""
    from qiita_control_plane.auth.tickets import sign_doput

    with pytest.raises(ValueError, match="positive"):
        sign_doput(upload_idx=1, secret=b"dev-secret", ttl_seconds=0)
    with pytest.raises(ValueError, match="positive"):
        sign_doput(upload_idx=1, secret=b"dev-secret", ttl_seconds=-1)
