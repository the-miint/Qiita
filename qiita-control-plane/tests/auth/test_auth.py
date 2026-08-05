"""Tests for Flight ticket signing."""

import json
import struct
import time

import pytest

# A valid 32-byte Ed25519 private seed for tests (any 32 bytes is a valid seed).
_TEST_SEED = b"\x01" * 32


def test_sign_ticket_importable():
    """sign_ticket must be importable."""
    from qiita_control_plane.auth.tickets import sign_ticket

    assert callable(sign_ticket)


def test_sign_ticket_returns_bytes():
    """sign_ticket must return bytes."""
    from qiita_control_plane.auth.tickets import sign_ticket

    ticket = sign_ticket(
        table="reference_sequences",
        filter={"feature_idx": [1, 2, 3]},
        secret=_TEST_SEED,
    )
    assert isinstance(ticket, bytes)
    assert len(ticket) > 0


def test_sign_ticket_is_deterministic():
    """Same inputs must produce the same ticket (given same expiry). Ed25519 is
    deterministic per RFC 8032, so a fixed seed + payload + expiry is stable."""
    from qiita_control_plane.auth.tickets import sign_ticket

    t1 = sign_ticket(
        table="reference_sequences",
        filter={"feature_idx": [1]},
        secret=_TEST_SEED,
        expiry_epoch=1000000,
    )
    t2 = sign_ticket(
        table="reference_sequences",
        filter={"feature_idx": [1]},
        secret=_TEST_SEED,
        expiry_epoch=1000000,
    )
    assert t1 == t2


def _payload_of(ticket: bytes) -> bytes:
    """Slice the canonical-JSON payload out of a signed ticket.

    The wire format is `<1B version><4B payload_len><payload><64B sig><8B expiry>`
    (pinned by test_sign_ticket_wire_format below).
    """
    payload_len = struct.unpack(">I", ticket[1:5])[0]
    return ticket[5 : 5 + payload_len]


def test_sign_ticket_wire_format():
    """Ticket wire format: 1B version, 4B payload len, payload, 64B Ed25519 sig, 8B expiry."""
    from qiita_control_plane.auth.tickets import sign_ticket

    ticket = sign_ticket(
        table="test_table",
        filter={"x": [1]},
        secret=_TEST_SEED,
        expiry_epoch=9999999999,
    )

    # Version byte — v2 is the Ed25519 wire format.
    assert ticket[0] == 2

    # Payload length (big-endian uint32)
    payload_len = struct.unpack(">I", ticket[1:5])[0]
    payload_bytes = ticket[5 : 5 + payload_len]

    # Payload is valid JSON with sorted keys
    payload = json.loads(payload_bytes)
    assert payload["table"] == "test_table"
    assert payload["filter"] == {"x": [1]}

    # Signature is 64 bytes (Ed25519)
    sig_start = 5 + payload_len
    sig_bytes = ticket[sig_start : sig_start + 64]
    assert len(sig_bytes) == 64

    # Expiry is big-endian uint64
    expiry_start = sig_start + 64
    expiry = struct.unpack(">Q", ticket[expiry_start : expiry_start + 8])[0]
    assert expiry == 9999999999

    # Total length check
    assert len(ticket) == 1 + 4 + payload_len + 64 + 8


def test_sign_ticket_includes_expiry_in_future():
    """Default expiry must be in the future."""
    from qiita_control_plane.auth.tickets import sign_ticket

    ticket = sign_ticket(
        table="test",
        filter={"x": [1]},
        secret=_TEST_SEED,
    )

    payload_len = struct.unpack(">I", ticket[1:5])[0]
    expiry_start = 1 + 4 + payload_len + 64
    expiry = struct.unpack(">Q", ticket[expiry_start : expiry_start + 8])[0]
    assert expiry > time.time()


def test_sign_ticket_canonical_json():
    """Payload JSON must have sorted keys and no whitespace in raw bytes."""
    from qiita_control_plane.auth.tickets import sign_ticket

    ticket = sign_ticket(
        table="test",
        filter={"z": [1], "a": [2]},
        secret=_TEST_SEED,
        expiry_epoch=1000000,
    )

    payload_len = struct.unpack(">I", ticket[1:5])[0]
    payload_str = ticket[5 : 5 + payload_len].decode("utf-8")

    # Check raw byte ordering: "filter" must appear before "table" in the payload string.
    # This catches regressions in sort_keys=True more reliably than checking parsed dict keys.
    assert payload_str.index('"filter"') < payload_str.index('"table"')

    # No whitespace
    assert " " not in payload_str
    assert "\n" not in payload_str


def test_sign_ticket_rejects_nonpositive_ttl():
    """sign_ticket must reject ttl_seconds <= 0."""
    from qiita_control_plane.auth.tickets import sign_ticket

    with pytest.raises(ValueError, match="positive"):
        sign_ticket(table="test", filter={"x": [1]}, secret=_TEST_SEED, ttl_seconds=0)
    with pytest.raises(ValueError, match="positive"):
        sign_ticket(table="test", filter={"x": [1]}, secret=_TEST_SEED, ttl_seconds=-1)


def test_sign_ticket_rejects_unscoped_ticket():
    """sign_ticket must refuse a ticket carrying NEITHER scoping mechanism.

    An empty filter authorizes ``SELECT * FROM <table>`` on the data plane, so
    the signing boundary rejects it rather than minting a dump-everything ticket.
    A filter with an empty value list is the same hole spelled differently.
    """
    from qiita_control_plane.auth.tickets import sign_ticket

    with pytest.raises(ValueError, match="requires a scope"):
        sign_ticket(table="test", filter={}, secret=_TEST_SEED)
    with pytest.raises(ValueError, match="empty value list"):
        sign_ticket(table="test", filter={"prep_sample_idx": []}, secret=_TEST_SEED)


def test_sign_ticket_members_selector():
    """The block-read selector form scopes a ticket in place of a filter.

    ``read_block`` carries members alone (an empty filter is legitimate there and
    only there); an explicitly EMPTY members list is refused, because it means
    the caller computed a block footprint and got nothing — a planning bug, never
    a licence to read the whole table.
    """
    import json

    from qiita_control_plane.auth.tickets import sign_ticket

    members = [{"prep_sample_idx": 11, "sequence_idx_start": 1, "sequence_idx_stop": 9}]
    ticket = sign_ticket(table="read_block", filter={}, members=members, secret=_TEST_SEED)
    payload = json.loads(_payload_of(ticket))
    assert payload["table"] == "read_block"
    assert payload["members"] == members

    with pytest.raises(ValueError, match="empty members selector"):
        sign_ticket(table="read_block", filter={}, members=[], secret=_TEST_SEED)


def test_sign_ticket_omits_members_when_absent():
    """A ticket with no members must sign byte-identical bytes to before.

    The data plane defaults the field, so emitting ``"members": []`` would change
    the canonical-JSON payload every existing ticket signs over for no reason.
    """
    import json

    from qiita_control_plane.auth.tickets import sign_ticket

    payload = json.loads(
        _payload_of(sign_ticket(table="read_masked", filter={"mask_idx": [7]}, secret=_TEST_SEED))
    )
    assert "members" not in payload
