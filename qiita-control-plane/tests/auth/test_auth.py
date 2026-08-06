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


def test_sign_ticket_signs_a_projection_column_list():
    """A projection list rides the ticket in the caller's order.

    Order is preserved rather than normalized: the data plane emits the columns
    in exactly this sequence, so the consumer's Arrow schema is a function of
    what it asked for, not of how our allowlist happens to be sorted.
    """
    import json

    from qiita_control_plane.auth.tickets import sign_ticket

    columns = ["feature_idx", "cigar", "position"]
    ticket = sign_ticket(
        table="alignment_visible",
        filter={"alignment_idx": [7]},
        columns=columns,
        secret=_TEST_SEED,
    )
    assert json.loads(_payload_of(ticket))["columns"] == columns


def test_sign_ticket_omits_columns_when_absent():
    """A ticket with no projection signs byte-identical bytes to before.

    Same reason as ``members``: the data plane defaults the field, so emitting
    ``"columns": []`` would change the canonical-JSON payload every existing
    ticket signs over, for nothing.
    """
    import json

    from qiita_control_plane.auth.tickets import sign_ticket

    kwargs = {
        "table": "alignment_visible",
        "filter": {"alignment_idx": [7]},
        "secret": _TEST_SEED,
        "expiry_epoch": 1_800_000_000,
    }
    assert "columns" not in json.loads(_payload_of(sign_ticket(**kwargs)))
    assert sign_ticket(**kwargs) == sign_ticket(**kwargs, columns=None)


def test_sign_ticket_rejects_an_empty_projection_list():
    """An explicit empty list is a caller bug, not "no opinion".

    This is the ONLY layer that can tell the two apart: the data plane's
    ``#[serde(default)]`` renders an omitted field as an empty Vec, so by the
    time the ticket is on the wire an empty list is indistinguishable from an
    absent one. Refuse it here, where the distinction still exists — a caller
    that computed a projection and got nothing must not silently receive every
    column instead.
    """
    from qiita_control_plane.auth.tickets import sign_ticket

    with pytest.raises(ValueError, match="empty projection"):
        sign_ticket(
            table="alignment_visible",
            filter={"alignment_idx": [7]},
            columns=[],
            secret=_TEST_SEED,
        )


def test_sign_ticket_rejects_an_unknown_projection_column():
    """An unknown name is refused at mint, so it is never signed.

    The data plane whitelists again on receipt (the names reach interpolated
    SQL), but catching it here is what turns a consumer's typo into a usable
    error instead of an InvalidArgument from a stream that already started.
    """
    from qiita_control_plane.auth.tickets import sign_ticket

    with pytest.raises(ValueError, match="no_such_column"):
        sign_ticket(
            table="alignment_visible",
            filter={"alignment_idx": [7]},
            columns=["feature_idx", "no_such_column"],
            secret=_TEST_SEED,
        )


def test_sign_ticket_rejects_duplicate_projection_columns():
    """A repeated name yields two identically-named Arrow fields, which
    consumers collapse or reject inconsistently. Refuse the ambiguous schema."""
    from qiita_control_plane.auth.tickets import sign_ticket

    with pytest.raises(ValueError, match="duplicate projection"):
        sign_ticket(
            table="alignment_visible",
            filter={"alignment_idx": [7]},
            columns=["feature_idx", "position", "feature_idx"],
            secret=_TEST_SEED,
        )


def test_sign_ticket_rejects_columns_on_a_table_with_no_allowlist():
    """Only the alignment surface takes a projection, by decision.

    Every other table streams every column, so a list signed onto one would be
    a ticket the data plane rejects outright — mint-time is the cheaper place to
    find that out, and the same argument the ``members`` guard above makes.
    """
    from qiita_control_plane.auth.tickets import sign_ticket

    with pytest.raises(ValueError, match="no projection"):
        sign_ticket(
            table="read_masked",
            filter={"mask_idx": [7], "prep_sample_idx": [3]},
            columns=["sequence_idx"],
            secret=_TEST_SEED,
        )


def test_cp_projection_allowlist_matches_the_rust_one_exactly():
    """``_PROJECTION_COLUMNS`` is a hand-copy of the data plane's
    ``ALIGNMENT_PROJECTION_COLUMNS`` (flight_service.rs). The CP validates and
    signs; the DP validates again and projects. Drift fails asymmetrically:

    * a column the DP allows but the CP does not is UNREACHABLE — nobody can get
      it signed, so the feature quietly does not work and no test notices;
    * a column the CP allows but the DP does not mints tickets the DP rejects,
      turning a deploy into a stream of InvalidArgument.

    Rust cannot import the Python and vice versa, so the const is parsed out of
    the source — exactly as ``test_cp_doget_allowlist_matches_the_rust_one_exactly``
    does for ALLOWED_TABLES.
    """
    import re
    from pathlib import Path

    from qiita_control_plane.auth.tickets import _PROJECTION_COLUMNS

    src = (
        Path(__file__).resolve().parents[3] / "qiita-data-plane" / "src" / "flight_service.rs"
    ).read_text()
    m = re.search(
        r"const ALIGNMENT_PROJECTION_COLUMNS: &\[&str\] = &\[(.*?)\];", src, flags=re.DOTALL
    )
    assert m, "ALIGNMENT_PROJECTION_COLUMNS not found in flight_service.rs"
    rust_columns = set(re.findall(r'"([^"]+)"', m.group(1)))
    cp_columns = _PROJECTION_COLUMNS["alignment_visible"]

    assert rust_columns == set(cp_columns), (
        "the CP projection allowlist and the data plane's have drifted; "
        f"only in Rust: {sorted(rust_columns - set(cp_columns))}; "
        f"only in Python: {sorted(set(cp_columns) - rust_columns)}"
    )


def test_cp_projection_allowlist_covers_only_tables_the_dp_projects():
    """The mapping's KEYS must match the data plane's, too — a table the CP
    thinks is projectable but the DP does not would mint tickets rejected on
    arrival. The DP keys off `is_alignment_doget_surface`, so today that is the
    single alignment view; this pins the pair, not just the column list.
    """
    from qiita_control_plane.auth.tickets import _PROJECTION_COLUMNS

    assert set(_PROJECTION_COLUMNS) == {"alignment_visible"}
