"""Flight ticket signing with Ed25519.

Wire format (all multi-byte integers are big-endian):

    <1B version><4B payload_len><payload_len B payload><64B Ed25519 signature><8B expiry_epoch>

- version: 2 (v1 was HMAC-SHA256 with a 32-byte tag; the data plane now verifies
  only v2)
- payload: canonical JSON (sorted keys, no whitespace, UTF-8)
- signature: Ed25519 over (version || payload_len || payload || expiry)
- expiry: Unix epoch seconds (uint64)

The signature covers the expiry to prevent an attacker from extending a ticket's
lifetime. Signing is asymmetric: the control plane holds the private key and
signs; the (publicly reachable) data plane holds only the public key and verifies,
so a data-plane compromise cannot forge tickets. The version byte lets the wire
format change without silently misverifying an older ticket.
"""

import asyncio
import struct
import time
from collections.abc import Callable
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from qiita_common.hashing import canonical_json

from ..block_read import READ_BLOCK_TABLE, READ_MASKED_BLOCK_TABLE

# The only tables a `members` selector may be signed onto — the block-read
# selectors, mirroring the data plane's BLOCK_READ_SOURCES. Sourced from the
# shared constants so the signer and the scope rule cannot drift.
_MEMBERS_TABLES = frozenset({READ_BLOCK_TABLE, READ_MASKED_BLOCK_TABLE})

# Columns each table may have signed into a ticket's projection list, mirroring
# the data plane's per-table allowlist (ALIGNMENT_PROJECTION_COLUMNS in
# flight_service.rs). Neither language can import the other, so the two are kept
# honest by a test that parses the Rust source — see tests/auth/test_auth.py.
#
# A table absent from this mapping takes no projection at all: the data plane
# streams every column and rejects a list outright. Only the alignment surface
# is listed; why it and nothing else is in `docs/architecture/flight.md`.
_PROJECTION_COLUMNS: dict[str, frozenset[str]] = {
    "alignment_visible": frozenset(
        {
            "alignment_idx",
            "prep_sample_idx",
            "sequence_idx",
            "feature_idx",
            "mate_feature_idx",
            "flags",
            "position",
            "stop_position",
            "mapq",
            "cigar",
            "mate_position",
            "template_length",
            "tag_as",
            "tag_xs",
            "tag_ys",
            "tag_xn",
            "tag_xm",
            "tag_xo",
            "tag_xg",
            "tag_nm",
            "tag_yt",
            "tag_md",
            "tag_sa",
        }
    ),
}

TICKET_VERSION = 2
DEFAULT_TTL_SECONDS = 300
SIGNATURE_SIZE = 64  # Ed25519


def _sign_payload(
    payload_dict: dict[str, Any],
    secret: bytes,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    expiry_epoch: int | None = None,
) -> bytes:
    """Sign an arbitrary JSON payload with Ed25519.

    `secret` is the raw 32-byte Ed25519 private seed (the control plane's
    `flight_signing_key`). Returns the complete token as bytes in the wire
    format described above.
    """
    if ttl_seconds <= 0:
        raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")
    if expiry_epoch is None:
        expiry_epoch = int(time.time()) + ttl_seconds

    # Canonical JSON (sorted keys, no whitespace, UTF-8) is the byte-for-byte
    # wire contract the Rust verifier checks the signature over — it verifies
    # these exact bytes, so the serialization must never drift. Sourced from the
    # single qiita_common.hashing.canonical_json rather than re-spelled here.
    payload = canonical_json(payload_dict)

    version_byte = struct.pack("B", TICKET_VERSION)
    payload_len = struct.pack(">I", len(payload))
    expiry_bytes = struct.pack(">Q", expiry_epoch)

    signed_input = version_byte + payload_len + payload + expiry_bytes
    signature = Ed25519PrivateKey.from_private_bytes(secret).sign(signed_input)

    return version_byte + payload_len + payload + signature + expiry_bytes


def token_expiry(token: bytes) -> int:
    """The `expiry_epoch` out of a signed token, per the wire format above.

    Resolved forward from the header rather than back from the end, so a token
    whose `payload_len` disagrees with its actual length raises here instead of
    returning eight plausible bytes. Exists so a caller asserting a TTL does not
    re-spell the layout — the format has one definition, at the top of this module.
    """
    payload_len = struct.unpack(">I", token[1:5])[0]
    expiry_start = 1 + 4 + payload_len + SIGNATURE_SIZE
    expiry = token[expiry_start : expiry_start + 8]
    if len(expiry) != 8:
        raise ValueError(
            f"token is {len(token)} bytes; its payload_len={payload_len} puts the "
            f"8-byte expiry at {expiry_start}, past the end"
        )
    return struct.unpack(">Q", expiry)[0]


async def run_signed_flight_call[T](sign: Callable[[], bytes], call: Callable[[bytes], T]) -> T:
    """Run a blocking data-plane Flight call off the event loop, minting its signed
    token inside the worker.

    `run_in_executor(None, ...)` submits to asyncio's default ThreadPoolExecutor,
    which holds `min(32, process_cpu_count() + 4)` threads, so a fan-out wider than
    that queues — 56 concurrent prep_sample read-mask resolutions left 24 calls
    waiting. Minting in the worker keeps that queue wait out of the token's TTL
    (`DEFAULT_TTL_SECONDS` above), which then spans mint -> the data plane's verify
    and nothing else. The data plane verifies before it does the work, so the call's
    own duration is under no TTL either (see `routes.admin`, which states that
    property where it acts on it by minting at the maximum lifetime).

    Lives here rather than beside a caller because `runner`, `actions` and `routes`
    all mint, and `runner` imports `actions` — a helper in either would invert that.

    Caller precondition: validate whatever `sign` will reject BEFORE entering the
    `try` that wraps this call. `sign_ticket` raises on a planning bug (empty filter
    values, a members selector on the wrong table, a missing projection list), and
    that raise happens on the worker, inside the caller's `except` — where a
    data-plane error handler will label it as one.
    """
    return await asyncio.get_running_loop().run_in_executor(None, lambda: call(sign()))


def sign_ticket(
    *,
    table: str,
    filter: dict[str, Any],
    secret: bytes,
    members: list[dict[str, int]] | None = None,
    columns: list[str] | None = None,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    expiry_epoch: int | None = None,
) -> bytes:
    """Sign a DoGet Flight ticket with Ed25519.

    **A signed ticket is always scoped.** The data plane treats an empty filter
    as ``SELECT * FROM <table>``, so signing an unscoped ticket would authorize a
    full-table dump. This boundary enforces that a ticket carries at least one of
    the two scoping mechanisms, so a future caller cannot mint a dump-everything
    ticket even if it forgets its route-level guard:

    * ``filter`` — the column/value form, non-empty with non-empty value lists.
    * ``members`` — the block-read selector (``read_block`` /
      ``read_masked_block``), a non-empty list of ``{prep_sample_idx,
      sequence_idx_start, sequence_idx_stop}`` triples that a flat column filter
      cannot express. ``read_masked_block`` carries BOTH (its ``mask_idx``
      filter plus members); ``read_block`` carries members alone, so an empty
      ``filter`` is legitimate there and only there.

    ``columns`` is the orthogonal, *projection* scope: it narrows what each
    returned row carries rather than which rows are returned. It is **required
    for, and only accepted for**, the tables in ``_PROJECTION_COLUMNS`` — the
    data plane refuses a projectable table's ticket that omits the list, so
    signing one here would only defer the failure to a client already holding a
    signed ticket. Validating it here rather than at the route is the same choice
    ``members`` makes above — this is the one place every ticket passes through,
    so a future caller cannot mint an unvalidated projection by forgetting a
    route-level guard.

    Both ``members`` and ``columns`` are omitted from the payload entirely when
    absent, so every existing ticket signs byte-identical bytes (the data plane
    defaults each field).
    """
    if filter and any(not value for value in filter.values()):
        raise ValueError("sign_ticket rejects a filter with an empty value list")
    if members and table not in _MEMBERS_TABLES:
        # The members selector is only meaningful for the block-read selectors,
        # and only they can be scoped by it alone. Signing it onto another table
        # would mint a ticket the data plane rejects — or, worse, one whose empty
        # filter passed the scope check below on the strength of a selector that
        # table ignores. Enforce here so the boundary holds what it documents.
        raise ValueError(
            f"sign_ticket: a members selector is only valid for "
            f"{sorted(_MEMBERS_TABLES)}, got {table!r}"
        )
    if members is not None and not members:
        # Distinct from "no members at all": an explicitly empty selector means
        # the caller computed a block footprint and got nothing, which is a
        # planning bug — never a licence to read the whole table.
        raise ValueError("sign_ticket rejects an empty members selector")
    if not filter and not members:
        raise ValueError(
            "sign_ticket requires a scope: a non-empty filter, a non-empty "
            "members selector, or both"
        )
    if columns is None and table in _PROJECTION_COLUMNS:
        # A projectable table REQUIRES its list, because the data plane refuses a
        # ticket for one that arrives without it. Signing anyway would mint a 201
        # ticket that can only ever fail at DoGet — the failure arrives one hop
        # later, at a client holding a signed ticket, instead of here where the
        # caller can see what it got wrong. Both halves of the mirrored rule
        # belong at the same boundary.
        raise ValueError(
            f"sign_ticket: table {table!r} requires a projection column list "
            f"(the data plane rejects a ticket for it without one)"
        )
    if columns is not None:
        allowed = _PROJECTION_COLUMNS.get(table)
        if allowed is None:
            raise ValueError(
                f"sign_ticket: table {table!r} takes no projection column list "
                f"(only {sorted(_PROJECTION_COLUMNS)} are projectable)"
            )
        if not columns:
            # Distinct from "no columns at all", and this is the ONLY layer that
            # can tell them apart: the data plane's `#[serde(default)]` renders
            # an omitted field as an empty list, so on the wire the two are one
            # value. A caller that computed a projection and got nothing has a
            # bug, and must not silently receive every column instead.
            raise ValueError("sign_ticket rejects an empty projection column list")
        unknown = sorted(set(columns) - allowed)
        if unknown:
            raise ValueError(f"sign_ticket: unknown projection column(s) {unknown} for {table!r}")
        if len(set(columns)) != len(columns):
            # Two identically-named Arrow fields, which consumers collapse or
            # reject inconsistently — refuse rather than pick for them.
            raise ValueError(f"sign_ticket: duplicate projection column(s) in {columns}")

    payload: dict[str, Any] = {"filter": filter, "table": table}
    if members:
        payload["members"] = members
    if columns:
        payload["columns"] = columns
    return _sign_payload(
        payload,
        secret,
        ttl_seconds=ttl_seconds,
        expiry_epoch=expiry_epoch,
    )


def sign_action(
    *,
    action: str,
    payload: dict[str, Any],
    secret: bytes,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> bytes:
    """Sign a DoAction token with Ed25519."""
    return _sign_payload(
        {"action": action, **payload},
        secret,
        ttl_seconds=ttl_seconds,
    )


def sign_doput(
    *,
    upload_idx: int,
    secret: bytes,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    expiry_epoch: int | None = None,
) -> bytes:
    """Sign a DoPut Flight ticket for streaming Arrow batches into a staged
    upload.

    Payload shape (the wire contract the Rust verifier will key off):
    `{"action": "doput", "upload_idx": N}` — no other fields. The data
    plane resolves the staging path from `upload_idx` server-side; the
    client never names paths.
    """
    return _sign_payload(
        {"action": "doput", "upload_idx": upload_idx},
        secret,
        ttl_seconds=ttl_seconds,
        expiry_epoch=expiry_epoch,
    )
