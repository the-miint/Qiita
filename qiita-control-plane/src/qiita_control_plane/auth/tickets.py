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

import struct
import time
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from qiita_common.hashing import canonical_json

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


def sign_ticket(
    *,
    table: str,
    filter: dict[str, Any],
    secret: bytes,
    members: list[dict[str, int]] | None = None,
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

    ``members`` is omitted from the payload entirely when absent, so every
    existing ticket signs byte-identical bytes (the data plane defaults the field).
    """
    if filter and any(not value for value in filter.values()):
        raise ValueError("sign_ticket rejects a filter with an empty value list")
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
    payload: dict[str, Any] = {"filter": filter, "table": table}
    if members:
        payload["members"] = members
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
