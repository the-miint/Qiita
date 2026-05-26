"""Flight ticket signing with HMAC-SHA256.

Wire format (all multi-byte integers are big-endian):

    <1B version><4B payload_len><payload_len B payload><32B HMAC-SHA256><8B expiry_epoch>

- version: always 1 for now
- payload: canonical JSON (sorted keys, no whitespace, UTF-8)
- HMAC: computed over (version || payload_len || payload || expiry)
- expiry: Unix epoch seconds (uint64)

The HMAC covers the expiry to prevent an attacker from extending a ticket's lifetime.
The version byte allows future wire format changes without breaking verification.
"""

import hashlib
import hmac
import json
import struct
import time
from typing import Any

TICKET_VERSION = 1
DEFAULT_TTL_SECONDS = 300
HMAC_DIGEST_SIZE = 32  # SHA-256


def _sign_payload(
    payload_dict: dict[str, Any],
    secret: bytes,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    expiry_epoch: int | None = None,
) -> bytes:
    """Sign an arbitrary JSON payload with HMAC-SHA256.

    Returns the complete token as bytes in the wire format described above.
    """
    if ttl_seconds <= 0:
        raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")
    if expiry_epoch is None:
        expiry_epoch = int(time.time()) + ttl_seconds

    payload = json.dumps(payload_dict, sort_keys=True, separators=(",", ":")).encode("utf-8")

    version_byte = struct.pack("B", TICKET_VERSION)
    payload_len = struct.pack(">I", len(payload))
    expiry_bytes = struct.pack(">Q", expiry_epoch)

    mac_input = version_byte + payload_len + payload + expiry_bytes
    mac = hmac.new(secret, mac_input, hashlib.sha256).digest()

    return version_byte + payload_len + payload + mac + expiry_bytes


def sign_ticket(
    *,
    table: str,
    filter: dict[str, Any],
    secret: bytes,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    expiry_epoch: int | None = None,
) -> bytes:
    """Sign a DoGet Flight ticket with HMAC-SHA256."""
    return _sign_payload(
        {"filter": filter, "table": table},
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
    """Sign a DoAction token with HMAC-SHA256."""
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
