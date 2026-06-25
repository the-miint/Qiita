"""Canonical-JSON hashing for content-addressed identifier dedup.

The control plane deduplicates certain identifiers on a stable hash of their
defining config (e.g. mask_definition.params_hash dedups a read mask on its
filter workflow + version + references + QC params). The hash must be
deterministic across processes and re-runs, so the JSON is serialized
canonically — sorted keys, no insignificant whitespace, UTF-8 — exactly the
form ``auth.tickets`` uses for signed payloads.

This mirrors the documented ``processing_idx`` dedup discipline
(``SHA-256(canonical JSON parameters)``); when the full processing hierarchy is
built, both can share this helper.
"""

import hashlib
import json
from typing import Any


def canonical_json(value: Any) -> bytes:
    """Serialize ``value`` to canonical JSON bytes (sorted keys, no whitespace).

    Deterministic for any JSON-serializable input — the same logical config
    always produces the same bytes regardless of dict insertion order.
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def canonical_params_hash(params: Any) -> bytes:
    """Return the SHA-256 digest (32 raw bytes) of ``params``' canonical JSON.

    Used as the dedup key for content-addressed identifiers. Returns the raw
    32-byte digest, not a hex string, so it maps directly onto a Postgres
    ``BYTEA`` column.
    """
    return hashlib.sha256(canonical_json(params)).digest()
