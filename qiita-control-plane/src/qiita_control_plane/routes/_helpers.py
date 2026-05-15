"""Cross-route helpers shared by sibling route modules.

Currently hosts etag_for_updated_at, the RFC-7232 quoted-ISO-8601 ETag
formatter that any route which sets an ETag header or honours an
If-Match header should call. Lifting it here keeps every PATCH-bearing
route on one formatter so callers can byte-compare ETag values across
endpoints.
"""

from datetime import datetime


def etag_for_updated_at(updated_at: datetime) -> str:
    """Build the quoted ETag header value from a row's updated_at timestamp.

    The surrounding double-quotes are required by RFC 7232's entity-tag
    grammar — the on-the-wire value is `"<iso8601>"`, not `<iso8601>`.
    The inner ISO 8601 timestamp is opaque to clients; only its
    byte-for-byte equality with a subsequent If-Match header matters.
    """
    return f'"{updated_at.isoformat()}"'
