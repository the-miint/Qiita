"""Recognising a data-plane Flight failure that a retry can clear.

Both services call the data plane over Flight and both have to decide whether a failure is
the data plane's state or the caller's request. The classification cannot be shared as code
with the data plane (Rust), and the client library gives nothing to match on besides the
message, so the one text each side keys on lives here rather than in each service.

The case this module exists for: the data plane refuses a request it has no free DuckLake
connection for, which is overload — the same call succeeds once a connection frees. pyarrow
surfaces that gRPC RESOURCE_EXHAUSTED as `ArrowInvalid`, a plain `ValueError` (measured on
both 23.0.1 and 25.0.0), so a caller that classifies by exception type files it as bad input
and never retries it.

**Rust twin:** the refusal is built in `qiita-data-plane/src/pool.rs`; the signature below and
that text must change together. A control-plane test parses the one out of the other, so a
reword fails a test rather than silently making every refusal permanent.

Only the busy refusal is here. The data plane's other transient failures — gRPC UNAVAILABLE,
and a DuckLake catalog write that lost a serialization race — are classified control-plane
side only (`qiita_control_plane.runner._upload`), and pyarrow raises those as `FlightError`
subclasses, which are not `ValueError`s.
"""

from __future__ import annotations

# The data plane's own prefix, not the gRPC status name. pyarrow renders a status two
# ways and only one of them spells "resource exhausted" with a space, so keying on the
# status name would miss; and gRPC answers RESOURCE_EXHAUSTED for an oversized message
# too, which no retry clears, so keying on it would over-match.
DATA_PLANE_BUSY_SIGNATURE = "data plane busy"


def is_data_plane_busy(exc: BaseException) -> bool:
    """True if `exc` is the data plane refusing a request for want of a free connection."""
    return DATA_PLANE_BUSY_SIGNATURE in str(exc).lower()
