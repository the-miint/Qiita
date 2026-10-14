"""Recognising a data-plane Flight failure that a retry can clear.

Both services call the data plane over Flight and both have to decide whether a failure
is the data plane's state or the caller's request. The classification cannot be shared as
code with the data plane (Rust), and the client library gives nothing to match on besides
the message, so the one text each side keys on lives here rather than in each service.

The case this module exists for: the data plane refuses a request it has no free DuckLake
connection for (`qiita-data-plane/src/pool.rs`), which is overload — the same call
succeeds once a connection frees. pyarrow surfaces that gRPC RESOURCE_EXHAUSTED as
`ArrowInvalid`, a plain `ValueError`, so a caller that classifies by exception type files
it as bad input and never retries it.

`qiita-control-plane/tests/test_dp_fetch_classifier.py` parses the refusal out of
`pool.rs`, so a reword there fails a test instead of silently making every refusal
permanent.
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
