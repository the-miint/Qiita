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

**Rust twin:** the refusal is built in `qiita-data-plane/src/pool.rs`, and the upload bound
in `flight_service/doput.rs` answers with the same prefix; the signature below and both texts must
change together. A control-plane test parses the signature out of the Rust, so a reword fails
a test rather than silently making every refusal permanent.

Which conditions count is `is_data_plane_transient` below. What matters here is how they
arrive: pyarrow raises the refusal as `ArrowInvalid` and the rest as `FlightError`
subclasses, which share no base but `Exception`, so a caller that catches by type needs both
branches.
"""

from __future__ import annotations

# The data plane's own prefix, not the gRPC status name. pyarrow renders a status two
# ways and only one of them spells "resource exhausted" with a space, so keying on the
# status name would miss; and gRPC answers RESOURCE_EXHAUSTED for an oversized message
# too, which no retry clears, so keying on it would over-match.
DATA_PLANE_BUSY_SIGNATURE = "data plane busy"

# Postgres SQLSTATE 40001 signature. The data plane's DuckLake catalog writes run
# under serializable isolation, so a concurrent-attach race surfaces this exact
# text — but stringified through the pyarrow FlightError the DP returns, so there
# is no typed asyncpg exception to isinstance against at this layer — a caller
# matching its own database's errors has the exception class and does not need
# this. Match the message. See FailureKind.DATA_PLANE_TRANSIENT.
DATA_PLANE_SERIALIZATION_SIGNATURE = "could not serialize access due to concurrent update"

# gRPC UNAVAILABLE signatures. A data-plane fetch that can't reach the DP surfaces
# as a pyarrow FlightUnavailableError — the DP momentarily saturated by a fan-out,
# restarting during a deploy, or briefly unreachable. This is transient by
# definition (gRPC's own retriable status): a redrive self-heals once the DP is
# back, so it must NOT be filed as a permanent bad-input. Matched by message for
# the same reason as the serialization signature (no typed exception at this
# layer); any one substring is sufficient.
#
# pyarrow renders a Flight error TWO ways, and the difference decides which
# substrings can fire. A client-side failure gives "Flight returned unavailable
# error, with message: failed to connect to all addresses…" — the first three match
# that, and it is the form every CP->DP failure reproduced so far takes (nothing
# listening, and a server that has gone away). A status the server puts on the wire
# gives "<message>. Detail: Unavailable. gRPC client debug context: …" with the class
# name absent, which none of the first three match.
#
# The fourth signature covers that second form, which the data plane does produce: it
# answers `unavailable` when it cannot open a DuckLake connection — the catalog
# unreachable or at its connection limit (`qiita-data-plane/src/pool.rs`). A status
# gRPC defines as retriable must not classify permanent on a stringification detail.
DATA_PLANE_UNAVAILABLE_SIGNATURES = (
    "flightunavailableerror",
    "flight returned unavailable",
    "failed to connect to all addresses",
    "detail: unavailable",
)

# Expired-token signatures, both required. A Flight token carries a TTL, set
# where the control plane mints it, that the data plane checks when the call
# arrives, so a token that ages out in transit comes back as the data plane's
# `AuthError::Expired` text under a gRPC unauthenticated status. The next attempt
# mints a fresh token, so a redrive self-heals it exactly as the two signatures
# above do.
#
# The expiry text alone is too loose — "ticket" names both a Flight ticket and a
# work_ticket here, and "work ticket expired" contains it. The unauthenticated
# marker is what makes it a Flight auth failure, and it survives both of pyarrow's
# renderings: the message-prefix form spells "unauthenticated error", the
# server-returned form appends "Detail: Unauthenticated". The class name alone is too loose
# in the other direction: every AuthError variant maps to unauthenticated, and
# `invalid signature` / `malformed payload` never self-heal. A test parses the data
# plane's `AuthError` Display impl so a reword there fails rather than silently
# reverting every expiry to permanent.
DATA_PLANE_TICKET_EXPIRED_SIGNATURES = ("unauthenticated", "ticket expired")


def is_data_plane_serialization_conflict(exc: BaseException) -> bool:
    """True if a data-plane Flight failure is a transient, retriable serialization
    conflict (SQLSTATE 40001) rather than a permanent bad-input / DP-down error."""
    return DATA_PLANE_SERIALIZATION_SIGNATURE in str(exc)


def is_data_plane_unavailable(exc: BaseException) -> bool:
    """True if a data-plane Flight failure is a transient gRPC UNAVAILABLE (the DP
    could not be reached), rather than a permanent bad-input error."""
    text = str(exc).lower()
    return any(sig in text for sig in DATA_PLANE_UNAVAILABLE_SIGNATURES)


def is_data_plane_ticket_expired(exc: BaseException) -> bool:
    """True if a data-plane Flight failure is an expired signing token, rather than
    a permanent auth failure (bad signature, malformed payload) or a bad input."""
    text = str(exc).lower()
    return all(sig in text for sig in DATA_PLANE_TICKET_EXPIRED_SIGNATURES)


def is_data_plane_busy(exc: BaseException) -> bool:
    """True if `exc` is the data plane refusing a request for want of a free connection."""
    return DATA_PLANE_BUSY_SIGNATURE in str(exc).lower()


def is_data_plane_transient(exc: BaseException) -> bool:
    """True if a data-plane Flight failure is one a retry or redrive can clear: a
    serialization conflict on a DuckLake catalog write, the data plane unreachable or
    unable to open a connection, the request refused for want of a free connection, or a
    signing token that aged out in transit. Everything else is the caller's problem and
    must stay permanent."""
    return (
        is_data_plane_serialization_conflict(exc)
        or is_data_plane_unavailable(exc)
        or is_data_plane_busy(exc)
        or is_data_plane_ticket_expired(exc)
    )
