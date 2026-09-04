"""Unit tests for the data-plane fetch failure classifier (`runner/_upload.py`).

Pins that a DuckLake serialization conflict (Postgres SQLSTATE 40001) surfaced
over Flight is classified RETRIABLE (`DATA_PLANE_TRANSIENT`), as is an expired
signing token, while every other DP-fetch failure stays permanent (`BAD_INPUT`) —
including the other unauthenticated causes, which share the expiry's pyarrow error
class and gRPC status but not its self-healing. The classifier matches the
*stringified* pyarrow `FlightError` by message — there is no typed asyncpg
exception to `isinstance` against at this layer — so a representative real string
is captured here. If the data plane ever reformats that error, THIS test is what
fails (rather than every DP fetch silently reverting to permanent in production).
"""

from __future__ import annotations

from qiita_common.backend_failure import FailureKind
from qiita_common.models import WorkTicketFailureStage

from qiita_control_plane.runner._upload import (
    _DP_TICKET_EXPIRED_SIGNATURES,
    _is_dp_serialization_conflict,
    _is_dp_ticket_expired,
    _is_dp_unavailable,
    _submission_dp_fetch_failure,
)

# A representative stringified pyarrow FlightError carrying the Postgres 40001
# serialization message, as seen verbatim in a real read-mask fan-out
# failure_reason (the concurrent DuckLake-attach race this classifier exists for).
_REAL_40001 = (
    "FlightInternalError: Flight returned internal error, with message: data plane "
    "stream error: External error: failed to attach DuckLake: Invalid Error: Failed "
    'to insert config option in DuckLake: Failed to execute query "UPDATE '
    '\\"public\\".\\"ducklake_metadata\\" SET ...": ERROR:  could not serialize '
    "access due to concurrent update\n"
)


# A representative stringified pyarrow FlightUnavailableError — the DP briefly
# unreachable (saturated by a read-mask fan-out, restarting during a deploy),
# captured verbatim from a real read-mask failure_reason. gRPC UNAVAILABLE is
# transient by definition; a redrive self-heals once the DP is back.
_REAL_UNAVAILABLE = (
    "could not materialize reads for prep_sample 30451 from the data plane: "
    "FlightUnavailableError: Flight returned unavailable error, with message: failed "
    "to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:50051: "
    "connection attempt timed out before receiving SETTINGS frame"
)


# A representative stringified pyarrow FlightUnauthenticatedError carrying the
# data plane's `AuthError::Expired` text, captured verbatim from a read-mask
# failure_reason. The signing token aged out before the call reached the DP; the
# next attempt mints a fresh one, so a redrive self-heals it.
# The message body of the string in the incident report, with the CP's own
# "FlightUnauthenticatedError: " prefix dropped: this is `str(exc)` as the classifier
# receives it (`_upload.py` passes the exception, not the composed failure_reason).
# With the prefix, the `unauthenticated` half of the match would be satisfied by the
# CP's own text and the fixture would stop testing pyarrow's rendering at all.
_REAL_EXPIRED = "Flight returned unauthenticated error, with message: ticket expired"

# The same gRPC status and the same pyarrow class, from `AuthError::InvalidSignature`
# — a key mismatch between the CP and the DP, which no redrive fixes. This is why
# the classifier keys off the expiry text and not the error class.
_REAL_BAD_SIGNATURE = "Flight returned unauthenticated error, with message: invalid signature"


# ---------------------------------------------------------------------------
# pyarrow's SECOND rendering.
#
# The fixtures above are the message-prefix form ("Flight returned <status> error,
# with message: …"). pyarrow also renders a status the server actually returned as
# "<message>. Detail: <Status>. gRPC client debug context: …", with the error CLASS
# NAME absent. Which form you get is not the error's kind — it is where the failure
# happened: a client-side connect failure takes the first, a status the data plane
# put on the wire takes the second.
#
# Captured from a pyarrow 23.0.1 FlightServerBase (the uv.lock version) raising each
# status, with controls: the same message under a different status renders a
# different Detail, and the same status with a different message keeps the Detail —
# so each half of the expiry match is shown to discriminate.
#
# The expiry fixture is what a live data plane produces. The UNAVAILABLE one is NOT
# known to occur on the CP->DP path — the data plane emits no `Status::unavailable`
# itself — it pins the rendering so the classifier is right if anything ever does.
_SERVER_RETURNED_EXPIRED = (
    "ticket expired. Detail: Unauthenticated. gRPC client debug context: "
    "UNKNOWN:Error received from peer ipv4:127.0.0.1:50112 "
    '{created_time:"2026-09-03T21:17:51.828988-06:00", grpc_status:16, '
    'grpc_message:"ticket expired. Detail: Unauthenticated"}. Client context: '
    "IOError: Server never sent a data message. Detail: Internal"
)

# A server-returned UNAVAILABLE: no connect failure happened, so none of the
# connect-shaped substrings appear. See the block comment above for why this is
# pinned even though nothing on the CP->DP path is known to produce it.
_SERVER_RETURNED_UNAVAILABLE = (
    "data plane is shutting down. Detail: Unavailable. gRPC client debug context: "
    "UNKNOWN:Error received from peer ipv4:127.0.0.1:50112 "
    '{created_time:"2026-09-03T21:17:51.830074-06:00", grpc_status:14, '
    'grpc_message:"data plane is shutting down. Detail: Unavailable"}'
)


def test_expiry_is_detected_in_both_pyarrow_renderings():
    """The classifier must fire on the form a live data plane produces, not only on
    the message-prefix form the other fixtures use."""
    assert _is_dp_ticket_expired(Exception(_REAL_EXPIRED)) is True
    assert _is_dp_ticket_expired(Exception(_SERVER_RETURNED_EXPIRED)) is True


def test_a_data_plane_that_is_up_but_unavailable_is_retriable():
    """A server-returned UNAVAILABLE carries no connect failure, so the three
    connect-shaped substrings never appear and it classified PERMANENT until
    "detail: unavailable" was added. gRPC defines the status as retriable, so a
    redrive self-heals it and it must not land as a bad-input an operator resolves —
    whether or not this path has yet produced one."""
    assert _is_dp_unavailable(Exception(_SERVER_RETURNED_UNAVAILABLE)) is True
    f = _submission_dp_fetch_failure("could not fetch ...", Exception(_SERVER_RETURNED_UNAVAILABLE))
    assert f.kind is FailureKind.DATA_PLANE_TRANSIENT
    assert f.transient is True


def test_the_two_renderings_do_not_cross_contaminate():
    """The controls that make the above discriminating: a different status under the
    same message must not read as an expiry, and neither rendering of UNAVAILABLE
    may read as one."""
    # Synthesized from the captured fixture, not captured: the grpc_status:16 it
    # leaves behind makes it a string pyarrow would not emit. That is fine here —
    # the subject is the substring match, not the rendering.
    same_message_other_status = _SERVER_RETURNED_EXPIRED.replace(
        "Detail: Unauthenticated", "Detail: Internal"
    )
    assert _is_dp_ticket_expired(Exception(same_message_other_status)) is False
    assert _is_dp_ticket_expired(Exception(_SERVER_RETURNED_UNAVAILABLE)) is False
    assert _is_dp_unavailable(Exception(_SERVER_RETURNED_EXPIRED)) is False


def test_serialization_conflict_is_detected():
    assert _is_dp_serialization_conflict(Exception(_REAL_40001)) is True


def test_non_serialization_error_is_not_detected():
    assert _is_dp_serialization_conflict(Exception("some other flight error")) is False
    assert _is_dp_serialization_conflict(FileNotFoundError("missing file")) is False


def test_unavailable_is_detected():
    assert _is_dp_unavailable(Exception(_REAL_UNAVAILABLE)) is True


def test_non_unavailable_error_is_not_detected():
    # A serialization conflict is retriable, but NOT via the unavailable path.
    assert _is_dp_unavailable(Exception(_REAL_40001)) is False
    assert _is_dp_unavailable(Exception("some other flight error")) is False


def test_ticket_expired_is_detected():
    assert _is_dp_ticket_expired(Exception(_REAL_EXPIRED)) is True


def test_other_unauthenticated_errors_are_not_detected():
    # Same class, same gRPC status, permanent cause — must not be swept in.
    assert _is_dp_ticket_expired(Exception(_REAL_BAD_SIGNATURE)) is False
    assert _is_dp_ticket_expired(Exception(_REAL_40001)) is False
    assert _is_dp_ticket_expired(Exception("some other flight error")) is False
    # `AuthError::ExpiryTooFar` — the nearest miss, and the variant a reword of the
    # Rust `Display` impl is most likely to collide with.
    assert (
        _is_dp_ticket_expired(
            Exception(
                "Flight returned unauthenticated error, with message: "
                "ticket expiry too far in the future"
            )
        )
        is False
    )


def test_an_expired_work_ticket_is_not_an_expired_flight_ticket():
    """ "ticket" names both a Flight ticket and a work_ticket in this repo, and
    "work ticket expired" contains the expiry text. The unauthenticated marker is
    what separates them, so a non-Flight message must not classify retriable."""
    assert _is_dp_ticket_expired(Exception("work ticket expired while queued")) is False


def test_expiry_signatures_match_the_data_planes_wording():
    """The classifier keys off text the Rust `Display` impl produces. Neither
    language can import the other, so the const is parsed out of the source —
    the same mechanism `tests/auth/test_auth.py` uses for the projection allowlist.
    A reword in `auth.rs` fails HERE rather than silently reverting every expired
    token to a permanent BAD_INPUT."""
    import re
    from pathlib import Path

    src = (Path(__file__).resolve().parents[2] / "qiita-data-plane" / "src" / "auth.rs").read_text()
    m = re.search(r'AuthError::Expired => write!\(f, "([^"]+)"\)', src)
    assert m, "AuthError::Expired arm not found in auth.rs"
    assert m.group(1) == "ticket expired", (
        f"the data plane now renders an expired ticket as {m.group(1)!r}; "
        f"update _DP_TICKET_EXPIRED_SIGNATURES ({_DP_TICKET_EXPIRED_SIGNATURES!r}) "
        "or expired tokens silently classify permanent again"
    )
    # The other half of the match: every ticket verification maps its AuthError to
    # gRPC unauthenticated, which is what puts that word in the client's error. A
    # verify site mapped to any other Status would break the match, so read them
    # out of the source rather than asserting the constant against itself.
    service = (
        Path(__file__).resolve().parents[2] / "qiita-data-plane" / "src" / "flight_service.rs"
    ).read_text()
    verify_calls = re.findall(
        r"auth::verify_\w+\([^;]*?\.map_err\(\|e\| Status::(\w+)\(", service, flags=re.DOTALL
    )
    assert verify_calls, "no auth::verify_* -> Status mapping found in flight_service.rs"
    assert set(verify_calls) == {"unauthenticated"}, (
        f"a ticket verification now maps to {sorted(set(verify_calls))}; the classifier's "
        f"{_DP_TICKET_EXPIRED_SIGNATURES!r} match assumes every one is unauthenticated"
    )


def test_serialization_conflict_classified_retriable():
    f = _submission_dp_fetch_failure(
        "could not fetch adapter sequences ...", Exception(_REAL_40001)
    )
    assert f.kind is FailureKind.DATA_PLANE_TRANSIENT
    assert f.transient is True  # a redrive self-heals
    assert f.stage is WorkTicketFailureStage.SUBMISSION
    assert f.step_name is None


def test_unavailable_classified_retriable():
    f = _submission_dp_fetch_failure(
        "could not materialize reads for prep_sample 30451 ...",
        Exception(_REAL_UNAVAILABLE),
    )
    assert f.kind is FailureKind.DATA_PLANE_TRANSIENT
    assert f.transient is True  # DP was briefly unreachable; a redrive self-heals
    assert f.stage is WorkTicketFailureStage.SUBMISSION
    assert f.step_name is None


def test_ticket_expired_classified_retriable():
    f = _submission_dp_fetch_failure(
        "could not materialize reads for prep_sample 30504 ...",
        Exception(_REAL_EXPIRED),
    )
    assert f.kind is FailureKind.DATA_PLANE_TRANSIENT
    assert f.transient is True  # the next attempt mints a fresh token
    assert f.stage is WorkTicketFailureStage.SUBMISSION
    assert f.step_name is None


def test_other_dp_fetch_failure_stays_permanent():
    f = _submission_dp_fetch_failure("could not fetch adapter sequences ...", Exception("boom"))
    assert f.kind is FailureKind.BAD_INPUT
    assert f.transient is False  # not retried — an operator must resolve it
    assert f.stage is WorkTicketFailureStage.SUBMISSION
    assert f.step_name is None


def test_bad_signature_stays_permanent():
    """A signing-key mismatch reaches this layer as the SAME pyarrow class as an
    expired token. It must stay permanent — a redrive re-signs with the same wrong
    key and fails identically."""
    f = _submission_dp_fetch_failure(
        "could not materialize reads for prep_sample 30504 ...",
        Exception(_REAL_BAD_SIGNATURE),
    )
    assert f.kind is FailureKind.BAD_INPUT
    assert f.transient is False
