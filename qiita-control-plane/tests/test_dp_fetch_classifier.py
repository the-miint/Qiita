"""Unit tests for the data-plane fetch failure classifier (`runner/_upload.py`).

Pins that a DuckLake serialization conflict (Postgres SQLSTATE 40001) surfaced
over Flight is classified RETRIABLE (`DATA_PLANE_TRANSIENT`), while every other
DP-fetch failure stays permanent (`BAD_INPUT`). The classifier matches the
*stringified* pyarrow `FlightError` by message — there is no typed asyncpg
exception to `isinstance` against at this layer — so a representative real string
is captured here. If the data plane ever reformats that error, THIS test is what
fails (rather than every DP fetch silently reverting to permanent in production).
"""

from __future__ import annotations

from qiita_common.backend_failure import FailureKind
from qiita_common.models import WorkTicketFailureStage

from qiita_control_plane.runner._upload import (
    _is_dp_serialization_conflict,
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


def test_other_dp_fetch_failure_stays_permanent():
    f = _submission_dp_fetch_failure("could not fetch adapter sequences ...", Exception("boom"))
    assert f.kind is FailureKind.BAD_INPUT
    assert f.transient is False  # not retried — an operator must resolve it
    assert f.stage is WorkTicketFailureStage.SUBMISSION
    assert f.step_name is None
