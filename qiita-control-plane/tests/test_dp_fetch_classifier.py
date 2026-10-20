"""Unit tests for the data-plane failure classification both services share
(`qiita_common.data_plane_errors`) and the control-plane wrapper that files it
(`runner/_upload.py`).

Pins which DP-fetch failures are RETRIABLE (`DATA_PLANE_TRANSIENT`) — a DuckLake
serialization conflict (Postgres SQLSTATE 40001) surfaced over Flight, a gRPC
UNAVAILABLE, the data plane refusing a request for want of a free connection, and an
expired signing token — while every other DP-fetch failure stays permanent
(`BAD_INPUT`), including the other unauthenticated causes, which share the expiry's
pyarrow error class and gRPC status but not its self-healing.

The classifier matches the *stringified* exception by message, because there is no type
to match on at this layer: the pyarrow class differs per gRPC status (a `FlightError`
subclass for most, `ArrowInvalid` — a plain `ValueError` — for RESOURCE_EXHAUSTED), and
the DuckLake conflict arrives as text inside a Flight error rather than as the
`asyncpg` exception the control plane's own DB errors raise. A representative real
string is captured here for each. If the data plane ever reformats one of these, THIS
test is what fails, rather than every DP fetch silently reverting to permanent in
production.
"""

from __future__ import annotations

from pathlib import Path

from qiita_common.backend_failure import FailureKind
from qiita_common.data_plane_errors import (
    DATA_PLANE_BUSY_SIGNATURE,
    DATA_PLANE_TICKET_EXPIRED_SIGNATURES,
    is_data_plane_busy,
    is_data_plane_serialization_conflict,
    is_data_plane_ticket_expired,
    is_data_plane_unavailable,
)
from qiita_common.models import WorkTicketFailureStage

from qiita_control_plane.runner._upload import _submission_dp_fetch_failure
from qiita_control_plane.testing.flight import REAL_BUSY_REFUSAL as _REAL_BUSY

# A representative stringified pyarrow FlightError carrying the Postgres 40001
# serialization message, as seen verbatim in a real read-mask fan-out
# failure_reason (the concurrent DuckLake-attach race this classifier exists for).
# The data plane's sources, which the contract tests below parse rather than restate.
DATA_PLANE_SRC = Path(__file__).resolve().parents[2] / "qiita-data-plane" / "src"

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
# Both fixtures are shapes a live data plane produces: it answers `unauthenticated` for
# an expired ticket and `unavailable` when it cannot open a DuckLake connection (the
# catalog unreachable or at its connection limit).
_SERVER_RETURNED_EXPIRED = (
    "ticket expired. Detail: Unauthenticated. gRPC client debug context: "
    "UNKNOWN:Error received from peer ipv4:127.0.0.1:50112 "
    '{created_time:"2026-09-03T21:17:51.828988-06:00", grpc_status:16, '
    'grpc_message:"ticket expired. Detail: Unauthenticated"}. Client context: '
    "IOError: Server never sent a data message. Detail: Internal"
)

# A server-returned UNAVAILABLE: no connect failure happened, so none of the
# connect-shaped substrings appear — which is the half of the match this fixture
# discriminates.
_SERVER_RETURNED_UNAVAILABLE = (
    "data plane is shutting down. Detail: Unavailable. gRPC client debug context: "
    "UNKNOWN:Error received from peer ipv4:127.0.0.1:50112 "
    '{created_time:"2026-09-03T21:17:51.830074-06:00", grpc_status:14, '
    'grpc_message:"data plane is shutting down. Detail: Unavailable"}'
)


def test_expiry_is_detected_in_both_pyarrow_renderings():
    """The classifier must fire on the form a live data plane produces, not only on
    the message-prefix form the other fixtures use."""
    assert is_data_plane_ticket_expired(Exception(_REAL_EXPIRED)) is True
    assert is_data_plane_ticket_expired(Exception(_SERVER_RETURNED_EXPIRED)) is True


def test_a_data_plane_that_is_up_but_unavailable_is_retriable():
    """A server-returned UNAVAILABLE carries no connect failure, so the three
    connect-shaped substrings never appear and only "detail: unavailable" matches it.
    The data plane answers this status when it cannot open a DuckLake connection, and
    gRPC defines it as retriable, so a redrive self-heals it and it must not land as a
    bad-input an operator resolves."""
    assert is_data_plane_unavailable(Exception(_SERVER_RETURNED_UNAVAILABLE)) is True
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
    assert is_data_plane_ticket_expired(Exception(same_message_other_status)) is False
    assert is_data_plane_ticket_expired(Exception(_SERVER_RETURNED_UNAVAILABLE)) is False
    assert is_data_plane_unavailable(Exception(_SERVER_RETURNED_EXPIRED)) is False


def test_serialization_conflict_is_detected():
    assert is_data_plane_serialization_conflict(Exception(_REAL_40001)) is True


def test_non_serialization_error_is_not_detected():
    assert is_data_plane_serialization_conflict(Exception("some other flight error")) is False
    assert is_data_plane_serialization_conflict(FileNotFoundError("missing file")) is False


def test_unavailable_is_detected():
    assert is_data_plane_unavailable(Exception(_REAL_UNAVAILABLE)) is True


def test_non_unavailable_error_is_not_detected():
    # A serialization conflict is retriable, but NOT via the unavailable path.
    assert is_data_plane_unavailable(Exception(_REAL_40001)) is False
    assert is_data_plane_unavailable(Exception("some other flight error")) is False


def test_ticket_expired_is_detected():
    assert is_data_plane_ticket_expired(Exception(_REAL_EXPIRED)) is True


def test_other_unauthenticated_errors_are_not_detected():
    # Same class, same gRPC status, permanent cause — must not be swept in.
    assert is_data_plane_ticket_expired(Exception(_REAL_BAD_SIGNATURE)) is False
    assert is_data_plane_ticket_expired(Exception(_REAL_40001)) is False
    assert is_data_plane_ticket_expired(Exception("some other flight error")) is False
    # `AuthError::ExpiryTooFar` — the nearest miss, and the variant a reword of the
    # Rust `Display` impl is most likely to collide with.
    assert (
        is_data_plane_ticket_expired(
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
    assert is_data_plane_ticket_expired(Exception("work ticket expired while queued")) is False


def test_expiry_signatures_match_the_data_planes_wording():
    """The classifier keys off text the Rust `Display` impl produces. Neither
    language can import the other, so the const is parsed out of the source —
    the same mechanism `tests/auth/test_auth.py` uses for the projection allowlist.
    A reword in `auth.rs` fails HERE rather than silently reverting every expired
    token to a permanent BAD_INPUT."""
    import re

    src = (DATA_PLANE_SRC / "auth.rs").read_text()
    m = re.search(r'AuthError::Expired => write!\(f, "([^"]+)"\)', src)
    assert m, "AuthError::Expired arm not found in auth.rs"
    assert m.group(1) == "ticket expired", (
        f"the data plane now renders an expired ticket as {m.group(1)!r}; "
        f"update DATA_PLANE_TICKET_EXPIRED_SIGNATURES ({DATA_PLANE_TICKET_EXPIRED_SIGNATURES!r}) "
        "or expired tokens silently classify permanent again"
    )
    # The other half of the match: every ticket verification maps its AuthError to
    # gRPC unauthenticated, which is what puts that word in the client's error. A
    # verify site mapped to any other Status would break the match, so read them
    # out of the source rather than asserting the constant against itself.
    service = (DATA_PLANE_SRC / "flight_service.rs").read_text()
    verify_calls = re.findall(
        r"auth::verify_\w+\([^;]*?\.map_err\(\|e\| Status::(\w+)\(", service, flags=re.DOTALL
    )
    assert verify_calls, "no auth::verify_* -> Status mapping found in flight_service.rs"
    assert set(verify_calls) == {"unauthenticated"}, (
        f"a ticket verification now maps to {sorted(set(verify_calls))}; the classifier's "
        f"{DATA_PLANE_TICKET_EXPIRED_SIGNATURES!r} match assumes every one is unauthenticated"
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


def test_busy_data_plane_classified_retriable():
    """A refusal for want of a free connection is overload, not bad input: the same
    fetch succeeds once a connection frees, so it must not be filed permanent."""
    f = _submission_dp_fetch_failure(
        "could not materialize reads for prep_sample 31074 ...",
        ValueError(_REAL_BUSY),
    )
    assert f.kind is FailureKind.DATA_PLANE_TRANSIENT
    assert f.transient is True  # a redrive self-heals once the pool frees up
    assert f.stage is WorkTicketFailureStage.SUBMISSION
    assert f.step_name is None


def test_busy_predicate_does_not_fire_on_the_other_dp_errors():
    """The match is the data plane's own prefix, so it covers the refusal and nothing
    else that carries the same gRPC status."""
    assert is_data_plane_busy(ValueError(_REAL_BUSY)) is True
    assert is_data_plane_busy(Exception(_REAL_40001)) is False
    assert is_data_plane_busy(Exception(_REAL_UNAVAILABLE)) is False
    assert is_data_plane_busy(Exception(_REAL_EXPIRED)) is False
    assert is_data_plane_busy(Exception("Flight returned internal error: no such table")) is False
    # The case the status name alone would have swallowed: gRPC answers
    # RESOURCE_EXHAUSTED for an oversized message too, and no redrive shrinks it.
    assert (
        is_data_plane_busy(
            ValueError(
                "gRPC returned resource exhausted error, with message: Received message "
                "larger than max (21368092 vs. 4194304)"
            )
        )
        is False
    )


def test_busy_signature_matches_the_data_planes_wording():
    """The signature is the data plane's own prefix, which no client library renders
    for us, so a reword in the Rust fails HERE rather than silently reverting every
    refusal to a permanent BAD_INPUT. Every `resource_exhausted` refusal in the
    component is checked — whatever shape it is written in — so one added later cannot
    ship unclassified."""
    import re

    refusals: list[str] = []
    for src_file in sorted(DATA_PLANE_SRC.glob("*.rs")):
        src = src_file.read_text()
        for call in re.finditer(r"Status::resource_exhausted\(", src):
            # The literals inside this call only: up to its `");`, which is where
            # a `format!` refusal ends. Reading past it collects whatever literal
            # comes next in the function.
            tail = src[call.end() : call.end() + 600]
            refusals.extend(re.findall(r'"([^"]*)"', tail.split(");", 1)[0]))
    assert refusals, "no Status::resource_exhausted refusal found in the data plane"
    assert any(DATA_PLANE_BUSY_SIGNATURE in r.lower() for r in refusals), (
        f"no data-plane refusal carries {DATA_PLANE_BUSY_SIGNATURE!r}; found {refusals!r}. "
        "Update the signature or a busy data plane classifies permanent again"
    )
    for refusal in refusals:
        assert DATA_PLANE_BUSY_SIGNATURE in refusal.lower(), (
            f"the data plane has a refusal that does not carry "
            f"{DATA_PLANE_BUSY_SIGNATURE!r}: {refusal!r}"
        )


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


# ---------------------------------------------------------------------------
# An `action:` entry's data-plane failure (`runner/_reconstruct._dispatch_action`).


async def _dispatch_action_raising(
    monkeypatch, exc: BaseException, name: str = "register-files"
) -> tuple[BaseException, list]:
    """Run `_dispatch_action` with its primitive raising `exc` and the progress-row
    writes stubbed; returns what it raised and the failure kinds it recorded."""
    from qiita_common.actions import WorkflowAction

    from qiita_control_plane.runner import _reconstruct

    recorded: list = []

    async def _noop(*a, **k):
        return None

    async def _record_failed(pool, **kw):
        recorded.append(kw["failure_kind"])

    async def _raise(*a, **k):
        raise exc

    monkeypatch.setattr(_reconstruct.step_progress, "record_submitting", _noop)
    monkeypatch.setattr(_reconstruct, "_best_effort_record_failed", _record_failed)
    monkeypatch.setattr(_reconstruct, "_run_action_primitive", _raise)
    entry = WorkflowAction(kind="action", name=name, inputs=[], outputs=[])
    try:
        await _reconstruct._dispatch_action(
            object(),
            entry,
            {},
            Path("/unused"),
            {"kind": "prep_sample", "prep_sample_idx": 1},
            work_ticket_idx=1,
            step_index=0,
            attempt=0,
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://dp:50051",
        )
    except BaseException as raised:
        return raised, recorded
    raise AssertionError("_dispatch_action did not raise")


async def test_action_entry_busy_data_plane_is_retried(monkeypatch):
    """A busy refusal from an `action:` entry's DoAction leaves `_dispatch_action` as a
    transient `DATA_PLANE_TRANSIENT` failure, which the retry loop re-runs."""
    import pyarrow.flight as flight
    from qiita_common.backend_failure import BackendFailure

    raised, recorded = await _dispatch_action_raising(
        monkeypatch, flight.FlightUnavailableError(_REAL_BUSY)
    )
    assert isinstance(raised, BackendFailure)
    assert raised.kind is FailureKind.DATA_PLANE_TRANSIENT
    assert raised.transient is True
    assert raised.stage is WorkTicketFailureStage.STEP_RUN
    assert raised.step_name == "register-files"
    assert recorded == [FailureKind.DATA_PLANE_TRANSIENT.value]


async def test_action_entry_other_failure_stays_unchanged(monkeypatch):
    """Any other failure from an `action:` entry propagates as it was raised and is
    recorded `UNKNOWN_PERMANENT`, as before."""
    exc = RuntimeError("boom")
    raised, recorded = await _dispatch_action_raising(monkeypatch, exc)
    assert raised is exc
    assert recorded == [FailureKind.UNKNOWN_PERMANENT.value]


async def test_action_entry_postgres_error_is_not_filed_as_a_data_plane_failure(monkeypatch):
    """A primitive's own Postgres serialization failure carries the same SQLSTATE text
    the data-plane classifier matches, but is not a pyarrow error, so it is not filed
    `DATA_PLANE_TRANSIENT`."""
    exc = RuntimeError("could not serialize access due to concurrent update")
    raised, recorded = await _dispatch_action_raising(monkeypatch, exc)
    assert raised is exc
    assert recorded == [FailureKind.UNKNOWN_PERMANENT.value]


async def test_register_files_rerun_after_the_files_moved_says_so(tmp_path):
    """The data plane moves the staged files before its catalog transaction, so a
    re-run of `register-files` can find the staging directory empty. Its reason says
    that an earlier attempt may have committed rather than only that nothing is there."""
    import pytest
    from qiita_common.actions import WorkflowAction

    from qiita_control_plane.runner import _run_action_primitive

    staging = tmp_path / "staging"
    staging.mkdir()
    entry = WorkflowAction(kind="action", name="register-files", inputs=["staging"], outputs=[])
    with pytest.raises(RuntimeError, match="registration may have committed"):
        await _run_action_primitive(
            object(),
            entry,
            {"staging": str(staging)},
            tmp_path,
            {"kind": "prep_sample", "prep_sample_idx": 1},
            work_ticket_idx=1,
            signing_key=b"\x00" * 32,
            data_plane_url="grpc://dp:50051",
        )


async def test_register_files_is_not_rerun_after_an_unavailable_data_plane(monkeypatch):
    """An unavailable data plane may have dropped partway through moving the staged
    files, so `register-files` is not re-run on it: the re-run would register only the
    files left in staging."""
    import pyarrow.flight as flight

    exc = flight.FlightUnavailableError(_REAL_UNAVAILABLE)
    raised, recorded = await _dispatch_action_raising(monkeypatch, exc)
    assert raised is exc
    assert recorded == [FailureKind.UNKNOWN_PERMANENT.value]


async def test_other_action_entry_is_rerun_after_an_unavailable_data_plane(monkeypatch):
    """Any other action entry is re-run on every transient data-plane failure."""
    import pyarrow.flight as flight
    from qiita_common.backend_failure import BackendFailure

    raised, recorded = await _dispatch_action_raising(
        monkeypatch,
        flight.FlightUnavailableError(_REAL_UNAVAILABLE),
        name="delete-alignment-sample",
    )
    assert isinstance(raised, BackendFailure)
    assert raised.kind is FailureKind.DATA_PLANE_TRANSIENT
    assert recorded == [FailureKind.DATA_PLANE_TRANSIENT.value]


async def test_register_files_is_rerun_after_an_expired_token(monkeypatch):
    """An expired token is refused at verification, before the data plane moves any
    staged file, so `register-files` is re-run on it."""
    import pyarrow.flight as flight
    from qiita_common.backend_failure import BackendFailure

    raised, recorded = await _dispatch_action_raising(
        monkeypatch, flight.FlightUnauthenticatedError(_REAL_EXPIRED)
    )
    assert isinstance(raised, BackendFailure)
    assert raised.kind is FailureKind.DATA_PLANE_TRANSIENT
    assert recorded == [FailureKind.DATA_PLANE_TRANSIENT.value]
