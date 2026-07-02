"""Unit tests for the email transports. aiosmtplib is mocked — no network."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from qiita_control_plane.notify import transport as transport_mod
from qiita_control_plane.notify.transport import (
    CaptureTransport,
    NoOpTransport,
    RenderedEmail,
    SmtpTransport,
    build_transport,
)

_RENDERED = RenderedEmail(
    subject="[qiita-miint] 1 work ticket finished",
    text="plain body",
    html="<p>html body</p>",
)


async def test_noop_returns_message_id_and_sends_nothing():
    t = NoOpTransport()
    assert t.name == "noop"
    message_id = await t.send(to="pi@example.org", rendered=_RENDERED)
    assert message_id.startswith("<") and message_id.endswith(">")


async def test_capture_records_sends():
    t = CaptureTransport()
    assert t.name == "capture"
    mid = await t.send(to="pi@example.org", rendered=_RENDERED)
    assert len(t.sent) == 1
    to, rendered, recorded_id = t.sent[0]
    assert to == "pi@example.org"
    assert rendered is _RENDERED
    assert recorded_id == mid


async def test_smtp_builds_multipart_and_headers(monkeypatch):
    fake_send = AsyncMock(return_value=({}, "250 OK"))
    monkeypatch.setattr(transport_mod.aiosmtplib, "send", fake_send)

    t = SmtpTransport(
        host="relay.example.org",
        port=25,
        sender="donotreply@ucsd.edu",
        reply_to="qiita-help@ucsd.edu",
        starttls="opportunistic",
        timeout_seconds=15,
    )
    message_id = await t.send(to="pi@example.org", rendered=_RENDERED)

    fake_send.assert_awaited_once()
    msg = fake_send.await_args.args[0]
    kwargs = fake_send.await_args.kwargs
    assert kwargs["hostname"] == "relay.example.org"
    assert kwargs["port"] == 25
    assert kwargs["start_tls"] is None  # opportunistic
    assert kwargs["timeout"] == 15

    assert msg["From"] == "donotreply@ucsd.edu"
    assert msg["To"] == "pi@example.org"
    assert msg["Reply-To"] == "qiita-help@ucsd.edu"
    assert msg["Subject"] == _RENDERED.subject
    assert msg["Message-ID"] == message_id
    # Multipart: text + html alternative.
    assert msg.is_multipart()
    subtypes = {part.get_content_subtype() for part in msg.iter_parts()}
    assert subtypes == {"plain", "html"}


@pytest.mark.parametrize(
    ("mode", "expected"),
    [("opportunistic", None), ("required", True), ("never", False)],
)
async def test_smtp_starttls_mapping(monkeypatch, mode, expected):
    fake_send = AsyncMock(return_value=({}, "250 OK"))
    monkeypatch.setattr(transport_mod.aiosmtplib, "send", fake_send)
    t = SmtpTransport(
        host="relay.example.org",
        port=25,
        sender="donotreply@ucsd.edu",
        reply_to=None,
        starttls=mode,
        timeout_seconds=15,
    )
    await t.send(to="pi@example.org", rendered=_RENDERED)
    assert fake_send.await_args.kwargs["start_tls"] is expected


def test_build_transport_picks_smtp_when_host_set():
    settings = SimpleNamespace(
        smtp_host="relay.example.org",
        smtp_port=25,
        smtp_from="donotreply@ucsd.edu",
        smtp_starttls="opportunistic",
        smtp_timeout_seconds=15,
        contact_email="qiita-help@ucsd.edu",
    )
    t = build_transport(settings)
    assert isinstance(t, SmtpTransport)
    assert t.name == "smtp"


def test_build_transport_noop_when_host_unset():
    settings = SimpleNamespace(
        smtp_host=None,
        smtp_port=25,
        smtp_from="donotreply@ucsd.edu",
        smtp_starttls="opportunistic",
        smtp_timeout_seconds=15,
        contact_email="qiita-help@ucsd.edu",
    )
    assert isinstance(build_transport(settings), NoOpTransport)
