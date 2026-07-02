"""Email transports: the thin layer that puts rendered bytes on the wire.

`Transport` is a structural protocol so the sweeper depends only on
`async send(...) -> message_id`. Three concrete implementations:

- `SmtpTransport` — a plain SMTP relay via aiosmtplib (async, no event-loop
  block), opportunistic/required/never STARTTLS. The live deploy relays through
  an IP-allowlisted, no-auth host.
- `NoOpTransport` — the default when SMTP_HOST is unset (dev/tests). Records
  nothing on the wire but still returns a synthetic message id and logs, so the
  feature is visibly dark rather than silently so.
- `CaptureTransport` — records sends in memory for tests.

`build_transport(settings)` picks between SMTP and no-op on SMTP_HOST.
"""

from __future__ import annotations

import email.utils
import logging
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import aiosmtplib

if TYPE_CHECKING:
    from ..config import Settings

_log = logging.getLogger(__name__)


# Defined here in the lower transport layer (though render.py produces it) so
# `Transport.send`'s signature stays dependency-free and there is no
# render↔transport import cycle.
@dataclass(frozen=True, slots=True)
class RenderedEmail:
    """A fully-rendered message: line-one subject plus text and (optional)
    HTML alternatives. `html` empty means text-only."""

    subject: str
    text: str
    html: str = ""


@runtime_checkable
class Transport(Protocol):
    """Structural contract every transport satisfies.

    `name` is recorded verbatim on the email_receipt (`smtp` / `noop` /
    `capture`), so it proves whether a send was live. `send` returns the
    message id (RFC Message-ID or relay id) and raises on failure — the
    sweeper's per-originator try/except turns a raise into a retriable
    `failed` receipt."""

    name: str

    async def send(self, *, to: str, rendered: RenderedEmail) -> str: ...


def build_message(
    *,
    sender: str,
    reply_to: str | None,
    to: str,
    rendered: RenderedEmail,
) -> EmailMessage:
    """Build a multipart (text + optional html) MIME message with a generated
    Message-ID. Shared by SmtpTransport and the transport tests."""
    msg = EmailMessage()
    # Subject is already newline-stripped by the renderer, but be defensive:
    # a CR/LF in a header is the classic injection vector.
    msg["Subject"] = rendered.subject.replace("\r", " ").replace("\n", " ")
    msg["From"] = sender
    msg["To"] = to
    if reply_to:
        msg["Reply-To"] = reply_to
    msg["Message-ID"] = email.utils.make_msgid()
    msg["Date"] = email.utils.formatdate(localtime=True)
    msg.set_content(rendered.text)
    if rendered.html:
        msg.add_alternative(rendered.html, subtype="html")
    return msg


class SmtpTransport:
    """Plain SMTP relay via aiosmtplib."""

    name = "smtp"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        sender: str,
        reply_to: str | None,
        starttls: str,
        timeout_seconds: int,
    ) -> None:
        self._host = host
        self._port = port
        self._sender = sender
        self._reply_to = reply_to
        self._starttls = starttls
        self._timeout_seconds = timeout_seconds

    def _start_tls_arg(self) -> bool | None:
        # aiosmtplib: start_tls=None → opportunistic (STARTTLS iff advertised),
        # True → required, False → never.
        if self._starttls == "required":
            return True
        if self._starttls == "never":
            return False
        return None

    async def send(self, *, to: str, rendered: RenderedEmail) -> str:
        msg = build_message(
            sender=self._sender,
            reply_to=self._reply_to,
            to=to,
            rendered=rendered,
        )
        await aiosmtplib.send(
            msg,
            hostname=self._host,
            port=self._port,
            start_tls=self._start_tls_arg(),
            timeout=self._timeout_seconds,
        )
        return msg["Message-ID"]


class NoOpTransport:
    """Default transport when SMTP_HOST is unset. Sends nothing, but returns a
    synthetic message id and logs so the feature is not silently dark."""

    name = "noop"

    async def send(self, *, to: str, rendered: RenderedEmail) -> str:
        message_id = email.utils.make_msgid()
        _log.info(
            "NoOpTransport: would send email to %s subject=%r (message_id=%s)",
            to,
            rendered.subject,
            message_id,
        )
        return message_id


@dataclass(slots=True)
class CaptureTransport:
    """In-memory transport for tests: records every send and hands back a
    generated message id."""

    name: str = "capture"
    sent: list[tuple[str, RenderedEmail, str]] = field(default_factory=list)

    async def send(self, *, to: str, rendered: RenderedEmail) -> str:
        message_id = email.utils.make_msgid()
        self.sent.append((to, rendered, message_id))
        return message_id


def build_transport(settings: Settings) -> Transport:
    """Return the configured transport: SmtpTransport when SMTP_HOST is set,
    else NoOpTransport. Reply-To reuses CONTACT_EMAIL."""
    if settings.smtp_host:
        return SmtpTransport(
            host=settings.smtp_host,
            port=settings.smtp_port,
            sender=settings.smtp_from,
            reply_to=settings.contact_email,
            starttls=settings.smtp_starttls,
            timeout_seconds=settings.smtp_timeout_seconds,
        )
    return NoOpTransport()
