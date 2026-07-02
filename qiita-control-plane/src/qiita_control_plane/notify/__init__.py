"""Email-notification subsystem for the control plane.

`transport.py` sends bytes over the wire (SMTP relay / no-op / capture);
`render.py` turns a template bundle + context into a `RenderedEmail`; and
`sweeper.py` is the in-process asyncio loop that coalesces a work-ticket
originator's terminal tickets into one digest and records an audit receipt.

The public entry points are `build_transport` (lifespan wiring) and
`run_sweeper` (the long-lived task started in lifespan).
"""

from .render import RenderedEmail, render_email, render_work_ticket_digest, template_sha
from .sweeper import run_sweeper, sweep_once
from .transport import (
    CaptureTransport,
    NoOpTransport,
    SmtpTransport,
    Transport,
    build_transport,
)

__all__ = [
    "CaptureTransport",
    "NoOpTransport",
    "RenderedEmail",
    "SmtpTransport",
    "Transport",
    "build_transport",
    "render_email",
    "render_work_ticket_digest",
    "run_sweeper",
    "sweep_once",
    "template_sha",
]
