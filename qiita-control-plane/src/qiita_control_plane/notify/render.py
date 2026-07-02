"""Jinja2 email rendering.

Each email is a bundle of three templates under `templates/email/`:
`<name>.subject.j2`, `<name>.html.j2`, `<name>.txt.j2`. Autoescape is on for
the HTML template (injection guard) and off for subject/text. The subject
is newline-collapsed so a template can't smuggle a header.

`render_work_ticket_digest` is the one bundle currently defined; it computes the
per-state counts and truncates the detail list before handing context to the
generic `render_email`.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from .transport import RenderedEmail

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates" / "email"

# The Jinja template-file suffixes that carry HTML and therefore need
# autoescaping. Names end in `.j2`, so the stock `select_autoescape`
# (which keys off `.html`/`.htm`) would never fire — hence a custom callable.
_HTML_SUFFIXES = (".html.j2",)

WORK_TICKET_DIGEST_TEMPLATE = "work_ticket_digest"

# Detail rows past this are summarized as "... and N more" so a thousand-sample
# plate isn't a multi-MB email.
MAX_DETAIL_ROWS = 50


def _autoescape(template_name: str | None) -> bool:
    if template_name is None:
        return False
    return template_name.endswith(_HTML_SUFFIXES)


_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATES_DIR)),
    autoescape=_autoescape,
    trim_blocks=True,
    lstrip_blocks=True,
)


def _strip_subject(raw: str) -> str:
    """Collapse whitespace (incl. newlines) to a single line — header-injection
    guard and cosmetic tidy for a subject rendered from a template."""
    return " ".join(raw.split())


def render_email(name: str, /, **context: Any) -> RenderedEmail:
    """Render the `<name>.{subject,html,txt}.j2` bundle against `context`."""
    subject = _strip_subject(_env.get_template(f"{name}.subject.j2").render(**context))
    html = _env.get_template(f"{name}.html.j2").render(**context)
    text = _env.get_template(f"{name}.txt.j2").render(**context)
    return RenderedEmail(subject=subject, text=text, html=html)


def template_sha(name: str) -> str:
    """SHA-256 over the three template files' bytes — the rendered revision's
    content hash, stored on the receipt for reproducibility."""
    h = hashlib.sha256()
    for suffix in ("subject", "html", "txt"):
        h.update((_TEMPLATES_DIR / f"{name}.{suffix}.j2").read_bytes())
    return h.hexdigest()


def _format_generated_at(generated_at: datetime) -> str:
    """Explicit-UTC label so a recipient never guesses the timezone."""
    return generated_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def render_work_ticket_digest(
    *,
    recipient: str,
    tickets: list[dict[str, Any]],
    generated_at: datetime,
    contact_email: str | None = None,
) -> RenderedEmail:
    """Render the work-ticket terminal-digest bundle.

    `tickets` is the full owed set for one originator, each a dict with
    `idx, action_id, action_version, state, failure_reason`. Counts are tallied
    by state; the detail list is truncated to `MAX_DETAIL_ROWS` with an
    overflow count.

    `contact_email` (the deploy's `CONTACT_EMAIL`, also the message's `Reply-To`)
    is rendered as a contact line in the footer; omit it and the footer degrades
    to the automated-message note with no address.
    """
    total = len(tickets)
    counts = sorted(Counter(t["state"] for t in tickets).items())
    detail = tickets[:MAX_DETAIL_ROWS]
    overflow = total - len(detail)
    return render_email(
        WORK_TICKET_DIGEST_TEMPLATE,
        recipient=recipient,
        tickets=detail,
        total=total,
        counts=counts,
        overflow=overflow,
        generated_at=_format_generated_at(generated_at),
        contact_email=contact_email,
    )
