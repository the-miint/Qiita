"""Jinja2 email rendering.

Each email is a bundle of three templates under `templates/email/`:
`<name>.subject.j2`, `<name>.html.j2`, `<name>.txt.j2`. Autoescape is on for
the HTML template (injection guard) and off for subject/text. The subject
is newline-collapsed so a template can't smuggle a header.

`render_work_ticket_digest` is the one bundle currently defined; it tallies the
terminal tickets by state, tallies the originator's still-active tickets by
state (and by action), carries the count held back pending a redrive, and
truncates the detail list before handing context to the generic `render_email`.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from jinja2 import Environment, FileSystemLoader
from qiita_common.models import WorkTicketState

from .transport import RenderedEmail

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates" / "email"

# The Jinja template-file suffixes that carry HTML and therefore need
# autoescaping. Names end in `.j2`, so the stock `select_autoescape`
# (which keys off `.html`/`.htm`) would never fire — hence a custom callable.
_HTML_SUFFIXES = (".html.j2",)

WORK_TICKET_DIGEST_TEMPLATE = "work_ticket_digest"

# Detail rows past this are summarized as "... and N more" so a thousand-sample
# plate isn't a multi-MB email.
MAX_DETAIL_ROWS = 50

# Lifecycle order — pending, queued, processing, completed, no_data, failed —
# read straight off the enum's declaration order. Every state list in the email
# is sorted by it, so a reader sees the states in the order a ticket actually
# moves through them rather than alphabetically.
_STATE_ORDER = {state.value: i for i, state in enumerate(WorkTicketState)}


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


def _ordered_counts(counter: Counter[str]) -> list[tuple[str, int]]:
    """(state, n) pairs in lifecycle order. An unknown state sorts last rather
    than raising — a digest must not fail to render over a cosmetic detail."""
    return sorted(
        counter.items(), key=lambda kv: (_STATE_ORDER.get(kv[0], len(_STATE_ORDER)), kv[0])
    )


def _phrase(counts: list[tuple[str, int]]) -> str:
    """The inline gloss: "3 queued, 20 processing"."""
    return ", ".join(f"{n} {state}" for state, n in counts)


def summarize_active(active_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Roll the per-(action, state) active tallies up into what the templates
    render: the total, the inline per-state gloss, the per-state map, and the
    per-action breakdown.

    Public because the sweeper also stores this on the `email_receipt` — the
    receipt must record the claim the email *made*, so both go through this one
    function rather than each rolling the rows up their own way.
    """
    by_state: Counter[str] = Counter()
    by_action: dict[tuple[str, str], Counter[str]] = {}
    for row in active_rows:
        n = int(row["n"])
        by_state[row["state"]] += n
        key = (row["action_id"], row["action_version"])
        by_action.setdefault(key, Counter())[row["state"]] += n

    # The per-action breakdown only earns its space when the active set spans
    # more than one action — for a single fanned-out action (the common case:
    # one `submit-pacbio-ingest` fanning out N `bam-to-parquet` tickets) it
    # would just restate the summary line.
    actions: list[dict[str, Any]] = []
    if len(by_action) > 1:
        actions = [
            {
                "action_id": action_id,
                "action_version": version,
                "total": sum(counts.values()),
                "summary": _phrase(_ordered_counts(counts)),
            }
            for (action_id, version), counts in sorted(by_action.items())
        ]

    ordered = _ordered_counts(by_state)
    return {
        "total": sum(by_state.values()),
        "summary": _phrase(ordered),
        "by_state": dict(ordered),
        "actions": actions,
    }


def render_work_ticket_digest(
    *,
    recipient: str,
    tickets: list[dict[str, Any]],
    generated_at: datetime,
    active_rows: Sequence[Mapping[str, Any]],
    held_total: int,
    contact_email: str | None = None,
) -> RenderedEmail:
    """Render the work-ticket terminal-digest bundle.

    `tickets` is the full owed set for one originator, each a dict with
    `idx, action_id, action_version, state, failure_reason`. Counts are tallied
    by state; the detail list is truncated to `MAX_DETAIL_ROWS` with an
    overflow count.

    `active_rows` is the same originator's *still-active* (non-terminal) tickets,
    pre-aggregated by the sweeper as `{action_id, action_version, state, n}` rows;
    `held_total` is how many of theirs are parked in retriable-FAILED, which the
    owed set deliberately withholds from email pending a redrive. Together the
    three buckets account for every ticket the recipient has: what just finished,
    what is still coming, and what is stuck. Both are REQUIRED — an empty
    `active_rows` is the meaningful "nothing else is in flight" answer, which the
    templates state outright, so defaulting it would let a caller emit that claim
    without ever having checked.

    `contact_email` (the deploy's `CONTACT_EMAIL`, also the message's `Reply-To`)
    is rendered as a contact line in the footer; omit it and the footer degrades
    to the automated-message note with no address.
    """
    total = len(tickets)
    counts = _ordered_counts(Counter(t["state"] for t in tickets))
    detail = tickets[:MAX_DETAIL_ROWS]
    overflow = total - len(detail)
    active = summarize_active(active_rows)
    return render_email(
        WORK_TICKET_DIGEST_TEMPLATE,
        recipient=recipient,
        tickets=detail,
        total=total,
        counts=counts,
        overflow=overflow,
        active_total=active["total"],
        active_summary=active["summary"],
        active_actions=active["actions"],
        held_total=held_total,
        generated_at=_format_generated_at(generated_at),
        contact_email=contact_email,
    )
