"""Unit tests for the email renderer (no DB, no network)."""

from datetime import UTC, datetime

from qiita_control_plane.notify.render import (
    MAX_DETAIL_ROWS,
    render_work_ticket_digest,
    template_sha,
)

_GENERATED_AT = datetime(2026, 7, 1, 12, 30, 45, tzinfo=UTC)


def _ticket(idx: int, *, state: str = "completed", failure_reason=None, action_id="align"):
    return {
        "idx": idx,
        "action_id": action_id,
        "action_version": "v1",
        "state": state,
        "failure_reason": failure_reason,
    }


def test_renders_subject_text_html():
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1), _ticket(2, state="failed", failure_reason="boom")],
        generated_at=_GENERATED_AT,
    )
    assert rendered.subject == "[qiita-miint] 2 work tickets finished"
    # Both counts appear in the text body.
    assert "1 completed" in rendered.text
    assert "1 failed" in rendered.text
    assert "ticket 1" in rendered.text
    assert "ticket 2" in rendered.text
    assert "boom" in rendered.text
    assert rendered.html  # HTML alternative is present.


def test_subject_singular_for_one_ticket():
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
    )
    assert rendered.subject == "[qiita-miint] 1 work ticket finished"


def test_html_escapes_failure_reason_and_action_id():
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[
            _ticket(
                1,
                state="failed",
                failure_reason="<script>alert('x')</script>",
                action_id="do <bad>",
            )
        ],
        generated_at=_GENERATED_AT,
    )
    # Autoescape neutralizes the markup in the HTML body...
    assert "<script>" not in rendered.html
    assert "&lt;script&gt;" in rendered.html
    assert "&lt;bad&gt;" in rendered.html
    # ...while the plain-text body carries the raw characters (no escaping).
    assert "<script>alert('x')</script>" in rendered.text
    assert "do <bad>" in rendered.text


def test_subject_is_single_line():
    # A template that emitted a newline would be a header-injection vector;
    # the renderer collapses whitespace to a single line.
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
    )
    assert "\n" not in rendered.subject
    assert "\r" not in rendered.subject


def test_detail_list_truncated_with_overflow():
    tickets = [_ticket(i) for i in range(MAX_DETAIL_ROWS + 25)]
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=tickets,
        generated_at=_GENERATED_AT,
    )
    # The subject counts the full set...
    assert f"{MAX_DETAIL_ROWS + 25}" in rendered.subject
    # ...but the detail list is capped and the overflow is summarized.
    assert "... and 25 more" in rendered.text
    assert f"ticket {MAX_DETAIL_ROWS - 1}" in rendered.text
    # Row beyond the cap is not spelled out individually.
    assert f"ticket {MAX_DETAIL_ROWS + 24}:" not in rendered.text


def test_utc_label_present():
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
    )
    assert "2026-07-01 12:30:45 UTC" in rendered.text
    assert "2026-07-01 12:30:45 UTC" in rendered.html


def test_template_sha_is_stable_hex():
    a = template_sha("work_ticket_digest")
    b = template_sha("work_ticket_digest")
    assert a == b
    assert len(a) == 64
    int(a, 16)  # hex
