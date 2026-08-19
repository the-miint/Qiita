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
        active_rows=[],
        held_total=0,
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
        active_rows=[],
        held_total=0,
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
        active_rows=[],
        held_total=0,
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
        active_rows=[],
        held_total=0,
    )
    assert "\n" not in rendered.subject
    assert "\r" not in rendered.subject


def test_detail_list_truncated_with_overflow():
    tickets = [_ticket(i) for i in range(MAX_DETAIL_ROWS + 25)]
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=tickets,
        generated_at=_GENERATED_AT,
        active_rows=[],
        held_total=0,
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
        active_rows=[],
        held_total=0,
    )
    assert "2026-07-01 12:30:45 UTC" in rendered.text
    assert "2026-07-01 12:30:45 UTC" in rendered.html


def test_contact_email_rendered_when_supplied():
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
        active_rows=[],
        held_total=0,
        contact_email="qiita-help@example.org",
    )
    assert "qiita-help@example.org" in rendered.text
    assert "qiita-help@example.org" in rendered.html
    # The HTML variant links the address for one-click contact.
    assert 'href="mailto:qiita-help@example.org"' in rendered.html
    # The old "please do not reply" line is gone — we set a Reply-To.
    assert "do not reply" not in rendered.text
    assert "do not reply" not in rendered.html


def test_contact_email_omitted_degrades_cleanly():
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
        active_rows=[],
        held_total=0,
    )
    # No address, no contact sentence, but the automated-message note remains.
    assert "automated message from qiita-miint" in rendered.text
    assert "Questions?" not in rendered.text
    assert "mailto:" not in rendered.html


def _active(state: str, n: int, *, action_id="align", version="v1"):
    return {"action_id": action_id, "action_version": version, "state": state, "n": n}


def test_detail_rows_are_one_per_line():
    # Regression: with trim_blocks on, the `{% endif %}` closing the optional
    # failure-reason clause sits at end-of-line and swallows the row's newline,
    # so every detail row (and the footer behind them) collapsed onto one line.
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1, state="failed", failure_reason="boom"), _ticket(2)],
        generated_at=_GENERATED_AT,
        active_rows=[],
        held_total=0,
    )
    lines = rendered.text.splitlines()
    assert "  - ticket 1: align v1 -> failed (boom)" in lines
    assert "  - ticket 2: align v1 -> completed" in lines
    # The footer is its own line, not glued to the last detail row.
    assert "Generated at 2026-07-01 12:30:45 UTC." in lines


def test_active_count_reported_in_body_and_subject():
    # A digest sent mid-fanout must say where in the batch the recipient is, not
    # just what terminalized.
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1, state="failed", failure_reason="boom")],
        generated_at=_GENERATED_AT,
        held_total=0,
        active_rows=[_active("queued", 3), _active("processing", 20)],
    )
    assert rendered.subject == "[qiita-miint] 1 work ticket finished, 23 still active"
    assert "23 still active (3 queued, 20 processing)." in rendered.text
    assert "23 still active" in rendered.html
    assert "3 queued, 20 processing" in rendered.html


def test_no_active_tickets_says_so_explicitly():
    # The "batch is done, act now" signal. Silence would be indistinguishable
    # from "we didn't look".
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
        active_rows=[],
        held_total=0,
    )
    assert "No other work tickets of yours are still active." in rendered.text
    assert "No other work tickets of yours are still active." in rendered.html
    # Nothing in flight → the subject stays as it was.
    assert rendered.subject == "[qiita-miint] 1 work ticket finished"
    assert "still active (" not in rendered.text


def test_held_for_redrive_bucket_is_reported():
    # Retriable-FAILED is withheld from the owed set AND is terminal, so it is in
    # neither of the other two buckets. Without its own line, a digest would tell
    # a recipient nothing is active while N of their tickets sit dead on infra.
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
        active_rows=[],
        held_total=3,
    )
    assert "No other work tickets of yours are still active." in rendered.text
    assert "3 held after exhausting infrastructure retries" in rendered.text
    assert "qiita ticket run" in rendered.text
    assert "3 held after exhausting infrastructure retries" in rendered.html
    # The redrive hint is a literal in the HTML, so its angle brackets escape.
    assert "<idx>" not in rendered.html
    assert "&lt;idx&gt;" in rendered.html


def test_no_held_tickets_renders_no_redrive_line():
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
        active_rows=[],
        held_total=0,
    )
    assert "held after exhausting" not in rendered.text
    assert "held after exhausting" not in rendered.html


def test_active_states_listed_in_lifecycle_order():
    # pending → queued → processing, not alphabetical (which would lead with
    # "processing").
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
        held_total=0,
        active_rows=[_active("processing", 1), _active("pending", 2), _active("queued", 3)],
    )
    assert "6 still active (2 pending, 3 queued, 1 processing)." in rendered.text


def test_terminal_counts_listed_in_lifecycle_order():
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1, state="failed"), _ticket(2, state="no_data"), _ticket(3)],
        generated_at=_GENERATED_AT,
        active_rows=[],
        held_total=0,
    )
    counts = [ln.strip() for ln in rendered.text.splitlines() if ln.startswith("  1 ")]
    assert counts == ["1 completed", "1 no_data", "1 failed"]


def test_per_action_breakdown_only_when_active_spans_several_actions():
    single = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
        held_total=0,
        active_rows=[_active("processing", 20, action_id="bam-to-parquet")],
    )
    # One fanned-out action: the per-action line would just restate the summary.
    assert "20 still active (20 processing)." in single.text
    assert "bam-to-parquet" not in single.text

    several = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
        active_rows=[
            _active("processing", 20, action_id="bam-to-parquet"),
            _active("pending", 4, action_id="read-mask", version="v2"),
            _active("queued", 1, action_id="read-mask", version="v2"),
        ],
        held_total=0,
    )
    assert "25 still active (4 pending, 1 queued, 20 processing)." in several.text
    assert "  20 bam-to-parquet v1 (20 processing)" in several.text
    assert "  5 read-mask v2 (4 pending, 1 queued)" in several.text


def test_html_escapes_active_action_id():
    rendered = render_work_ticket_digest(
        recipient="pi@example.org",
        tickets=[_ticket(1)],
        generated_at=_GENERATED_AT,
        active_rows=[
            _active("queued", 1, action_id="do <bad>"),
            _active("queued", 1, action_id="fine"),
        ],
        held_total=0,
    )
    assert "do <bad>" not in rendered.html
    assert "do &lt;bad&gt;" in rendered.html


def test_template_sha_is_stable_hex():
    a = template_sha("work_ticket_digest")
    b = template_sha("work_ticket_digest")
    assert a == b
    assert len(a) == 64
    int(a, 16)  # hex


def test_state_order_table_is_immutable():
    # A module-level lookup table shared by every render: a stray write from one
    # caller would corrupt every subsequent email. MappingProxyType makes that a
    # TypeError at the write rather than a mystery downstream.
    import pytest

    from qiita_control_plane.notify.render import _STATE_ORDER

    with pytest.raises(TypeError):
        _STATE_ORDER["cancelled"] = 99
