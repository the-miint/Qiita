"""Wire-model tests for the POST /alignment/ticket/doget request body."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from qiita_common.models import AlignmentDoGetTicketRequest


def test_accepts_positive_work_ticket_idx():
    req = AlignmentDoGetTicketRequest(work_ticket_idx=42)
    assert req.work_ticket_idx == 42


@pytest.mark.parametrize("bad", [0, -1])
def test_rejects_non_positive_work_ticket_idx(bad):
    """work_ticket_idx is Field(gt=0) — 0 and negatives are rejected (the route's
    422 boundary), so a bad body never reaches the action_context lookup."""
    with pytest.raises(ValidationError):
        AlignmentDoGetTicketRequest(work_ticket_idx=bad)


def test_forbids_extra_fields():
    """extra='forbid' — alignment_idx / the cohort come from the work ticket's
    action_context, never the request body, so an extra key is a client error."""
    with pytest.raises(ValidationError):
        AlignmentDoGetTicketRequest(work_ticket_idx=1, alignment_idx=7)
