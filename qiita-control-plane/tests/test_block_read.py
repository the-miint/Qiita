"""Unit tests for the block-read scope rule (`qiita_control_plane.block_read`).

Pure function, no DB — the rule that decides whether a block work ticket streams
RAW reads or MASK-scoped reads. Getting this wrong streams un-QC'd,
non-host-depleted (human-containing) reads into an aligner, so every branch is
pinned here rather than only exercised through the route.
"""

import pytest

from qiita_control_plane.block_read import (
    READ_BLOCK_TABLE,
    READ_MASKED_BLOCK_TABLE,
    resolve_block_read_scope,
)


def test_read_mask_block_resolves_to_the_raw_selector():
    """No alignment intent ⇒ a read-mask block, which masks RAW reads."""
    table, filter_ = resolve_block_read_scope(
        action_context={"instrument_model": "NovaSeq"},
        ticket_alignment_idx=None,
        ticket_mask_idx=42,
    )
    assert table == READ_BLOCK_TABLE
    # Raw block reads are scoped by members alone — a mask filter here would
    # wrongly restrict the reads a mask is being COMPUTED over.
    assert filter_ == {}


def test_align_block_resolves_to_the_mask_scoped_selector():
    table, filter_ = resolve_block_read_scope(
        action_context={"alignment_idx": 7},
        ticket_alignment_idx=7,
        ticket_mask_idx=42,
    )
    assert table == READ_MASKED_BLOCK_TABLE
    assert filter_ == {"mask_idx": [42]}


def test_alignment_deleted_mid_flight_is_a_hard_error():
    """work_ticket.alignment_idx is ON DELETE SET NULL.

    A mid-flight `DELETE /alignment-definition` NULLs the column while
    action_context still names the alignment. Falling through to the raw branch
    would stream un-QC'd, non-host-depleted reads into an aligner, so the
    disagreement must fail loudly instead.
    """
    with pytest.raises(ValueError, match="deleted mid-flight"):
        resolve_block_read_scope(
            action_context={"alignment_idx": 7},
            ticket_alignment_idx=None,
            ticket_mask_idx=42,
        )


def test_column_alignment_without_context_alignment_is_a_hard_error():
    """The mirror-image disagreement is equally unresolvable.

    Silently choosing either branch would be a guess about what the block is
    for; there is no safe default, so refuse.
    """
    with pytest.raises(ValueError, match="refusing to guess"):
        resolve_block_read_scope(
            action_context={},
            ticket_alignment_idx=7,
            ticket_mask_idx=42,
        )


def test_align_block_requires_a_mask_idx():
    """An align block reads the pass-set of ONE mask; without it the selector
    would blend every mask's rows for those ranges."""
    with pytest.raises(ValueError, match="must carry the completed mask_idx"):
        resolve_block_read_scope(
            action_context={"alignment_idx": 7},
            ticket_alignment_idx=7,
            ticket_mask_idx=None,
        )


def test_align_block_mismatched_idx_is_a_hard_error():
    """A column/context mismatch on the VALUE (not just presence) is the same
    class of error — the ticket describes two different alignments."""
    with pytest.raises(ValueError, match="disagrees with"):
        resolve_block_read_scope(
            action_context={"alignment_idx": 7},
            ticket_alignment_idx=8,
            ticket_mask_idx=42,
        )


def test_raw_block_needs_no_mask_idx():
    """A read-mask block's mask_idx is the mask it is CREATING, not a filter on
    its input, so its absence must not block the raw selector."""
    table, filter_ = resolve_block_read_scope(
        action_context={},
        ticket_alignment_idx=None,
        ticket_mask_idx=None,
    )
    assert (table, filter_) == (READ_BLOCK_TABLE, {})
