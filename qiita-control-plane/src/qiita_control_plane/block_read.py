"""Shared block-read scope rule.

A block-scoped compute job (read-mask-block's ``qc`` / ``host_filter``, align's
``align_sharded``) reads its block's reads by STREAMING them from the data plane
over a block-read DoGet, minted at job runtime by ``work_ticket_idx``. This module
owns the one rule that turns a block work ticket into that ticket's scope, so the
mint route (``routes/read.py``) and its tests validate the same thing the runner
used to decide at submit time.

Two selectors, mirroring the data plane's ``BLOCK_READ_SOURCES``:

* ``read_block`` — the block's RAW ``read`` rows, scoped by members alone. What a
  read-mask block masks.
* ``read_masked_block`` — the block's ``read_masked`` rows (trimmed,
  host/QC-``pass``-filtered) scoped by members AND exactly one ``mask_idx``. What
  an align block aligns.

Choosing between them is safety-critical, not cosmetic: streaming raw reads to an
align job would realign non-host-depleted, un-QC'd (human-containing) reads. See
``resolve_block_read_scope`` for why the discriminator is ``action_context`` and
not the ``work_ticket.alignment_idx`` column.
"""

from typing import Any

# The two block-read DoGet selector names. Must match the data plane's
# BLOCK_READ_SOURCES (flight_service.rs) — these are the values we sign into a
# ticket's `table`, and the data plane rejects anything else.
READ_BLOCK_TABLE = "read_block"
READ_MASKED_BLOCK_TABLE = "read_masked_block"


def resolve_block_read_scope(
    *,
    action_context: dict[str, Any],
    ticket_alignment_idx: int | None,
    ticket_mask_idx: int | None,
) -> tuple[str, dict[str, list[int]]]:
    """Decide which block-read selector a block work ticket authorizes.

    Returns ``(table, filter)`` for `sign_ticket`; the caller supplies the
    ``members`` selector separately (it comes from ``qiita.block_member``, not
    from the ticket). Raises ``ValueError`` on any inconsistent ticket — the
    caller translates it to its own error type (HTTP 422 at the mint route).

    ``ticket_alignment_idx`` / ``ticket_mask_idx`` are the work_ticket COLUMNS;
    ``action_context`` is the plan-time context.

    **Why the discriminator is action_context, not the column.** An align block
    ticket carries a non-NULL ``alignment_idx`` and must read MASKED reads. But
    ``work_ticket.alignment_idx`` is ``ON DELETE SET NULL``, so a mid-flight
    ``DELETE /alignment-definition`` NULLs the column while ``action_context``
    still carries the idx. Discriminating on the column would then silently fall
    through to the raw-reads branch and stream un-QC'd, non-host-depleted reads
    into an aligner. So we read the intent from ``action_context`` and treat any
    disagreement with the column as a hard error rather than a fallback.

    This check is MORE load-bearing here than it was at submit time: the mint
    happens at job runtime, so it sees deletions that landed after submission.
    """
    context_alignment_idx = action_context.get("alignment_idx")

    if context_alignment_idx is None:
        # A read-mask block: raw reads, no mask scope. If the COLUMN carries an
        # alignment_idx the context does not, the ticket is malformed — fail
        # rather than guess which one describes the work.
        if ticket_alignment_idx is not None:
            raise ValueError(
                f"work ticket carries alignment_idx {ticket_alignment_idx} but its "
                "action_context declares none — refusing to guess whether this block "
                "reads raw or masked reads"
            )
        return READ_BLOCK_TABLE, {}

    if ticket_alignment_idx != context_alignment_idx:
        raise ValueError(
            f"align block ticket action_context alignment_idx {context_alignment_idx} "
            f"disagrees with work_ticket.alignment_idx {ticket_alignment_idx!r} — the "
            "alignment definition was likely deleted mid-flight (the column is ON "
            "DELETE SET NULL); refusing to silently stream raw reads to an aligner"
        )
    if ticket_mask_idx is None:
        raise ValueError(
            "an align block ticket must carry the completed mask_idx its reads were "
            "masked under (set at plan time); found NULL"
        )
    return READ_MASKED_BLOCK_TABLE, {"mask_idx": [ticket_mask_idx]}
