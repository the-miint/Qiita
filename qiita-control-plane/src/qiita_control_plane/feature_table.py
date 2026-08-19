"""Shared feature-table (OGU) helpers.

The `action_context` scope contract for a feature-table work ticket lives here so
the two boundaries that read it — the alignment DoGet mint route
(`routes/alignment.py`) and the runner resolver (`runner/_feature_table.py`) —
validate ONE rule, not two hand-copied ones. Each boundary still validates
independently (per the fail-at-every-boundary ethos) and translates the raised
`ValueError` into its own error type (HTTP 422 / SUBMISSION BAD_INPUT).
"""

from typing import Any


def parse_feature_table_scope(action_context: dict[str, Any]) -> tuple[int, list[int]]:
    """Validate and extract ``(alignment_idx, prep_sample_idx cohort)`` from a
    feature-table ticket's ``action_context``.

    Returns the positive ``alignment_idx`` and the non-empty list of positive
    ``prep_sample_idx``. Raises ``ValueError`` on any bad shape. ``bool`` is an
    ``int`` subclass in Python, so it is rejected explicitly — a JSON ``true``
    must never masquerade as an identifier.
    """
    alignment_idx = action_context.get("alignment_idx")
    prep_sample_idx = action_context.get("prep_sample_idx")
    if not isinstance(alignment_idx, int) or isinstance(alignment_idx, bool) or alignment_idx <= 0:
        raise ValueError("feature-table action_context requires a positive alignment_idx")
    if (
        not isinstance(prep_sample_idx, list)
        or not prep_sample_idx
        or not all(
            isinstance(p, int) and not isinstance(p, bool) and p > 0 for p in prep_sample_idx
        )
    ):
        raise ValueError(
            "feature-table action_context requires a non-empty prep_sample_idx list "
            "of positive integers"
        )
    return alignment_idx, prep_sample_idx
