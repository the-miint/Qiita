"""Shared feature-table (OGU) helpers.

The `action_context` scope contract for a feature-table work ticket lives here so
the two boundaries that read it — the alignment DoGet mint route
(`routes/alignment.py`) and the runner resolver (`runner/_feature_table.py`) —
validate ONE rule, not two hand-copied ones. Each boundary still validates
independently (per the fail-at-every-boundary ethos) and translates the raised
`ValueError` into its own error type (HTTP 422 / SUBMISSION BAD_INPUT).

`denovo_alignment_processing_idx` is here for the same reason with a third caller:
the client-side recipe (`cli/user/feature_table.py`) pairs the two arms itself,
from a REST listing rather than from Postgres, and the rule about which pairings
are admissible must not have a server copy and a client copy.
"""

from typing import Any

# The `params["subject"]` value a de novo alignment carries. Here rather than at the
# runner that mints it, because this module is the one both the runner and the client
# recipe import — the reverse direction is a cycle.
SUBJECT_ASSEMBLY = "assembly"


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


def parse_feature_table_denovo(action_context: dict[str, Any]) -> int | None:
    """The optional ``denovo_alignment_idx`` — the second alignment run a COMBINED
    feature table estimates over — or ``None`` for today's reference-only table.

    Separate from ``parse_feature_table_scope`` rather than a third element of its
    tuple, because absence is the ordinary case and every existing caller of that
    function wants nothing to do with this one: a route or resolver that does not
    understand the de novo arm should not be handed it.

    Raises ``ValueError`` on a present-but-malformed value. Absent is not
    malformed; a ``None`` written explicitly reads the same as omitted, so a
    caller can clear the key without special-casing.
    """
    denovo_alignment_idx = action_context.get("denovo_alignment_idx")
    if denovo_alignment_idx is None:
        return None
    if (
        not isinstance(denovo_alignment_idx, int)
        or isinstance(denovo_alignment_idx, bool)
        or denovo_alignment_idx <= 0
    ):
        raise ValueError(
            "feature-table action_context denovo_alignment_idx must be a positive integer"
        )
    return denovo_alignment_idx


def denovo_alignment_processing_idx(
    *,
    denovo_alignment_idx: int,
    denovo_params: Any,
    reference_params: Any,
) -> int:
    """Validate that `denovo_params` is a de novo alignment pairable with the
    reference arm's `reference_params`, and return the assembly run it aligned
    against.

    The one definition of which two alignment runs may form a combined table, called
    by both drivers — the runner resolver reads its params from Postgres, the client
    recipe from the pool listing, and neither may be the only one that checks.

    Two conditions, both of which produce a plausible table rather than an error when
    unchecked:

    * **The de novo arm must address assemblies.** The two arms' param key sets are
      disjoint by construction (`runner/_alignment.py`), so a caller who passes the
      same reference alignment twice is caught here rather than getting a table whose
      "de novo" arm is a second copy of the reference one — which, after precedence,
      is the reference arm reconciled against itself and therefore empty.
    * **Both arms must have been aligned at the same `mask_idx`.** Different masks
      filtered different reads, so the reads one arm could place are not the reads
      the other could, and precedence would be deciding between two populations
      rather than between two placements of one read.

    `processing_idx` is returned rather than taken as a parameter because it is IN
    the de novo alignment's hashed params: the assembly run and the alignment against
    it cannot disagree. That is also why one de novo alignment covers a whole cohort
    — `prep_sample_idx` is not in that hash.

    Raises ``ValueError``; each caller translates to its own error type.
    """
    if not isinstance(denovo_params, dict):
        raise ValueError(
            f"alignment {denovo_alignment_idx} records no params, so it cannot be "
            f"identified as a de novo alignment"
        )
    subject = denovo_params.get("subject")
    if subject != SUBJECT_ASSEMBLY:
        raise ValueError(
            f"alignment {denovo_alignment_idx} is not a de novo alignment: its "
            f"params.subject is {subject!r}, expected {SUBJECT_ASSEMBLY!r}"
        )
    reference_mask_idx = (
        reference_params.get("mask_idx") if isinstance(reference_params, dict) else None
    )
    if denovo_params.get("mask_idx") != reference_mask_idx:
        raise ValueError(
            f"de novo alignment {denovo_alignment_idx} was aligned at mask_idx "
            f"{denovo_params.get('mask_idx')}, but the reference arm was aligned at "
            f"{reference_mask_idx} — both arms must read the same masked pass-set"
        )
    processing_idx = denovo_params.get("processing_idx")
    if not isinstance(processing_idx, int) or isinstance(processing_idx, bool):
        raise ValueError(
            f"de novo alignment {denovo_alignment_idx} carries no processing_idx in "
            f"its params, so the assembly run it aligned against is unknown"
        )
    return processing_idx
