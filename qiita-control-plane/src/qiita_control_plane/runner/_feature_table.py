"""Runner resolver for the metagenomic feature-table (OGU) workflow.

Runs at SUBMIT (before the step loop), gated on a step consuming
``genome_map_path``. Given the ticket's ``action_context`` (``alignment_idx`` +
the explicit ``prep_sample_idx`` cohort) and the reference-scoped ticket's
``reference_idx`` (a framework scope scalar), it:

1. verifies the ticket names a real alignment whose reference matches the scope
   (an alignment carries its own reference; a mismatch would make the genome map,
   the per-feature lengths, and the alignment's ``feature_idx`` belong to
   different references and silently drop everything at coverage/OGU time);
2. refuses an incomplete cohort — any ``prep_sample_idx`` not ``completed`` in
   ``alignment_sample`` — because a partial cohort would build a table over an
   incomplete alignment (this is the completeness gate the DoGet mint route
   delegates upstream); and
3. stages the reference's ``feature_idx -> genome_idx`` map as a workspace
   Parquet the compute job reads (Postgres-only data; the alignment slice and
   per-feature lengths stream from the data plane at job runtime).

``reference_idx`` is deliberately NOT injected as a binding: a reference-scoped
ticket already flows it to the job as a scope scalar
(``SCOPE_SCALARS_BY_KIND[REFERENCE]``), and re-binding it via ``params:`` would
collide with that injection.
"""

import json
from pathlib import Path
from typing import Any

import asyncpg

from ..actions.library import export_member_genome
from ..feature_table import parse_feature_table_scope
from ..repositories.alignment_definition import fetch_alignment_definition_by_idx
from ..repositories.block import list_incomplete_alignment_samples
from ._upload import _submission_bad_input

# The genome-map Parquet the compute job consumes as an input (feature_idx ->
# genome_idx for the whole reference), staged by _resolve_feature_table_bindings.
GENOME_MAP_PATH_BINDING = "genome_map_path"

# Cap on how many offending prep_sample_idx values a completeness error lists.
_MAX_REPORTED = 20


async def _resolve_feature_table_bindings(
    pool: asyncpg.Pool,
    *,
    action_context: dict[str, Any],
    reference_idx: int,
    workspace: Path,
) -> dict[str, Any]:
    """Validate the cohort + reference and stage the feature->genome map.

    Returns ``{GENOME_MAP_PATH_BINDING: <parquet path>}``. Raises a
    SUBMISSION-attributed BackendFailure (BAD_INPUT) on any bad input so it lands
    in ``run_workflow``'s outer FAILED handler, like the other pre-loop resolvers.
    """
    try:
        alignment_idx, prep_sample_idx = parse_feature_table_scope(action_context)
    except ValueError as exc:
        raise _submission_bad_input(str(exc)) from exc

    row = await fetch_alignment_definition_by_idx(pool, alignment_idx)
    if row is None:
        raise _submission_bad_input(f"alignment {alignment_idx} not found")
    params = row["params"]
    if isinstance(params, str):
        params = json.loads(params)
    align_reference_idx = params.get("reference_idx") if isinstance(params, dict) else None
    if align_reference_idx != reference_idx:
        raise _submission_bad_input(
            f"alignment {alignment_idx} targets reference {align_reference_idx}, but the "
            f"work ticket is scoped to reference {reference_idx}"
        )

    incomplete = await list_incomplete_alignment_samples(pool, alignment_idx, prep_sample_idx)
    if incomplete:
        raise _submission_bad_input(
            f"alignment {alignment_idx}: {len(incomplete)} of {len(prep_sample_idx)} "
            f"prep_sample(s) are not completed (alignment_sample) — cannot build a "
            f"feature table over an incomplete cohort: {incomplete[:_MAX_REPORTED]}"
        )

    workspace.mkdir(parents=True, exist_ok=True)
    genome_map_path = workspace / "feature_genome_map.parquet"
    await export_member_genome(pool, reference_idx, genome_map_path)
    return {GENOME_MAP_PATH_BINDING: genome_map_path}
