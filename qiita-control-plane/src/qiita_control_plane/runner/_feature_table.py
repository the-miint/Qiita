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

When ``action_context`` carries ``denovo_alignment_idx`` the ticket asks for a
COMBINED table and the resolver does all three again for the de novo arm: it
validates that alignment addresses assemblies at the reference arm's mask, applies
the per-sample arm gate (``_ARM_GATE``), and stages a second map. Absent that key
nothing below runs and the resolver's behaviour is unchanged.

``reference_idx`` is deliberately NOT injected as a binding: a reference-scoped
ticket already flows it to the job as a scope scalar
(``SCOPE_SCALARS_BY_KIND[REFERENCE]``), and re-binding it via ``params:`` would
collide with that injection.
"""

import json
from pathlib import Path
from typing import Any

import asyncpg

from ..actions.library import export_assembly_member_genome, export_member_genome
from ..feature_table import (
    denovo_alignment_processing_idx,
    parse_feature_table_denovo,
    parse_feature_table_scope,
)
from ..repositories.alignment_definition import fetch_alignment_definition_by_idx
from ..repositories.assembly import (
    ASSEMBLY_SAMPLE_COMPLETED,
    ASSEMBLY_SAMPLE_NO_DATA,
    count_assembly_membership_without_genome,
    fetch_assembly_sample_states,
)
from ..repositories.block import list_incomplete_alignment_samples
from ._upload import _submission_bad_input

# The genome-map Parquet the compute job consumes as an input (feature_idx ->
# genome_idx for the whole reference), staged by _resolve_feature_table_bindings.
GENOME_MAP_PATH_BINDING = "genome_map_path"

# Cap on how many offending prep_sample_idx values a completeness error lists.
_MAX_REPORTED = 20


# The de novo genome-map Parquet, staged beside the reference one for a combined
# table. A separate binding rather than a wider file: the two maps have different
# shapes (this one carries `prep_sample_idx`), and a job holding one path could not
# tell a reference-only ticket from a combined one.
DENOVO_GENOME_MAP_PATH_BINDING = "denovo_genome_map_path"

# The assembly run the de novo arm aligned against, bound as a SCALAR beside that
# map — a scalar cannot ride `inputs:`, so the step declares it under `params:` the
# way `mask_idx` is declared. Derived here rather than taken as a context key: it is
# in the de novo alignment's own hashed params, so a caller cannot name a run the
# alignment did not use.
DENOVO_PROCESSING_IDX_BINDING = "denovo_processing_idx"


async def _validate_denovo_arm(
    pool: asyncpg.Pool,
    *,
    denovo_alignment_idx: int,
    reference_alignment_params: dict[str, Any],
    prep_sample_idx: list[int],
) -> int:
    """Validate the de novo alignment and its cohort, returning the assembly run's
    ``processing_idx``.

    Two of the three checks are ``denovo_alignment_processing_idx``, shared with the
    client-side driver so the rule about which two runs may be paired has one
    definition. The third — the per-sample arm gate below — is the whole reason this
    function is not ``list_incomplete_alignment_samples`` again.
    """
    row = await fetch_alignment_definition_by_idx(pool, denovo_alignment_idx)
    if row is None:
        raise _submission_bad_input(f"alignment {denovo_alignment_idx} not found")
    params = row["params"]
    if isinstance(params, str):
        params = json.loads(params)
    try:
        processing_idx = denovo_alignment_processing_idx(
            denovo_alignment_idx=denovo_alignment_idx,
            denovo_params=params,
            reference_params=reference_alignment_params,
        )
    except ValueError as exc:
        raise _submission_bad_input(str(exc)) from exc

    await _apply_arm_gate(
        pool,
        denovo_alignment_idx=denovo_alignment_idx,
        processing_idx=processing_idx,
        prep_sample_idx=prep_sample_idx,
    )
    return processing_idx


async def _apply_arm_gate(
    pool: asyncpg.Pool,
    *,
    denovo_alignment_idx: int,
    processing_idx: int,
    prep_sample_idx: list[int],
) -> None:
    """Decide, per prep_sample, whether the de novo arm is expected — and refuse the
    submission if any prep_sample's two gates disagree about that.

    **The assembly gate is what disambiguates a missing de novo alignment row.**
    ``list_incomplete_alignment_samples`` treats absence as incomplete, which is
    right for the reference arm and wrong here: a prep_sample that assembled nothing
    never gets a de novo ``alignment_sample`` row, and refusing the cohort for it
    would contradict the design's own rule that a combined table degrades
    gracefully to plain reference alignment rather than erroring. But loosening it
    to "no row → skip this prep_sample" is worse, because absence is then ambiguous
    between *legitimately nothing to assemble* and *the de novo alignment has not
    run yet* — and the second silently returns a reference-only answer for a
    prep_sample
    that should have had both arms.

    So the assembly gate decides which absence is which. Its five states and what
    each means are the canonical statement on
    ``repositories.assembly.fetch_assembly_sample_state``; what is decided HERE is
    only which of them is fatal to the cohort and which degrades one prep_sample to
    reference-only:

    * ``no_data`` — nothing was assembled, so no de novo arm is expected and this
      prep_sample is reference-only. The one graceful path.
    * ``completed`` — contigs exist, so a de novo alignment row is REQUIRED.
      Absent or ``pending`` is the silent-wrong-answer case and is refused.
    * ``invalidated`` — the contigs are withdrawn.
    * ``pending`` — the assembly is still running, so the answer would change
      underneath the table.
    * absent — the run never reached this prep_sample, which is not the same as its
      having had nothing to assemble.

    The last three are refusals whatever the alignment gate says, because none of
    them is a state a table may be built over.
    """
    assembly_states = await fetch_assembly_sample_states(
        pool, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
    )
    expected = [
        idx for idx in prep_sample_idx if assembly_states.get(idx) == ASSEMBLY_SAMPLE_COMPLETED
    ]
    unusable = {
        idx: assembly_states.get(idx)
        for idx in prep_sample_idx
        if assembly_states.get(idx) not in (ASSEMBLY_SAMPLE_COMPLETED, ASSEMBLY_SAMPLE_NO_DATA)
    }
    if unusable:
        listed = sorted(unusable.items())[:_MAX_REPORTED]
        raise _submission_bad_input(
            f"assembly run {processing_idx}: {len(unusable)} of {len(prep_sample_idx)} "
            f"prep_sample(s) are neither 'completed' nor 'no_data' in assembly_sample, "
            f"so a combined table cannot be built over them — a run that is still going, "
            f"withdrawn, or never reached this prep_sample is not an assembly that "
            f"produced nothing: {listed}"
        )

    incomplete = await list_incomplete_alignment_samples(pool, denovo_alignment_idx, expected)
    if incomplete:
        raise _submission_bad_input(
            f"de novo alignment {denovo_alignment_idx}: {len(incomplete)} of "
            f"{len(expected)} assembled prep_sample(s) are not completed "
            f"(alignment_sample). These samples HAVE contigs, so leaving them out of "
            f"the de novo arm would answer reference-only for a prep_sample that "
            f"should have had both arms: {incomplete[:_MAX_REPORTED]}"
        )


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
        denovo_alignment_idx = parse_feature_table_denovo(action_context)
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
    bindings: dict[str, Any] = {GENOME_MAP_PATH_BINDING: genome_map_path}

    if denovo_alignment_idx is None:
        return bindings

    processing_idx = await _validate_denovo_arm(
        pool,
        denovo_alignment_idx=denovo_alignment_idx,
        reference_alignment_params=params,
        prep_sample_idx=prep_sample_idx,
    )
    # The map is refused rather than silently short when any of the run's
    # memberships has no genome, for the reason
    # `count_assembly_membership_without_genome` gives: the omission does not show
    # up as missing rows, it shows up as those genomes covering more of a shorter
    # length than they really do.
    unminted = await count_assembly_membership_without_genome(
        pool, prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    )
    if unminted:
        listed = sorted(unminted.items())[:_MAX_REPORTED]
        raise _submission_bad_input(
            f"{len(unminted)} prep_sample(s) of assembly run {processing_idx} have "
            f"membership rows with no genome_idx, so the de novo map would omit their "
            f"contigs and shorten their genomes' length denominators. An operator has "
            f"to run the assembly-genome backfill on the host before this run can be "
            f"used as a de novo arm; `qiita-admin` is host-side and reads "
            f"DATABASE_URL, so a submitter cannot run it: {listed}"
        )

    denovo_map_path = workspace / "denovo_genome_map.parquet"
    await export_assembly_member_genome(
        pool,
        prep_sample_idx=prep_sample_idx,
        processing_idx=processing_idx,
        out_path=denovo_map_path,
    )
    bindings[DENOVO_GENOME_MAP_PATH_BINDING] = denovo_map_path
    bindings[DENOVO_PROCESSING_IDX_BINDING] = processing_idx
    return bindings
