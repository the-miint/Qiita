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
   Parquet the compute job reads (Postgres data; the alignment slice and
   per-feature lengths stream from the data plane at job runtime instead).

When ``action_context`` carries ``denovo_alignment_idx`` the ticket asks for a
COMBINED table and the resolver does all three again for the de novo arm: it
validates that alignment addresses assemblies at the reference arm's mask, applies
the per-sample arm gate (``_ARM_GATE``), and stages a second map. Absent that key
nothing below runs and the resolver's behaviour is unchanged.

That arm also stages a THIRD Parquet the reference arm has no counterpart for — the
assembled genomes' CheckM scores — and it is the one thing here that reads the data
plane as well as Postgres, because neither store holds the whole of it.
``_stage_denovo_genome_quality`` carries why.

``reference_idx`` is deliberately NOT injected as a binding: a reference-scoped
ticket already flows it to the job as a scope scalar
(``SCOPE_SCALARS_BY_KIND[REFERENCE]``), and re-binding it via ``params:`` would
collide with that injection.
"""

import json
from pathlib import Path
from typing import Any

import asyncpg
import pyarrow as pa
import pyarrow.flight as flight
from qiita_common.assembly_constants import (
    BIN_QUALITY_SCORE_COLUMNS,
    BIN_QUALITY_SUBJECT_KEY,
    BIN_QUALITY_TABLE,
)
from qiita_common.parquet import PARQUET_OPTS_INTERMEDIATE, validate_parquet_path

from ..actions.library import export_assembly_member_genome, export_member_genome
from ..auth.tickets import run_signed_flight_call, sign_ticket
from ..feature_table import (
    denovo_alignment_processing_idx,
    parse_feature_table_denovo,
    parse_feature_table_scope,
)
from ..miint import duckdb_connect
from ..repositories.alignment_definition import fetch_alignment_definition_by_idx
from ..repositories.assembly import (
    ASSEMBLY_SAMPLE_COMPLETED,
    ASSEMBLY_SAMPLE_NO_DATA,
    count_assembly_membership_without_genome,
    fetch_assembly_genome_subject,
    fetch_assembly_sample_states,
)
from ..repositories.block import list_incomplete_alignment_samples
from ._upload import _submission_bad_input, _submission_dp_fetch_failure

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

# The de novo arm's per-genome quality Parquet: one row per genome of the run,
# carrying the CheckM scores of the subject that genome was minted from. A superset
# of the map's genomes rather than exactly them — `_stage_denovo_genome_quality`
# says when the two can differ and why the extra rows are inert.
#
# A THIRD file rather than more columns on the de novo map, because that map's row
# set is shared verbatim with the REST genome map (`GET /assembly/.../genome-map`),
# so widening it would either change that response or fork the shared SQL. The
# shapes differ anyway: the map is per CONTIG and this is per GENOME.
DENOVO_GENOME_QUALITY_PATH_BINDING = "denovo_genome_quality_path"

# The subject relation the quality join drives from: the shared subject key plus the
# genome it bridges to. The types come from `BIN_QUALITY_SUBJECT_KEY`'s own `_idx`
# rule rather than a table spelled here, so this cannot say string where the DDL pin
# says BIGINT — a disagreement that would bind through an implicit cast and match
# nothing, which is the failure the pin exists to catch one layer up.
_SUBJECT_COLUMNS = (*BIN_QUALITY_SUBJECT_KEY, "genome_idx")
_SUBJECT_ARROW_TYPES = {
    col: (pa.int64() if col.endswith("_idx") else pa.string()) for col in _SUBJECT_COLUMNS
}


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


def _do_get_bin_quality(data_plane_url: str, ticket_bytes: bytes) -> pa.Table:
    """Synchronous Flight DoGet of one assembly run's per-subject quality rows —
    runs in a thread executor (pyarrow.flight is sync). A module function so tests
    patch this attribute; unlike `_reference._do_get_reference_sequence_chunks` it is
    not re-exported from the package, so a `_runner_pkg.`-style patch would miss it.

    `read_all()` where the taxonomy export streams: that one is a row per feature of
    a GG2-scale reference, this one a row per assembled subject. Measured on the
    deploy 2026-09-04 — the whole lake's `bin_quality` is 23,027 rows / ~2.2 MB
    across four runs, and the largest single run is 9,030. A cohort is a subset of
    one run, so this fits in memory by a wide margin.

    The ticket scopes to a run and cannot narrow by `kind` (not a filterable column),
    so the UNBINNED rows the join later drops arrive too. That is 5.6% of the table —
    799 rows on the largest run — so narrowing it would buy nothing.
    """
    with flight.FlightClient(data_plane_url) as client:
        return client.do_get(flight.Ticket(ticket_bytes)).read_all()


def _write_denovo_genome_quality(
    subjects: list[asyncpg.Record], quality: pa.Table, out_path: Path
) -> None:
    """Join the streamed quality rows onto the run's subject->genome bridge and write
    `(prep_sample_idx, genome_idx)` plus `BIN_QUALITY_SCORE_COLUMNS` to `out_path`,
    one row per subject the bridge names.

    Both sides are already in memory and neither is bound to a step, so they are
    registered as Arrow relations on one connection rather than staged as files: an
    intermediate written to the ticket workspace only to be read back three
    statements later is a shared-filesystem dependency bought for nothing, and a file
    nothing declares is one nothing cleans up.

    **LEFT from the subject side.** A genome with no quality row keeps its row with
    NULL scores. `bin_quality` is written empty-with-schema when CheckM scored
    nothing — no refined bin, none circular, or no CheckM DB — and `_write_bin_quality`
    skips a class whose tool output is absent, so a MAG or LCG subject with no quality
    row is a normal outcome. (The residue length cut is NOT one of the causes here:
    the driving side admits MAG and LCG only.) An inner join would drop those genomes
    and leave this file disagreeing with the map it must match.

    **The join carries `prep_sample_idx` because `bin_id` is only unique within a
    prep_sample.** It is a refined bin's FASTA stem for a MAG and the assembler's
    contig id for an LCG, and both restart per prep_sample — every one of them has a
    `bin.1`, and contig ids collide the same way — so without that term one
    prep_sample's scores land on its neighbour's genome. (A different reason from the
    one `reconcile.denovo_map_join` gives for its own prep_sample term: that one is
    about a content-addressed `feature_idx`, which does not apply to a key made of
    names.)

    **One quality row per subject is assumed, not enforced.** Two rows for one
    `BIN_QUALITY_SUBJECT_KEY` would fan this join out to two rows for one genome,
    silently. The upstream writer joins CheckM's lineage and qa tables on their Bin Id
    column and LEFT-joins DAS_Tool's summary, so a duplicate key in any of the three
    would reach here. None ever has: on the deploy 2026-09-04 the lake held 23,027
    `bin_quality` rows under 23,027 distinct subject keys across every run. That is
    evidence the tools do not duplicate in practice, not a proof they cannot, which is
    why this stays an assumption — but a `DISTINCT` here would hide such a load rather
    than fix it.
    """
    scores = ", ".join(f"q.{c}" for c in BIN_QUALITY_SCORE_COLUMNS)
    on = " AND ".join(f"q.{c} = s.{c}" for c in BIN_QUALITY_SUBJECT_KEY)
    out_sql = validate_parquet_path(out_path)
    # Both the ON clause above and this relation's key columns come from the one
    # constant: a member added to it must appear on both sides of the join, and
    # spelling them here by hand would instead bind `q.<new> = s.<new>` against a
    # relation that has no such column.
    subject_table = pa.table(
        {
            col: pa.array([r[col] for r in subjects], _SUBJECT_ARROW_TYPES[col])
            for col in _SUBJECT_COLUMNS
        }
    )
    success = False
    try:
        with duckdb_connect() as con:
            # `PARQUET_OPTS_INTERMEDIATE` requires this (its own comment says why);
            # safe here because nothing reads this file in row order.
            con.execute("SET preserve_insertion_order=false")
            con.register("assembly_subject", subject_table)
            con.register("bin_quality_stream", quality)
            con.execute(
                f"COPY (SELECT s.prep_sample_idx, s.genome_idx, {scores}"
                f" FROM assembly_subject s"
                f" LEFT JOIN bin_quality_stream q ON {on})"
                f" TO '{out_sql}' ({PARQUET_OPTS_INTERMEDIATE})"
            )
        success = True
    finally:
        # A half-written file left where the binding would have pointed, for the
        # reason `_resolve_qc_adapters` unlinks its own: the next resume rebinds this
        # path without rewriting it.
        if not success:
            out_path.unlink(missing_ok=True)


async def _stage_denovo_genome_quality(
    pool: asyncpg.Pool,
    *,
    prep_sample_idx: list[int],
    processing_idx: int,
    workspace: Path,
    data_plane_url: str,
    signing_key: bytes,
) -> Path:
    """Stage the de novo arm's per-genome CheckM scores as a workspace Parquet.

    Two sources, joined here because neither store holds both halves: the scores are
    `bin_quality` in DuckLake, keyed by the subject CheckM scored; `genome_idx` is a
    Postgres column (`fetch_assembly_genome_subject` carries that).

    Signed in-process rather than through a route, like the adapter set and the shard
    roster: no route mints a `bin_quality` ticket, so this resolver is the only path
    to the table.

    Read AFTER the map export, in a separate transaction. Nothing deletes from
    `qiita.assembly_membership` or `qiita.genome` on any production path, so the two
    reads can only diverge by ADDING a subject between them — leaving this side a
    superset of the map's genomes, never a subset. A quality row for a genome the map
    omits is inert (nothing joins it); a mapped genome missing from here would not be,
    which is why the order is this way round rather than the reverse.

    The lake read is a third snapshot, and the workflow does mint a run's genomes
    before registering its `bin_quality` rows — `write-assembly-membership` runs
    several steps ahead of `register-files`. That window is not reachable from here:
    `finalize-assembly-sample` writes the `completed` state LAST, after
    `register-files`, and `_validate_denovo_arm` refuses any prep_sample not in that
    state. So a run nameable as a de novo arm has its lake rows registered, and an
    unscored genome here means CheckM did not score it rather than a load still in
    flight.
    """
    subjects = await fetch_assembly_genome_subject(
        pool, prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    )
    try:
        quality = await run_signed_flight_call(
            lambda: sign_ticket(
                table=BIN_QUALITY_TABLE,
                filter={
                    "prep_sample_idx": prep_sample_idx,
                    "processing_idx": [processing_idx],
                },
                secret=signing_key,
            ),
            lambda ticket: _do_get_bin_quality(data_plane_url, ticket),
        )
    except Exception as exc:
        raise _submission_dp_fetch_failure(
            f"could not fetch bin_quality for assembly run {processing_idx} from the "
            f"data plane: {type(exc).__name__}: {exc}",
            exc,
        ) from exc
    quality_path = workspace / "denovo_genome_quality.parquet"
    _write_denovo_genome_quality(subjects, quality, quality_path)
    return quality_path


async def _resolve_feature_table_bindings(
    pool: asyncpg.Pool,
    *,
    action_context: dict[str, Any],
    reference_idx: int,
    workspace: Path,
    data_plane_url: str,
    signing_key: bytes,
) -> dict[str, Any]:
    """Validate the cohort + reference and stage the feature->genome map.

    Returns ``{GENOME_MAP_PATH_BINDING: <parquet path>}`` for a reference-only
    ticket, plus ``DENOVO_GENOME_MAP_PATH_BINDING``,
    ``DENOVO_GENOME_QUALITY_PATH_BINDING`` and ``DENOVO_PROCESSING_IDX_BINDING``
    when the context asks for the de novo arm. Those three are bound together or
    not at all, which is what lets the job refuse a partial binding. Raises a
    SUBMISSION-attributed BackendFailure (BAD_INPUT) on any bad input so it lands
    in ``run_workflow``'s outer FAILED handler, like the other pre-loop resolvers.

    ``data_plane_url`` / ``signing_key`` are used only by the de novo arm; a
    reference-only ticket never touches them.
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
    bindings[DENOVO_GENOME_QUALITY_PATH_BINDING] = await _stage_denovo_genome_quality(
        pool,
        prep_sample_idx=prep_sample_idx,
        processing_idx=processing_idx,
        workspace=workspace,
        data_plane_url=data_plane_url,
        signing_key=signing_key,
    )
    return bindings
