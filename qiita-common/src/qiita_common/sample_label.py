"""The public sample label — one definition, shared by the label-map route and
the masked-read export's filename stem.

A published feature table cannot carry `prep_sample_idx`: those identifiers are
ours and mean nothing outside this system. The label is what replaces them, and
it has to be derivable for every sample, including ones that are not yet
submitted and ones that were never pooled. Hence three forms, in preference
order:

1. `ena_run_accession` — public and unique per sequenced sample, but NULL until
   submission succeeds.
2. `<biosample_accession>.<run>.<pool>.<prep_sample>` — the composite the
   masked-read export already names its files with (`pooled_sample_label`).
3. `<biosample_accession>.<prep_sample>` — `sequenced_sample.sequenced_pool_idx`
   is nullable, so form 2 is not always constructible.

Forms 2 and 3 end in `prep_sample_idx`, which is unique, so every form is unique
within a cohort. Consumers never need to parse a label to recover its parts: the
response ships the accessions and identifiers as columns of their own.
"""


def pooled_sample_label(
    *,
    biosample_accession: str,
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
    prep_sample_idx: int,
) -> str:
    """`<biosample_accession>.<run>.<pool>.<prep_sample>`.

    Single-sourced because the masked-read export names its per-sample output
    files with exactly this string: an export and the label map that ships
    beside a feature table must not disagree about what a sample is called.
    """
    return f"{biosample_accession}.{sequencing_run_idx}.{sequenced_pool_idx}.{prep_sample_idx}"


def compose_sample_label(
    *,
    prep_sample_idx: int,
    biosample_accession: str | None,
    ena_run_accession: str | None,
    sequencing_run_idx: int | None,
    sequenced_pool_idx: int | None,
) -> str:
    """The published label for one sample. See the module docstring for the three
    forms and why each exists.

    Raises ValueError rather than emitting a label with a `None` in it. Both
    conditions are unreachable through the route — it 422s a missing accession
    before composing, and its query reaches `sequencing_run_idx` through
    `sequenced_pool` so the pair is always co-present — which is the point: the
    raise catches a caller or a query that has gone wrong, not a user error.
    """
    if not biosample_accession:
        raise ValueError(
            f"prep_sample {prep_sample_idx} has no biosample_accession and cannot be labelled"
        )
    if (sequencing_run_idx is None) != (sequenced_pool_idx is None):
        raise ValueError(
            f"prep_sample {prep_sample_idx} has sequencing_run_idx"
            f" {sequencing_run_idx!r} with sequenced_pool_idx {sequenced_pool_idx!r};"
            " the run is reached through the pool, so they are co-present or both absent"
        )
    if ena_run_accession:
        return ena_run_accession
    if sequenced_pool_idx is None:
        return f"{biosample_accession}.{prep_sample_idx}"
    return pooled_sample_label(
        biosample_accession=biosample_accession,
        sequencing_run_idx=sequencing_run_idx,
        sequenced_pool_idx=sequenced_pool_idx,
        prep_sample_idx=prep_sample_idx,
    )
