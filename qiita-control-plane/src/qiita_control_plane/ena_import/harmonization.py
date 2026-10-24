"""ENA sample attributes and the run's taxon ids -> the biosample import's metadata dict.

`attribute_mapping.map_ena_attributes` splits one BioSample's attributes into a
curated set landing on a `biosample_global_field` (cross-study comparable) and
everything else, retained as study-local rather than dropped. The run's `tax_id` and
`host_tax_id`, checked against the curated table in `host_by_sample_taxon`, fill the
taxon fields. The halves go to
`repositories.biosample.resolve_or_import_biosample_by_ena_accession` as two
separate dicts; this module holds no SQL.
"""

from __future__ import annotations

import json
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field

from qiita_common.models import (
    BIOSAMPLE_DISPLAY_HOST_TAXON_ID,
    BIOSAMPLE_DISPLAY_TAXON_ID,
    MISSING_REASON_NOT_APPLICABLE,
    MISSING_REASON_NOT_PROVIDED,
    FieldDataType,
)
from qiita_common.models.ena import EnaRunRecord

from qiita_control_plane.host_by_sample_taxon import NON_HOST_TAXA
from qiita_control_plane.repositories._sample_helpers import (
    MetadataParseError,
    SampleEntityKind,
    coordinate_degree_bound,
    parse_text_for_data_type,
)

from .attribute_mapping import map_ena_attributes

_TAXON_DISPLAY_NAMES = frozenset({BIOSAMPLE_DISPLAY_HOST_TAXON_ID, BIOSAMPLE_DISPLAY_TAXON_ID})


@dataclass(frozen=True)
class HarmonizationResult:
    """One biosample's harmonization outcome.

    `mapped_count`: attributes written as globally-linked metadata.
    `retained_unmapped`: the study-local fields written: raw ENA tags (not dropped),
    and `<field> (as submitted)` for a mapped value its global field could not hold.
    Such a field still counts in `mapped_count`; its global slot holds `not provided`.
    `warnings`: taxon fields written as `not provided`, mapped values kept
    `(as submitted)`, or ENA data that disagrees.
    """

    mapped_count: int
    retained_unmapped: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# INSDC missing-value terms, plus `missing` and `unknown`, which ENA host text also uses.
_MISSING_HOST_TEXT = frozenset(
    {
        "missing",
        MISSING_REASON_NOT_APPLICABLE,
        "not collected",
        MISSING_REASON_NOT_PROVIDED,
        "restricted access",
        "unknown",
    }
)
_HOST_TEXT_MAX = 80


def _host_text(run: EnaRunRecord) -> str | None:
    """ENA's free-text `host` when it names something, not when it is a missing-value term."""
    if run.host is None:
        return None
    term = run.host.strip().lower()
    if term in _MISSING_HOST_TEXT or term.startswith("missing:"):
        return None
    return run.host.strip()


def _host_taxon_id(
    run: EnaRunRecord,
    implied_hosts: Mapping[str, str | None],
    loaded_term_ids: Collection[str],
) -> tuple[str, list[str]]:
    who = run.sample_accession
    warnings: list[str] = []
    implied = run.tax_id in implied_hosts
    implied_host = implied_hosts.get(run.tax_id)

    host_tax_id = run.host_tax_id
    if host_tax_id is not None and host_tax_id == run.tax_id:
        warnings.append(
            f"{who}: host_tax_id {host_tax_id} is the biosample's own taxon, not a host"
        )
        host_tax_id = None
    elif host_tax_id is not None and host_tax_id in NON_HOST_TAXA:
        warnings.append(f"{who}: host_tax_id {host_tax_id} names an environment, not a host")
        host_tax_id = None

    if host_tax_id is not None:
        if implied and implied_host != host_tax_id:
            warnings.append(
                f"{who}: ENA host_tax_id {host_tax_id} differs from host"
                f" {implied_host or 'none'} implied by tax_id {run.tax_id}"
            )
        if host_tax_id in loaded_term_ids:
            return host_tax_id, warnings
        warnings.append(f"{who}: host_tax_id {host_tax_id} is not a loaded NCBI Taxonomy term")
        return MISSING_REASON_NOT_PROVIDED, warnings

    text = _host_text(run)
    if text is not None:
        shown = text if len(text) <= _HOST_TEXT_MAX else text[:_HOST_TEXT_MAX] + "..."
        warnings.append(f"{who}: ENA gives host text {shown!r} but no usable host_tax_id")
        return MISSING_REASON_NOT_PROVIDED, warnings

    if implied and implied_host is None:
        return MISSING_REASON_NOT_APPLICABLE, warnings
    if implied_host is not None:
        if implied_host in loaded_term_ids:
            return implied_host, warnings
        warnings.append(
            f"{who}: host {implied_host} implied by tax_id {run.tax_id} is not a loaded"
            " NCBI Taxonomy term"
        )
        return MISSING_REASON_NOT_PROVIDED, warnings

    reason = (
        "ENA gives no tax_id"
        if run.tax_id is None
        else f"the table has no row for tax_id {run.tax_id}"
    )
    warnings.append(f"{who}: ENA gives no host_tax_id and {reason}")
    return MISSING_REASON_NOT_PROVIDED, warnings


def _taxon_id(run: EnaRunRecord, loaded_term_ids: Collection[str]) -> tuple[str, list[str]]:
    if run.tax_id is None:
        return MISSING_REASON_NOT_PROVIDED, [f"{run.sample_accession}: ENA gives no tax_id"]
    if run.tax_id in loaded_term_ids:
        return run.tax_id, []
    return MISSING_REASON_NOT_PROVIDED, [
        f"{run.sample_accession}: tax_id {run.tax_id} is not a loaded NCBI Taxonomy term"
    ]


def build_biosample_metadata(
    attributes: dict[str, list[str]],
    *,
    ena_run: EnaRunRecord,
    implied_hosts: Mapping[str, str | None],
    loaded_term_ids: Collection[str],
) -> tuple[dict[str, str], dict[str, str], HarmonizationResult]:
    """Split one BioSample's ENA attributes into `(global_metadata,
    local_metadata, result)` for the import.

    The two dicts cannot be merged: an unmapped tag is often spelled exactly
    like a global field the mapping declined (ENA's environmental-context tags
    are), and the import resolves any key naming a global to that global.

    A tag with several values never reaches a typed handler: it is kept study-local as
    a JSON array. Only taxon ids in `loaded_term_ids` are written; anything else is
    `not provided` with a warning. A tag named like a taxon field is dropped with a warning.
    """
    mapped, unmapped = map_ena_attributes(
        {tag: values[0] for tag, values in attributes.items() if len(values) == 1}
    )
    unmapped.update(
        {
            tag: json.dumps(sorted(values), ensure_ascii=False)
            for tag, values in attributes.items()
            if len(values) != 1
        }
    )
    skipped = sorted(unmapped.keys() & _TAXON_DISPLAY_NAMES)
    for tag in skipped:
        del unmapped[tag]
    attribute_warnings = [
        f"{ena_run.sample_accession}: attribute {tag!r} skipped, taxon ids come from the run"
        for tag in skipped
    ]
    host, host_warnings = _host_taxon_id(ena_run, implied_hosts, loaded_term_ids)
    taxon, taxon_warnings = _taxon_id(ena_run, loaded_term_ids)
    global_metadata = {
        **mapped,
        BIOSAMPLE_DISPLAY_HOST_TAXON_ID: host,
        BIOSAMPLE_DISPLAY_TAXON_ID: taxon,
    }
    return (
        global_metadata,
        dict(unmapped),
        HarmonizationResult(
            mapped_count=len(mapped),
            retained_unmapped=sorted(unmapped),
            warnings=host_warnings + taxon_warnings + attribute_warnings,
        ),
    )


# The study-local field a mapped value its global field cannot hold is kept under: a
# local field may not share a global's name within one study.
AS_SUBMITTED_SUFFIX = " (as submitted)"


def retain_unparseable_as_submitted(
    metadata: Mapping[str, str],
    local_metadata: Mapping[str, str],
    *,
    data_types: Mapping[str, FieldDataType],
    internal_names: Mapping[str, str | None],
    missing_reasons: Collection[str],
    sample_accession: str,
) -> tuple[dict[str, str], dict[str, str], list[str]]:
    """Keep a mapped value its global field's type cannot hold (`"39 deg"` for a
    coordinate, a longitude of `"200"` past its degree bound, `"deep"` for a depth)
    instead of letting it fail the whole run.

    The global field gets `not provided` and the submitted text is kept study-local
    under `<field> (as submitted)`, with a warning. `internal_names` carries each
    field's global internal_name, so a coordinate is range-checked (past ±90/±180 is
    unparseable), not just type-checked. A missing-value reason the import recognizes
    (`missing_reasons`) is left as is, and so is a field absent from `data_types` or a
    TERMINOLOGY field, whose values the import resolves itself.

    A blank value has no submitted text worth keeping, so it stores `not provided`
    alone, with no `(as submitted)` field. When the `(as submitted)` name is already a
    raw ENA tag, keeping both would overwrite one, so this raises a `MetadataParseError`
    naming the taken field (the caller folds it into a `failed` outcome). Returns
    `(metadata, local_metadata, warnings)`.
    """
    kept = dict(metadata)
    local = dict(local_metadata)
    warnings: list[str] = []
    for display_name, value in metadata.items():
        data_type = data_types.get(display_name)
        if (
            data_type is None
            or data_type is FieldDataType.TERMINOLOGY
            or value.strip() in missing_reasons
        ):
            continue
        degree_bound = coordinate_degree_bound(
            SampleEntityKind.BIOSAMPLE, internal_names.get(display_name)
        )
        try:
            parse_text_for_data_type(display_name, data_type, value, degree_bound=degree_bound)
        except MetadataParseError as exc:
            if not value.strip():
                kept[display_name] = MISSING_REASON_NOT_PROVIDED
                warnings.append(
                    f"{sample_accession}: {display_name!r} is blank;"
                    f" stored {MISSING_REASON_NOT_PROVIDED!r}"
                )
                continue
            local_name = f"{display_name}{AS_SUBMITTED_SUFFIX}"
            if local_name in local:
                raise MetadataParseError(
                    field_key=display_name,
                    data_type=data_type,
                    text_value=value,
                    reason=f"{exc.reason}; cannot keep the submitted text as"
                    f" {local_name!r}, already a submitted attribute",
                ) from exc
            kept[display_name] = MISSING_REASON_NOT_PROVIDED
            local[local_name] = value
            warnings.append(
                f"{sample_accession}: {display_name!r} value {value!r} cannot be stored as"
                f" {data_type} ({exc.reason}); stored {MISSING_REASON_NOT_PROVIDED!r} and"
                f" kept the submitted text as {local_name!r}"
            )
    return kept, local, warnings
