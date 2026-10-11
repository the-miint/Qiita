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
)
from qiita_common.models.ena import EnaRunRecord

from qiita_control_plane.host_by_sample_taxon import NON_HOST_TAXA

from .attribute_mapping import map_ena_attributes

_TAXON_DISPLAY_NAMES = frozenset({BIOSAMPLE_DISPLAY_HOST_TAXON_ID, BIOSAMPLE_DISPLAY_TAXON_ID})


@dataclass(frozen=True)
class HarmonizationResult:
    """One biosample's harmonization outcome.

    `mapped_count`: attributes written as globally-linked metadata.
    `retained_unmapped`: raw ENA tags written as study-local metadata (not
    dropped).
    `warnings`: taxon fields written as `not provided`, or ENA data that disagrees.
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
