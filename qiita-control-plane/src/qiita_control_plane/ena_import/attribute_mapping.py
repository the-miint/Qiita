"""ENA sample-attribute tag -> `biosample_global_field.display_name` mapping
(conservative by design). `ena_import.harmonization` uses this to split one BioSample's
ENA attributes into a curated set that lands on an existing global field (cross-study
comparable) and everything else, retained as study-local metadata rather than dropped.

Covers only tags that map onto an existing global field with no ontology-term
resolution or semantic reinterpretation. Two vocabularies are recognized side by side,
since real submitters use both: the GSC-MIxS display-name form ENA's checklists document
(`"collection date"`, ...) and the underscore MIxS short-name form (notably DDBJ) uses
(`collection_date`, `geo_loc_name`, `lat_lon`, `depth`).

  - `collection date`/`collection_date`, `depth`, and the geographic-location
    coordinate + country/sea fields: value passes through unchanged.
  - `geo_loc_name` -> `geographic location (country and/or sea)`: MIxS packs
    `country:region:locality`; only the country/sea part (before the first `:`) is
    kept -- region/locality dropped rather than guessed at a finer granularity.
  - `lat_lon` -> SPLIT into latitude AND longitude fields. Packs both coords + hemisphere
    (`"<lat> <N|S> <lon> <E|W>"`); parsed and signed (S/W negate). A value not matching
    that exact shape (including an INSDC missing marker) is left UNMAPPED, not guessed.

Deliberately NOT mapped: `host` (free-text name, not an NCBI Taxonomy id), anything
resolving `taxon_id`/`host_taxon_id`, and the three GSC-MIxS environmental-context tags
(`env_broad_scale`/`env_local_scale`/`env_medium`). The last are TERMINOLOGY-typed
(bound to ENVO), resolving against an ENVO CURIE, but ENA's raw values are submitter free
text -- mapping directly would fabricate an ENVO-resolution step this ticket doesn't own.
All stay unmapped so a real taxon/ENVO-resolution ticket can own them.

Normalization (whitespace-collapsed, lower-cased, underscore-folded-to-space) only
smooths spacing/casing/vocabulary quirks; the lookup is exact-match on the normalized
form -- an unrecognized tag is retained as study-local metadata, never dropped or guessed.
"""

from __future__ import annotations

import re
from collections.abc import Callable

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_tag(tag: str) -> str:
    """Lower-case, fold underscores to spaces, and collapse whitespace so spacing/casing
    quirks and the underscore MIxS short-name (`"collection_date"`) hit the same
    exact-match entry as the display-name form (`"collection date"`)."""
    folded = tag.strip().lower().replace("_", " ")
    return _WHITESPACE_RE.sub(" ", folded)


# One handler per normalized tag. Takes the raw value and returns the
# {display_name: value} entries to merge into `mapped`, or `None` if the value can't be
# safely mapped -- in which case the ORIGINAL tag+value is retained as unmapped.
_TagHandler = Callable[[str], dict[str, str] | None]


def _passthrough(display_name: str) -> _TagHandler:
    """Most tags map 1:1 onto an existing global field with no value transform."""

    def _handler(value: str) -> dict[str, str]:
        return {display_name: value}

    return _handler


def _map_geo_loc_name(value: str) -> dict[str, str]:
    """MIxS `geo_loc_name` is `country:region:locality`; only the country/sea part
    (before the first `:`) lands on `geographic location (country and/or sea)` --
    region/locality are dropped rather than guessed at. A value with no `:` (or an INSDC
    missing marker) passes through whole; `harmonization.py`'s `known_missing_reasons`
    wiring already resolves a missing marker on a mapped field."""
    country = value.split(":", 1)[0] if ":" in value else value
    return {"geographic location (country and/or sea)": country.strip()}


# MIxS `lat_lon`: "<lat> <N|S> <lon> <E|W>", e.g. "35.6895 N 139.6917 E". Deliberately
# rejects any other shape (bare "-33.8, 151.2", INSDC-missing markers): split into two
# NUMERIC fields only when both numbers are unambiguous.
_LAT_LON_RE = re.compile(
    r"^(?P<lat>\d+(?:\.\d+)?)\s+(?P<lat_dir>[NSns])\s+"
    r"(?P<lon>\d+(?:\.\d+)?)\s+(?P<lon_dir>[EWew])$"
)


def _map_lat_lon(value: str) -> dict[str, str] | None:
    """Split MIxS `lat_lon` into the two NUMERIC coordinate fields, negating for S/W.
    Returns `None` (leave the tag unmapped) for anything not matching the exact
    `"<lat> <N|S> <lon> <E|W>"` shape, including INSDC missing markers. Numeric
    substrings are preserved verbatim (sign-prefixed, not re-formatted through `float`)
    so submitted precision survives exactly."""
    match = _LAT_LON_RE.match(value.strip())
    if match is None:
        return None
    lat = match["lat"] if match["lat_dir"].upper() == "N" else f"-{match['lat']}"
    lon = match["lon"] if match["lon_dir"].upper() == "E" else f"-{match['lon']}"
    return {
        "geographic location (latitude)": lat,
        "geographic location (longitude)": lon,
    }


# Curated normalized ENA/MIxS attribute tag -> handler. Every display_name a handler
# returns names a global field a migration already seeded; this table doesn't create it.
_ENA_ATTRIBUTE_TAG_HANDLERS: dict[str, _TagHandler] = {
    "collection date": _passthrough("collection date"),
    "geographic location (country and/or sea)": _passthrough(
        "geographic location (country and/or sea)"
    ),
    "geographic location (latitude)": _passthrough("geographic location (latitude)"),
    "geographic location (longitude)": _passthrough("geographic location (longitude)"),
    "depth": _passthrough("depth"),
    "geo loc name": _map_geo_loc_name,
    "lat lon": _map_lat_lon,
}


def map_ena_attributes(attributes: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    """Split one BioSample's ENA attribute map into `(mapped, unmapped)`.

    `mapped` is keyed by `biosample_global_field.display_name` (never `internal_name`)
    so it composes directly with the `_sample_helpers` preflight functions, which key on
    `display_name`. `unmapped` is keyed by the ORIGINAL (un-normalized) tag --
    `ena_import.harmonization` writes it as a purely-local `biosample_study_field` so the
    value is retained rather than dropped.

    A recognized tag whose handler declines its value (currently only `lat_lon` on an
    unparseable/missing value) is retained as unmapped, like an unrecognized tag.
    Normalized-form collisions in one sample aren't expected (attributes are keyed by raw
    tag), so no de-dup is done; the later one wins in `mapped`.
    """
    mapped: dict[str, str] = {}
    unmapped: dict[str, str] = {}
    for tag, value in attributes.items():
        handler = _ENA_ATTRIBUTE_TAG_HANDLERS.get(_normalize_tag(tag))
        result = handler(value) if handler is not None else None
        if result is None:
            unmapped[tag] = value
        else:
            mapped.update(result)
    return mapped, unmapped
