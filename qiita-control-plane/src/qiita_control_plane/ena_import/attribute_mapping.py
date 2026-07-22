"""ENA sample-attribute tag -> `biosample_global_field.display_name` mapping
(T03, owner decision D-A: conservative). `ena_import.harmonization` uses this
to split one BioSample's submitter-defined ENA attributes into a curated set
that lands on an existing global field (cross-study comparable) and
everything else, which is retained as study-local metadata rather than
dropped.

Deliberately conservative -- covers only the directly-ingestible ENA/MIxS
attribute tags that map onto an existing biosample_global_field with no
ontology-term resolution or other semantic reinterpretation. Two vocabularies
are recognized for the same underlying concepts, since real submitters use
both: the GSC-MIxS **display-name** form ENA's own checklists document
(`"collection date"`, `"geographic location (country and/or sea)"`, ...) and
the **underscore MIxS short-name** form real submitters (notably DDBJ) often
use instead (`collection_date`, `geo_loc_name`, `lat_lon`, `depth`). Both
forms are recognized side by side -- neither supersedes the other:

  - `collection date` / `collection_date` (TEXT -- rebound from DATE by
    `20260616000000_collection_date_text.sql` to hold ISO8601 partials/bare
    years and INSDC missing-value strings verbatim) and the two
    geographic-location coordinate fields plus the country/sea field, all
    seeded by `db/migrations/20260501000007_biosample_field.sql`: value
    passes through unchanged.
  - `depth` (NUMERIC), seeded by
    `db/migrations/20260709000000_seed_water_metadata_terminology.sql`:
    value passes through unchanged (same tag in both vocabularies).
  - `geo_loc_name` -> `geographic location (country and/or sea)`: MIxS's
    `geo_loc_name` packs `country:region:locality` into one string; only the
    country/sea part (the substring before the first `:`, or the whole
    string if there is no `:`) is extracted -- region/locality are dropped
    rather than guessed at a finer granularity this field doesn't model.
  - `lat_lon` -> SPLIT into `geographic location (latitude)` AND
    `geographic location (longitude)`: MIxS's `lat_lon` packs both
    coordinates plus their hemisphere into one string
    (`"<lat> <N|S> <lon> <E|W>"`, e.g. `"35.6895 N 139.6917 E"`). Parsed and
    signed (S/W negate); a value that doesn't match this exact shape --
    including an INSDC missing-value marker like `"missing"`, which cannot
    be split into two numbers without guessing -- is left UNMAPPED (retained
    as raw local metadata under the original combined tag) rather than
    guessed at or crashed on.

Deliberately NOT mapped here (owner decisions, reconciled harmonization plan):

  - `host` -- ENA's `host` attribute is free-text (a common/scientific name),
    not an NCBI Taxonomy id. Mapping it onto `host_taxon_id`
    (terminology-typed) would fabricate a term-id resolution this ticket does
    not own.
  - any tag that would resolve `taxon_id` / `host_taxon_id` -- both stay
    unmapped so a real taxon-resolution ticket can own that deliberately;
    this ticket never invents one.
  - the three GSC-MIxS environmental-context tags (broad-scale / local /
    medium environmental context, in EITHER vocabulary --
    `env_broad_scale`/`env_local_scale`/`env_medium` is the underscore form
    of the same three) -- SAME principle as the taxon exclusion above,
    extended on discovery: `20260608000001_seed_envo_terminology.sql`
    rebound all three from TEXT to TERMINOLOGY (bound to ENVO), predating
    this ticket. A TERMINOLOGY-typed field resolves its value against
    `terminology_term.term_id` (an ENVO CURIE like `ENVO:00000447`), not a
    free-text label -- ENA's raw attribute value for these tags is
    submitter free text (`"marine biome"`, `"sea water"`, ...), not
    necessarily a CURIE. Mapping it directly would either fabricate an
    ENVO-resolution step this ticket does not own or fail to parse for most
    real values; both are worse than retaining the raw text as study-local
    metadata (T03-1), so these three tags stay unmapped here too.

Normalization (whitespace-collapsed, lower-cased, underscore-folded-to-space)
only smooths a submitter's spacing/casing/vocabulary-punctuation quirk; the
lookup table itself is exact-match on the normalized form, never
fuzzy/partial -- a tag this table does not recognize is retained as
study-local metadata (T03-1), never silently dropped or guessed at.
"""

from __future__ import annotations

import re
from collections.abc import Callable

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_tag(tag: str) -> str:
    """Lower-case, fold underscores to spaces, and collapse internal
    whitespace so a submitter's spacing/casing quirk (`"Collection Date"`,
    `"collection  date"`) AND the underscore MIxS short-name vocabulary
    (`"collection_date"`) both hit the same exact-match table entry as the
    GSC-MIxS display-name form (`"collection date"`)."""
    folded = tag.strip().lower().replace("_", " ")
    return _WHITESPACE_RE.sub(" ", folded)


# One handler per normalized tag. Each takes the attribute's raw (untouched)
# value and returns the {global_field_display_name: value} entries to merge
# into `mapped`, or `None` if this particular value can't be safely mapped --
# in which case the ORIGINAL tag+value is retained as unmapped (T03-1),
# never split or guessed at.
_TagHandler = Callable[[str], dict[str, str] | None]


def _passthrough(display_name: str) -> _TagHandler:
    """Most tags map 1:1 onto an existing global field with no value
    transform -- both the DISPLAY-NAME (this table's normal keys) and the
    handful of underscore-vocabulary tags that are just a differently-
    spelled synonym for the same field (`depth`)."""

    def _handler(value: str) -> dict[str, str]:
        return {display_name: value}

    return _handler


def _map_geo_loc_name(value: str) -> dict[str, str]:
    """MIxS `geo_loc_name` is `country:region:locality`; only the
    country/sea part (before the first `:`) lands on
    `geographic location (country and/or sea)` -- region/locality have no
    corresponding field and are dropped rather than guessed at. A value
    with no `:` (or an INSDC missing-value marker) passes through whole;
    downstream `known_missing_reasons` wiring in `harmonization.py` already
    resolves a missing-value marker on a mapped field, so no special-casing
    is needed here."""
    country = value.split(":", 1)[0] if ":" in value else value
    return {"geographic location (country and/or sea)": country.strip()}


# MIxS `lat_lon`: "<lat> <N|S> <lon> <E|W>", e.g. "35.6895 N 139.6917 E".
# Deliberately does not accept a bare "-33.8, 151.2" or any other shape --
# an unrecognized format either falls through to unmapped or (if it looks
# INSDC-missing, e.g. "missing"/"not collected") is indistinguishable from
# "unparseable" here, which is the point: split the tag into two NUMERIC
# fields only when both numbers are unambiguous.
_LAT_LON_RE = re.compile(
    r"^(?P<lat>\d+(?:\.\d+)?)\s+(?P<lat_dir>[NSns])\s+"
    r"(?P<lon>\d+(?:\.\d+)?)\s+(?P<lon_dir>[EWew])$"
)


def _map_lat_lon(value: str) -> dict[str, str] | None:
    """Split MIxS `lat_lon` into the two existing NUMERIC coordinate
    fields, negating for S/W. Returns `None` (leave the combined tag
    unmapped, retained as raw local metadata) for anything that doesn't
    match the exact `"<lat> <N|S> <lon> <E|W>"` shape -- including an
    INSDC missing-value marker like `"missing"`, which cannot be split
    into two numbers without guessing which half is "missing." The
    original numeric substrings are preserved verbatim (sign-prefixed, not
    re-formatted through `float`) so submitted precision survives exactly.
    """
    match = _LAT_LON_RE.match(value.strip())
    if match is None:
        return None
    lat = match["lat"] if match["lat_dir"].upper() == "N" else f"-{match['lat']}"
    lon = match["lon"] if match["lon_dir"].upper() == "E" else f"-{match['lon']}"
    return {
        "geographic location (latitude)": lat,
        "geographic location (longitude)": lon,
    }


# Curated ENA/MIxS attribute tag (normalized: lower-cased, underscore-folded,
# whitespace-collapsed) -> handler. Every display_name a handler returns
# names a global field a migration has already seeded (see module
# docstring) -- this table does not itself create the target global field.
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

    `mapped` is keyed by `biosample_global_field.display_name` (never
    `internal_name`) so it composes directly with
    `qiita_control_plane.repositories._sample_helpers.preflight_global_metadata`
    and its siblings, which key on `display_name`. `unmapped` is keyed by the
    ORIGINAL (un-normalized) tag as submitted -- `ena_import.harmonization`
    writes it as the `display_name` of a purely-local `biosample_study_field`
    so the value is retained rather than dropped (T03-1).

    A recognized tag whose handler declines to map its particular value
    (currently only `lat_lon` on an unparseable/missing value) is retained
    as unmapped under its original tag, exactly like an unrecognized tag --
    never split, dropped, or guessed at.

    Two distinct raw tags whose normalized forms collide (e.g. differing only
    by case, whitespace, or underscore-vs-space) are not expected in one
    sample's attribute dict -- `EnaSampleAttributes.attributes` is already
    keyed by the raw tag -- so no de-dup is performed here; the later one
    wins in `mapped`.
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
