"""ENA sample-attribute tag -> `biosample_global_field.display_name` mapping
(T03, owner decision D-A: conservative). `ena_import.harmonization` uses this
to split one BioSample's submitter-defined ENA attributes into a curated set
that lands on an existing global field (cross-study comparable) and
everything else, which is retained as study-local metadata rather than
dropped.

Deliberately conservative -- covers only the directly-ingestible ENA/MIxS
attribute tags that map onto an existing biosample_global_field with no unit
conversion, ontology-term resolution, or other semantic reinterpretation:

  - `collection date` (TEXT -- rebound from DATE by
    `20260616000000_collection_date_text.sql` to hold ISO8601 partials/bare
    years and INSDC missing-value strings verbatim) and the two
    geographic-location coordinate fields plus the country/sea field, all
    seeded by `db/migrations/20260501000007_biosample_field.sql`.
  - `depth` (NUMERIC), seeded by
    `db/migrations/20260709000000_seed_water_metadata_terminology.sql`.

Deliberately NOT mapped here (owner decisions, reconciled TASK-03 plan):

  - `host` -- ENA's `host` attribute is free-text (a common/scientific name),
    not an NCBI Taxonomy id. Mapping it onto `host_taxon_id`
    (terminology-typed) would fabricate a term-id resolution this ticket does
    not own.
  - any tag that would resolve `taxon_id` / `host_taxon_id` -- both stay
    unmapped so a real taxon-resolution ticket can own that deliberately;
    this ticket never invents one.
  - the three GSC-MIxS environmental-context tags (broad-scale / local /
    medium environmental context) -- SAME principle as the taxon exclusion
    above, extended on discovery: `20260608000001_seed_envo_terminology.sql`
    rebound all three from TEXT to TERMINOLOGY (bound to ENVO), predating
    this ticket. A TERMINOLOGY-typed field resolves its value against
    `terminology_term.term_id` (an ENVO CURIE like `ENVO:00000447`), not a
    free-text label -- ENA's raw attribute value for these tags is
    submitter free text (`"marine biome"`, `"sea water"`, ...), not
    necessarily a CURIE. Mapping it directly would either fabricate an
    ENVO-resolution step this ticket does not own or fail to parse for most
    real values; both are worse than retaining the raw text as study-local
    metadata (T03-1), so these three tags stay unmapped here too.

Normalization (whitespace-collapsed, lower-cased) only smooths a submitter's
spacing/casing quirk; the lookup table itself is exact-match, never
fuzzy/partial -- a tag this table does not recognize is retained as
study-local metadata (T03-1), never silently dropped or guessed at.
"""

from __future__ import annotations

import re

# Curated ENA/MIxS attribute tag (normalized: lower-cased, whitespace-
# collapsed) -> the biosample_global_field.display_name it lands on. Every
# value here names a display_name a migration has already seeded (see module
# docstring) -- this table does not itself create the target global field.
_ENA_ATTRIBUTE_TAG_TO_GLOBAL_FIELD_DISPLAY_NAME: dict[str, str] = {
    "collection date": "collection date",
    "geographic location (country and/or sea)": "geographic location (country and/or sea)",
    "geographic location (latitude)": "geographic location (latitude)",
    "geographic location (longitude)": "geographic location (longitude)",
    "depth": "depth",
}

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_tag(tag: str) -> str:
    """Lower-case + collapse internal whitespace so a submitter's spacing or
    casing quirk (`"Collection Date"`, `"collection  date"`) still matches
    the exact-match table above."""
    return _WHITESPACE_RE.sub(" ", tag.strip().lower())


def map_ena_attributes(attributes: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    """Split one BioSample's ENA attribute map into `(mapped, unmapped)`.

    `mapped` is keyed by `biosample_global_field.display_name` (never
    `internal_name`) so it composes directly with
    `qiita_control_plane.repositories._sample_helpers.preflight_global_metadata`
    and its siblings, which key on `display_name`. `unmapped` is keyed by the
    ORIGINAL (un-normalized) tag as submitted -- `ena_import.harmonization`
    writes it as the `display_name` of a purely-local `biosample_study_field`
    so the value is retained rather than dropped (T03-1).

    Two distinct raw tags whose normalized forms collide (e.g. differing only
    by case) are not expected in one sample's attribute dict --
    `EnaSampleAttributes.attributes` is already keyed by the raw tag -- so no
    de-dup is performed here; the later one wins in `mapped`.
    """
    mapped: dict[str, str] = {}
    unmapped: dict[str, str] = {}
    for tag, value in attributes.items():
        display_name = _ENA_ATTRIBUTE_TAG_TO_GLOBAL_FIELD_DISPLAY_NAME.get(_normalize_tag(tag))
        if display_name is None:
            unmapped[tag] = value
        else:
            mapped[display_name] = value
    return mapped, unmapped
