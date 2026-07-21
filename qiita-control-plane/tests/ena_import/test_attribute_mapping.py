"""Tests for `ena_import.attribute_mapping.map_ena_attributes` (T03, owner
decision D-A): a curated, conservative ENA sample-attribute tag ->
`biosample_global_field.display_name` mapping. Split output — mapped
(display_name -> value) vs. unmapped (raw tag -> value) — never drops a
tag; normalization is whitespace/case-insensitive but the lookup table
itself is exact-match, never fuzzy.
"""

from qiita_control_plane.ena_import.attribute_mapping import map_ena_attributes


def test_map_ena_attributes_maps_known_tags_to_display_names():
    attributes = {
        "collection date": "2019-06-01",
        "geographic location (country and/or sea)": "USA: California",
        "geographic location (latitude)": "32.88",
        "geographic location (longitude)": "-117.24",
        "depth": "10.5",
    }

    mapped, unmapped = map_ena_attributes(attributes)

    assert mapped == attributes
    assert unmapped == {}


def test_map_ena_attributes_is_case_and_whitespace_insensitive():
    attributes = {
        "  Collection Date  ": "2019-06-01",
        "GEOGRAPHIC LOCATION (COUNTRY AND/OR SEA)": "USA: California",
        "Depth": "10.5",
    }

    mapped, unmapped = map_ena_attributes(attributes)

    assert mapped == {
        "collection date": "2019-06-01",
        "geographic location (country and/or sea)": "USA: California",
        "depth": "10.5",
    }
    assert unmapped == {}


def test_map_ena_attributes_leaves_host_unmapped():
    """`host` is free-text (a common/scientific name), not an NCBI taxon id --
    mapping it onto `host_taxon_id` (terminology-typed) would fabricate a
    term-id resolution this ticket does not own (owner decision: taxon)."""
    attributes = {"host": "Homo sapiens"}

    mapped, unmapped = map_ena_attributes(attributes)

    assert mapped == {}
    assert unmapped == {"host": "Homo sapiens"}


def test_map_ena_attributes_leaves_taxon_id_tags_unmapped():
    attributes = {"taxon_id": "9606", "host taxid": "9606"}

    mapped, unmapped = map_ena_attributes(attributes)

    assert mapped == {}
    assert unmapped == attributes


def test_map_ena_attributes_leaves_environmental_context_triad_unmapped():
    """broad-scale/local/medium environmental context are TERMINOLOGY-typed
    (bound to ENVO by `20260608000001_seed_envo_terminology.sql`); mapping
    ENA's free-text value directly onto them would require an ENVO-CURIE
    resolution this ticket does not own (same principle as the taxon
    exclusion) -- so they stay unmapped, retained as raw local metadata."""
    attributes = {
        "broad-scale environmental context": "marine biome",
        "local environmental context": "coastal water",
        "environmental medium": "sea water",
    }

    mapped, unmapped = map_ena_attributes(attributes)

    assert mapped == {}
    assert unmapped == attributes


def test_map_ena_attributes_retains_unrecognized_tag_raw():
    attributes = {"strain": "GA17570", "ENA-FIRST-PUBLIC": "2011-01-25"}

    mapped, unmapped = map_ena_attributes(attributes)

    assert mapped == {}
    assert unmapped == attributes


def test_map_ena_attributes_splits_a_mixed_dict():
    attributes = {
        "collection date": "2019-06-01",
        "depth": "10.5",
        "host": "Homo sapiens",
        "strain": "GA17570",
    }

    mapped, unmapped = map_ena_attributes(attributes)

    assert mapped == {"collection date": "2019-06-01", "depth": "10.5"}
    assert unmapped == {"host": "Homo sapiens", "strain": "GA17570"}


def test_map_ena_attributes_empty_input_returns_empty_splits():
    mapped, unmapped = map_ena_attributes({})
    assert mapped == {}
    assert unmapped == {}
