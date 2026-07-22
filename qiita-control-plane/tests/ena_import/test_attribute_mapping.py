"""Tests for `ena_import.attribute_mapping.map_ena_attributes` (conservative
by design): a curated ENA sample-attribute tag ->
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
    term-id resolution this ticket does not own (deliberately out of scope: taxon)."""
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


# ---------------------------------------------------------------------------
# Underscore MIxS vocabulary (real DDBJ/submitter shapes, live-ingestion find)
# ---------------------------------------------------------------------------


def test_map_ena_attributes_underscore_collection_date_maps_like_display_name():
    mapped, unmapped = map_ena_attributes({"collection_date": "2021-11-15"})

    assert mapped == {"collection date": "2021-11-15"}
    assert unmapped == {}


def test_map_ena_attributes_underscore_depth_maps_like_display_name():
    mapped, unmapped = map_ena_attributes({"depth": "10"})

    assert mapped == {"depth": "10"}
    assert unmapped == {}


def test_map_ena_attributes_geo_loc_name_extracts_country_before_colon():
    # Real observed shape: PRJDB40386's SAMD01820063.
    mapped, unmapped = map_ena_attributes(
        {"geo_loc_name": "Japan:Shinga, Ritsumeikan University BKC"}
    )

    assert mapped == {"geographic location (country and/or sea)": "Japan"}
    assert unmapped == {}


def test_map_ena_attributes_geo_loc_name_without_colon_uses_whole_string():
    mapped, unmapped = map_ena_attributes({"geo_loc_name": "USA"})

    assert mapped == {"geographic location (country and/or sea)": "USA"}
    assert unmapped == {}


def test_map_ena_attributes_geo_loc_name_strips_whitespace_around_colon():
    mapped, unmapped = map_ena_attributes({"geo_loc_name": "Japan : Shiga"})

    assert mapped == {"geographic location (country and/or sea)": "Japan"}
    assert unmapped == {}


def test_map_ena_attributes_lat_lon_splits_into_latitude_and_longitude():
    # Real observed shape: "<lat> N|S <lon> E|W" (Tokyo, N/E -- both positive).
    mapped, unmapped = map_ena_attributes({"lat_lon": "35.6895 N 139.6917 E"})

    assert mapped == {
        "geographic location (latitude)": "35.6895",
        "geographic location (longitude)": "139.6917",
    }
    assert unmapped == {}


def test_map_ena_attributes_lat_lon_negates_south_and_west():
    mapped, unmapped = map_ena_attributes({"lat_lon": "33.8 S 151.2 E"})

    assert mapped == {
        "geographic location (latitude)": "-33.8",
        "geographic location (longitude)": "151.2",
    }
    assert unmapped == {}


def test_map_ena_attributes_lat_lon_missing_value_stays_unmapped():
    """A combined lat_lon that is an INSDC missing-value marker can't be
    split into two numbers without guessing which half is "missing" --
    falls to local retention under the original tag, never guessed at."""
    mapped, unmapped = map_ena_attributes({"lat_lon": "missing"})

    assert mapped == {}
    assert unmapped == {"lat_lon": "missing"}


def test_map_ena_attributes_lat_lon_unparseable_stays_unmapped():
    mapped, unmapped = map_ena_attributes({"lat_lon": "not applicable"})

    assert mapped == {}
    assert unmapped == {"lat_lon": "not applicable"}


def test_map_ena_attributes_leaves_underscore_environmental_context_triad_unmapped():
    """Same ontology-resolution deferral as the display-name form -- the
    underscore short names are still TERMINOLOGY/ENVO-typed fields this
    ticket does not own resolving free text against."""
    attributes = {
        "env_broad_scale": "marine biome",
        "env_local_scale": "coastal water",
        "env_medium": "sea water",
    }

    mapped, unmapped = map_ena_attributes(attributes)

    assert mapped == {}
    assert unmapped == attributes


def test_map_ena_attributes_underscore_is_case_and_whitespace_insensitive():
    mapped, unmapped = map_ena_attributes({"  Geo_Loc_Name  ": "Japan:Shiga"})

    assert mapped == {"geographic location (country and/or sea)": "Japan"}
    assert unmapped == {}


def test_map_ena_attributes_both_vocabularies_coexist_when_targets_differ():
    """Display-name and underscore forms are additive -- both keep working,
    side by side, in the same attribute map, as long as they don't target
    the same global field (a submitter would not plausibly send both
    spellings of the same fact; that colliding case is covered by
    `map_ena_attributes`'s documented "later one wins" rule, not tested
    here)."""
    attributes = {
        "collection_date": "2021-11-15",
        "geographic location (latitude)": "32.88",
        "lat_lon": "35.6895 N 139.6917 E",
        "depth": "10",
    }

    mapped, unmapped = map_ena_attributes(attributes)

    assert unmapped == {}
    assert mapped == {
        "collection date": "2021-11-15",
        # "geographic location (latitude)" is overwritten by lat_lon's
        # split, processed later in dict-iteration order -- documents the
        # existing "later one wins" collision rule rather than special-
        # casing it.
        "geographic location (latitude)": "35.6895",
        "geographic location (longitude)": "139.6917",
        "depth": "10",
    }
