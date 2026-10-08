"""Tests for `ena_import.harmonization.build_biosample_metadata`."""

from pathlib import Path

from qiita_control_plane import ena_import
from qiita_control_plane.ena_import.harmonization import build_biosample_metadata


def test_build_biosample_metadata_empty_input_marks_host_taxon_id_unknown():
    global_metadata, local_metadata, result = build_biosample_metadata({})

    assert global_metadata == {"host taxon id": "not provided"}
    assert local_metadata == {}
    assert result.mapped_count == 0


def test_build_biosample_metadata_single_values_map_as_before():
    global_metadata, local_metadata, result = build_biosample_metadata(
        {"depth": ["10"], "collection_date": ["2019-06-01"], "host": ["Homo sapiens"]}
    )

    assert global_metadata == {
        "depth": "10",
        "collection date": "2019-06-01",
        "host taxon id": "not provided",
    }
    assert local_metadata == {"host": "Homo sapiens"}
    assert result.mapped_count == 2


def test_build_biosample_metadata_multi_valued_tags_never_reach_a_handler():
    global_metadata, local_metadata, result = build_biosample_metadata(
        {
            "depth": ["10", "5"],
            "collection_date": ["2017", "2019-06-01"],
            "geo_loc_name": ["Argentina", "Brazil"],
            "BioSampleModel": ["MIMARKS.survey", "MIGS/MIMS/MIMARKS.human-associated"],
            "ENA-FIRST-PUBLIC": ["2024-12-13", "2024-12-13T01:09:31Z"],
        }
    )

    assert global_metadata == {"host taxon id": "not provided"}
    assert local_metadata == {
        "depth": '["10", "5"]',
        "collection_date": '["2017", "2019-06-01"]',
        "geo_loc_name": '["Argentina", "Brazil"]',
        "BioSampleModel": '["MIGS/MIMS/MIMARKS.human-associated", "MIMARKS.survey"]',
        "ENA-FIRST-PUBLIC": '["2024-12-13", "2024-12-13T01:09:31Z"]',
    }
    assert result.mapped_count == 0


def test_build_biosample_metadata_json_keeps_non_ascii_unescaped():
    _, local_metadata, _ = build_biosample_metadata({"site": ["Z\u00fcrich", "S\u00e3o Paulo"]})

    assert local_metadata == {"site": '["S\u00e3o Paulo", "Z\u00fcrich"]'}


def test_ena_import_source_has_no_bare_host_taxon_id_literal():
    """The display name is written through its constant, never a literal."""
    package_dir = Path(ena_import.__file__).parent
    offenders = [p.name for p in package_dir.glob("*.py") if '"host taxon id"' in p.read_text()]
    assert not offenders
