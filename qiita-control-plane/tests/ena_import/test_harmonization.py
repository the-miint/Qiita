"""Tests for `ena_import.harmonization`: `build_biosample_metadata` and
`retain_unparseable_as_submitted`."""

import json
from pathlib import Path

import pytest
from qiita_common.models import MISSING_REASON_NOT_PROVIDED, FieldDataType
from qiita_common.models.biosample import (
    BIOSAMPLE_FIELD_GEOGRAPHIC_LOCATION_LATITUDE,
    BIOSAMPLE_FIELD_GEOGRAPHIC_LOCATION_LONGITUDE,
)
from qiita_common.models.ena import EnaRunRecord

from qiita_control_plane import ena_import
from qiita_control_plane.ena_import.harmonization import (
    build_biosample_metadata,
    retain_unparseable_as_submitted,
)
from qiita_control_plane.host_by_sample_taxon import implied_hosts
from qiita_control_plane.repositories._sample_helpers import MetadataParseError

FIXTURES = Path(__file__).parent / "fixtures"

# The seeded NCBI Taxonomy terms the fixtures touch; 749906 and 9823 are not seeded.
_LOADED = frozenset({"9606", "10090", "408170", "646099", "410661", "1561972"})


def _run(fixture: str, **overrides) -> EnaRunRecord:
    data = json.loads((FIXTURES / fixture).read_text())
    row = dict(zip(data["columns"], data["rows"][0], strict=True))
    return EnaRunRecord(**{**row, **overrides})


def _build(run: EnaRunRecord, attributes: dict[str, list[str]] | None = None):
    taxa = [t for t in (run.tax_id,) if t]
    return build_biosample_metadata(
        attributes or {},
        ena_run=run,
        implied_hosts=implied_hosts(taxa),
        loaded_term_ids=_LOADED,
    )


_HUMAN_GUT = _run("ena_runs_human_gut.json")


def _fields(run: EnaRunRecord):
    global_metadata, _, result = _build(run)
    return global_metadata["host taxon id"], global_metadata["taxon id"], result.warnings


def test_human_gut_run_takes_host_and_taxon_from_ena():
    assert _fields(_run("ena_runs_human_gut.json")) == ("9606", "408170", [])


def test_blank_host_tax_id_falls_back_to_the_table_without_a_warning():
    run = _run("ena_runs_human_gut.json", host_tax_id="", host="")
    assert _fields(run) == ("9606", "408170", [])


def test_hostless_environment_is_not_applicable_and_not_reported():
    assert _fields(_run("ena_runs_seawater.json")) == ("not applicable", "1561972", [])


def test_unloaded_host_tax_id_is_not_provided_and_does_not_fall_back_to_the_table():
    run = _run("ena_runs_human_gut.json", host_tax_id="9823")

    host, taxon, warnings = _fields(run)

    assert (host, taxon) == ("not provided", "408170")
    assert any("9823" in w and run.sample_accession in w for w in warnings)


def test_host_text_without_a_host_tax_id_is_not_provided_and_the_warning_quotes_it():
    run = _run("ena_runs_host_text_only.json")

    host, taxon, warnings = _fields(run)

    assert (host, taxon) == ("not provided", "not provided")
    assert any("insect5" in w and run.sample_accession in w for w in warnings)


@pytest.mark.parametrize(
    ("overrides", "quoted"),
    [
        ({"host": "Homo sapiens"}, "Homo sapiens"),
        ({"tax_id": "408170", "host": "Mus musculus"}, "Mus musculus"),
        ({"host": "Anaerobic reactor treating cattle manure"}, "Anaerobic reactor"),
    ],
)
def test_host_text_without_a_host_tax_id_beats_the_table(overrides, quoted):
    run = _run("ena_runs_seawater.json", **overrides)
    run2 = _run("ena_runs_human_gut.json", host_tax_id=None, **overrides)

    for r in (run, run2):
        host, _, warnings = _fields(r)
        assert host == "not provided"
        assert any(quoted in w and r.sample_accession in w for w in warnings)


@pytest.mark.parametrize(
    "text", ["missing", "Not Collected", " unknown ", "missing: control sample"]
)
def test_missing_value_host_text_falls_back_to_the_table(text):
    run = _run("ena_runs_human_gut.json", host_tax_id=None, host=text)

    assert _fields(run) == ("9606", "408170", [])


def test_long_host_text_is_truncated_in_the_warning():
    run = _run("ena_runs_seawater.json", host="x" * 500)

    _, _, warnings = _fields(run)

    assert max(len(w) for w in warnings) < 250


@pytest.mark.parametrize("host_tax_id", ["1561972", "256318", "408170"])
def test_host_tax_id_naming_an_environment_is_not_a_host(host_tax_id):
    run = _run("ena_runs_seawater.json", host_tax_id=host_tax_id)

    host, _, warnings = _fields(run)

    assert host == "not applicable"
    assert any(host_tax_id in w and "not a host" in w for w in warnings)


def test_host_tax_id_equal_to_the_sample_taxon_is_not_a_host_even_off_the_table():
    run = _run("ena_runs_host_text_only.json", host_tax_id="749906", host=None)

    host, _, warnings = _fields(run)

    assert host == "not provided"
    assert any("749906" in w and "own taxon" in w for w in warnings)


def test_environment_host_tax_id_still_falls_back_to_the_table_host():
    run = _run("ena_runs_human_gut.json", host_tax_id="1561972", host=None)

    host, _, warnings = _fields(run)

    assert host == "9606"
    assert any("1561972" in w and "not a host" in w for w in warnings)


def test_host_tax_id_on_a_hostless_environment_is_reported_as_a_conflict():
    run = _run("ena_runs_seawater.json", host_tax_id="9606")

    host, _, warnings = _fields(run)

    assert host == "9606"
    assert any("9606" in w and "differs" in w and "none" in w for w in warnings)


def test_absent_tax_id_is_reported_without_printing_none():
    _, taxon, warnings = _fields(_run("ena_runs_missing_sample_record.json"))

    assert taxon == "not provided"
    assert not any("None" in w for w in warnings)


@pytest.mark.parametrize("tag", ["taxon id", "host taxon id"])
def test_attribute_tag_naming_a_written_taxon_field_is_dropped_and_reported(tag):
    global_metadata, local_metadata, result = _build(_HUMAN_GUT, {tag: ["1"], "site": ["lab"]})

    assert (global_metadata["host taxon id"], global_metadata["taxon id"]) == ("9606", "408170")
    assert local_metadata == {"site": "lab"}
    assert result.retained_unmapped == ["site"]
    assert any(repr(tag) in w for w in result.warnings)


def test_run_without_a_sample_record_has_both_fields_not_provided_and_two_warnings():
    host, taxon, warnings = _fields(_run("ena_runs_missing_sample_record.json"))

    assert (host, taxon) == ("not provided", "not provided")
    assert len(warnings) == 2


def test_multi_valued_host_tax_id_is_not_provided_with_a_warning():
    """Synthetic: no recorded row carries a ';'-joined host_tax_id."""
    run = _run("ena_runs_human_gut.json", host_tax_id="9606;9605")

    host, _, warnings = _fields(run)

    assert host == "not provided"
    assert any("9606;9605" in w for w in warnings)


def test_host_tax_id_wins_over_a_conflicting_table_host_and_the_conflict_is_reported():
    run = _run("ena_runs_human_gut.json", host_tax_id="10090")

    host, _, warnings = _fields(run)

    assert host == "10090"
    assert any("10090" in w and "9606" in w for w in warnings)


def test_table_host_that_is_not_loaded_is_not_provided_with_a_warning():
    run = _run("ena_runs_human_gut.json", host_tax_id=None, host=None)
    global_metadata, _, result = build_biosample_metadata(
        {},
        ena_run=run,
        implied_hosts={"408170": "9606"},
        loaded_term_ids=frozenset({"408170"}),
    )

    assert global_metadata["host taxon id"] == "not provided"
    assert any("9606" in w for w in result.warnings)


def test_empty_run_marks_both_taxon_fields_not_provided():
    run = _run("ena_runs_missing_sample_record.json")

    global_metadata, local_metadata, result = _build(run)

    assert global_metadata == {"host taxon id": "not provided", "taxon id": "not provided"}
    assert local_metadata == {}
    assert result.mapped_count == 0
    assert result.warnings


def test_build_biosample_metadata_single_values_map_as_before():
    global_metadata, local_metadata, result = _build(
        _HUMAN_GUT, {"depth": ["10"], "collection_date": ["2019-06-01"], "host": ["Homo sapiens"]}
    )

    assert global_metadata == {
        "depth": "10",
        "collection date": "2019-06-01",
        "host taxon id": "9606",
        "taxon id": "408170",
    }
    assert local_metadata == {"host": "Homo sapiens"}
    assert result.mapped_count == 2


def test_build_biosample_metadata_multi_valued_tags_never_reach_a_handler():
    global_metadata, local_metadata, result = _build(
        _HUMAN_GUT,
        {
            "depth": ["10", "5"],
            "collection_date": ["2017", "2019-06-01"],
            "geo_loc_name": ["Argentina", "Brazil"],
            "BioSampleModel": ["MIMARKS.survey", "MIGS/MIMS/MIMARKS.human-associated"],
            "ENA-FIRST-PUBLIC": ["2024-12-13", "2024-12-13T01:09:31Z"],
        },
    )

    assert global_metadata == {"host taxon id": "9606", "taxon id": "408170"}
    assert local_metadata == {
        "depth": '["10", "5"]',
        "collection_date": '["2017", "2019-06-01"]',
        "geo_loc_name": '["Argentina", "Brazil"]',
        "BioSampleModel": '["MIGS/MIMS/MIMARKS.human-associated", "MIMARKS.survey"]',
        "ENA-FIRST-PUBLIC": '["2024-12-13", "2024-12-13T01:09:31Z"]',
    }
    assert result.mapped_count == 0


def test_build_biosample_metadata_json_keeps_non_ascii_unescaped():
    _, local_metadata, _ = _build(_HUMAN_GUT, {"site": ["Z\u00fcrich", "S\u00e3o Paulo"]})

    assert local_metadata == {"site": '["S\u00e3o Paulo", "Z\u00fcrich"]'}


def test_ena_import_source_has_no_bare_taxon_id_literals():
    """The display names are written through their constants, never a literal."""
    package_dir = Path(ena_import.__file__).parent
    offenders = [
        p.name
        for p in package_dir.glob("*.py")
        if '"host taxon id"' in p.read_text() or '"taxon id"' in p.read_text()
    ]
    assert not offenders


_TYPES = {
    "depth": FieldDataType.NUMERIC,
    "geographic location (latitude)": FieldDataType.NUMERIC,
    "geographic location (longitude)": FieldDataType.NUMERIC,
    "taxon id": FieldDataType.TERMINOLOGY,
}

# display_name -> global internal_name, so a coordinate is range-checked (past its
# degree bound), not just type-checked. Non-coordinate fields carry a benign name.
_INTERNAL_NAMES = {
    "depth": "depth",
    "geographic location (latitude)": BIOSAMPLE_FIELD_GEOGRAPHIC_LOCATION_LATITUDE,
    "geographic location (longitude)": BIOSAMPLE_FIELD_GEOGRAPHIC_LOCATION_LONGITUDE,
    "taxon id": "taxon_id",
}


def test_retain_unparseable_keeps_a_bad_value_study_local():
    metadata, local, warnings = retain_unparseable_as_submitted(
        {"depth": "deep", "geographic location (latitude)": "39 deg"},
        {"env_medium": "soil"},
        data_types=_TYPES,
        internal_names=_INTERNAL_NAMES,
        missing_reasons={"not collected", "not provided"},
        sample_accession="SAMEA1",
    )

    assert metadata == {
        "depth": MISSING_REASON_NOT_PROVIDED,
        "geographic location (latitude)": MISSING_REASON_NOT_PROVIDED,
    }
    assert local == {
        "env_medium": "soil",
        "depth (as submitted)": "deep",
        "geographic location (latitude) (as submitted)": "39 deg",
    }
    assert warnings[1] == (
        "SAMEA1: 'geographic location (latitude)' value '39 deg' cannot be stored as"
        " numeric (not a valid decimal number); stored 'not provided' and kept the"
        " submitted text as 'geographic location (latitude) (as submitted)'"
    )
    assert len(warnings) == 2


def test_retain_unparseable_leaves_valid_values_and_missing_reasons():
    metadata = {
        "depth": "12.5",
        "geographic location (latitude)": " not collected ",
        "taxon id": "anything",  # TERMINOLOGY: resolved by the import, not here
        "sampling height": "tall",  # no data type known: left for the import to report
    }

    kept, local, warnings = retain_unparseable_as_submitted(
        metadata,
        {},
        data_types=_TYPES,
        internal_names=_INTERNAL_NAMES,
        missing_reasons={"not collected"},
        sample_accession="SAMEA1",
    )

    assert kept == metadata
    assert local == {}
    assert warnings == []


def test_retain_unparseable_raises_naming_the_taken_as_submitted_field():
    # a raw tag already holds the `(as submitted)` name: keeping both would overwrite
    # one, so the run fails and the reason names the taken field.
    with pytest.raises(MetadataParseError) as excinfo:
        retain_unparseable_as_submitted(
            {"depth": "deep"},
            {"depth (as submitted)": "raw tag"},
            data_types=_TYPES,
            internal_names=_INTERNAL_NAMES,
            missing_reasons=set(),
            sample_accession="SAMEA1",
        )

    message = str(excinfo.value)
    assert "cannot keep the submitted text as 'depth (as submitted)'" in message
    assert "already a submitted attribute" in message


def test_retain_unparseable_stores_a_blank_value_without_an_as_submitted_field():
    # A blank value has no submitted text worth keeping: `not provided`, no local field.
    kept, local, warnings = retain_unparseable_as_submitted(
        {"depth": "   "},
        {},
        data_types=_TYPES,
        internal_names=_INTERNAL_NAMES,
        missing_reasons=set(),
        sample_accession="SAMEA1",
    )

    assert kept == {"depth": MISSING_REASON_NOT_PROVIDED}
    assert local == {}
    assert warnings == ["SAMEA1: 'depth' is blank; stored 'not provided'"]


def test_retain_unparseable_keeps_an_out_of_range_coordinate_as_submitted():
    # A longitude past +/-180 is unparseable (range, not just type), so it is kept as
    # submitted rather than failing the run.
    kept, local, warnings = retain_unparseable_as_submitted(
        {"geographic location (longitude)": "200", "geographic location (latitude)": "95"},
        {},
        data_types=_TYPES,
        internal_names=_INTERNAL_NAMES,
        missing_reasons=set(),
        sample_accession="SAMEA1",
    )

    assert kept == {
        "geographic location (longitude)": MISSING_REASON_NOT_PROVIDED,
        "geographic location (latitude)": MISSING_REASON_NOT_PROVIDED,
    }
    assert local == {
        "geographic location (longitude) (as submitted)": "200",
        "geographic location (latitude) (as submitted)": "95",
    }
    assert any("not a coordinate between -180 and 180 degrees" in w for w in warnings)
    assert any("not a coordinate between -90 and 90 degrees" in w for w in warnings)
