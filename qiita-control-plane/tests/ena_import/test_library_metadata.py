"""Unit tests for the ENA `library_*` -> study-local metadata mapping (pure,
no database): blank handling at the model, trimming at the mapper, and the
display-name convention the CHANGELOG promises users."""

from qiita_common.models.ena import EnaRunRecord

from qiita_control_plane.ena_import.registration import (
    ENA_LIBRARY_LAYOUT_FIELD_NAME,
    ENA_LIBRARY_SELECTION_FIELD_NAME,
    ENA_LIBRARY_SOURCE_FIELD_NAME,
    ENA_LIBRARY_STRATEGY_FIELD_NAME,
    _library_metadata,
)


def _record(**overrides) -> EnaRunRecord:
    fields = {
        "run_accession": "SRR1",
        "experiment_accession": "SRX1",
        "sample_accession": "SAMN1",
        "study_accession": "PRJNA1",
        "library_strategy": "WGS",
        "library_source": "GENOMIC",
        "library_selection": None,
        "library_layout": "PAIRED",
    }
    fields.update(overrides)
    return EnaRunRecord(**fields)


def test_library_metadata_keys_off_the_four_display_names():
    assert _library_metadata(_record()) == {
        ENA_LIBRARY_STRATEGY_FIELD_NAME: "WGS",
        ENA_LIBRARY_SOURCE_FIELD_NAME: "GENOMIC",
        ENA_LIBRARY_LAYOUT_FIELD_NAME: "PAIRED",
    }


def test_library_metadata_trims_deposited_whitespace():
    md = _library_metadata(_record(library_strategy="  RNA-Seq "))
    assert md[ENA_LIBRARY_STRATEGY_FIELD_NAME] == "RNA-Seq"


def test_blank_library_fields_yield_no_entry():
    # "" and whitespace-only both normalize to None at the model (the
    # EnaStudyHeader convention), so neither reaches the slot; None likewise.
    md = _library_metadata(
        _record(library_strategy="", library_selection="   ", library_layout=None)
    )
    assert ENA_LIBRARY_STRATEGY_FIELD_NAME not in md
    assert ENA_LIBRARY_SELECTION_FIELD_NAME not in md
    assert ENA_LIBRARY_LAYOUT_FIELD_NAME not in md
    assert md[ENA_LIBRARY_SOURCE_FIELD_NAME] == "GENOMIC"


def test_blank_normalizes_to_none_on_the_model():
    assert _record(library_strategy="").library_strategy is None
    assert _record(library_strategy="   ").library_strategy is None


def test_display_names_are_lowercase_and_ena_prefixed():
    # Lowercase follows the local-field convention ("ena sample id"); the
    # "ena " prefix keeps the names from being byte-identical to the pruned
    # "Library strategy"-style globals a migrate:down would re-seed.
    assert (
        ENA_LIBRARY_STRATEGY_FIELD_NAME,
        ENA_LIBRARY_SOURCE_FIELD_NAME,
        ENA_LIBRARY_SELECTION_FIELD_NAME,
        ENA_LIBRARY_LAYOUT_FIELD_NAME,
    ) == (
        "ena library strategy",
        "ena library source",
        "ena library selection",
        "ena library layout",
    )
