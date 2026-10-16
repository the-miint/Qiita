"""Classification rules for resolving a pool roster by external identifiers.

Pure: the facts the database would supply are handed in, so every rule is
exercised without Postgres.
"""

from typing import get_args

import pytest
from qiita_common.models import (
    BiosampleRef,
    RosterProblem,
    RosterResolvedRow,
    RosterResolveRow,
    StudyRef,
)

from qiita_control_plane.repositories.biosample import BiosampleLookupKey
from qiita_control_plane.repositories.study import StudyLookupKey
from qiita_control_plane.roster_resolution import (
    _BIOSAMPLE_SPEC,
    _STUDY_SPEC,
    RosterFacts,
    classify_roster,
)

TUBE = "0363157924"
TUBE_RAW = "363157924"
BIOSAMPLE = {"biosample_accession": "SAMN1"}
PRIMARY = {"bioproject_accession": "PRJNA1"}


def _facts() -> RosterFacts:
    """Biosample 21 carries every identifier and links to studies 101 and 102;
    biosample 22 links to 101 only. Study 103 exists but links to nothing."""
    return RosterFacts(
        biosample_by_key={
            "biosample_accession": {"SAMN1": 21, "SAMN2": 22},
            "ena_sample_accession": {"ERS1": 21},
            "export_entity_id": {"QB21": 21},
            "matrix_tube_id": {TUBE: 21},
        },
        study_by_key={
            "bioproject_accession": {"PRJNA1": 101, "PRJNA2": 102, "PRJNA3": 103},
            "ena_study_accession": {"PRJEB1": 101},
            "export_entity_id": {"QS101": 101},
        },
        study_links={21: [101, 102], 22: [101]},
    )


def _row(
    item_id: str = "1",
    *,
    biosample: dict = BIOSAMPLE,
    primary: dict = PRIMARY,
    secondaries: tuple[dict, ...] = (),
) -> RosterResolveRow:
    return RosterResolveRow(
        item_id=item_id,
        biosample=BiosampleRef(**biosample),
        primary_project=StudyRef(**primary),
        secondary_projects=[StudyRef(**s) for s in secondaries],
    )


def _resolved(item_id: str, biosample_idx: int, secondaries: list[int] | None = None):
    return RosterResolvedRow(
        item_id=item_id,
        biosample_idx=biosample_idx,
        primary_study_idx=101,
        secondary_study_idxs=secondaries or [],
    )


@pytest.mark.parametrize(
    ("ref_model", "lookup_key"),
    [(BiosampleRef, BiosampleLookupKey), (StudyRef, StudyLookupKey)],
)
def test_ref_fields_match_lookup_keys(ref_model, lookup_key):
    """Tests the case where a ref model and its lookup keys are compared: each
    field is resolved by the key of the same name, so the two sets must match."""
    assert set(ref_model.model_fields) == set(get_args(lookup_key))


@pytest.mark.parametrize("spec", [_BIOSAMPLE_SPEC, _STUDY_SPEC])
def test__RefSpec_keys_are_ref_fields(spec):
    """Tests the case where a spec's per-key normalizers and miss messages are
    compared to its ref model: a key that is not a field would never apply."""
    spec_keys = set(spec.normalizers) | set(spec.miss_messages)
    assert spec_keys <= set(spec.ref_model.model_fields)


@pytest.mark.parametrize(
    "biosample",
    [
        {"biosample_accession": "SAMN1"},
        {"ena_sample_accession": "ERS1"},
        {"export_entity_id": "QB21"},
        {"matrix_tube_id": TUBE_RAW},
    ],
)
@pytest.mark.parametrize(
    "primary",
    [
        {"bioproject_accession": "PRJNA1"},
        {"ena_study_accession": "PRJEB1"},
        {"export_entity_id": "QS101"},
    ],
)
def test_classify_roster_each_identifier_alone(biosample, primary):
    """Tests the case where each entity is named by a single identifier of any key;
    a tube that lost its leading zero resolves to the stored form."""
    result = classify_roster([_row(biosample=biosample, primary=primary)], _facts())
    assert result == ([_resolved("1", 21)], [])


@pytest.mark.parametrize(
    ("biosample", "primary", "message"),
    [
        (
            {"biosample_accession": "SAMN404"},
            PRIMARY,
            "no biosample has biosample_accession 'SAMN404'",
        ),
        (
            {"ena_sample_accession": "ERS404"},
            PRIMARY,
            "no biosample has ena_sample_accession 'ERS404'",
        ),
        ({"export_entity_id": "QB404"}, PRIMARY, "no biosample has export_entity_id 'QB404'"),
        (
            {"matrix_tube_id": "999"},
            PRIMARY,
            "no biosample has matrix_tube_id '0000000999'; register it before submitting",
        ),
        (
            BIOSAMPLE,
            {"bioproject_accession": "PRJNA404"},
            "no study has bioproject_accession 'PRJNA404'",
        ),
        (
            BIOSAMPLE,
            {"ena_study_accession": "PRJEB404"},
            "no study has ena_study_accession 'PRJEB404'",
        ),
        (BIOSAMPLE, {"export_entity_id": "QS404"}, "no study has export_entity_id 'QS404'"),
    ],
)
def test_classify_roster_each_identifier_miss(biosample, primary, message):
    """Tests the case where the only identifier given for an entity names nothing."""
    result = classify_roster([_row(biosample=biosample, primary=primary)], _facts())
    assert result == ([], [RosterProblem(item_id="1", message=message)])


def test_classify_roster_all_identifiers_agree():
    """Tests the case where every key is given for each entity and all of them
    name the same one, with a linked secondary project."""
    row = _row(
        biosample={
            "biosample_accession": "SAMN1",
            "ena_sample_accession": "ERS1",
            "export_entity_id": "QB21",
            "matrix_tube_id": TUBE,
        },
        primary={
            "bioproject_accession": "PRJNA1",
            "ena_study_accession": "PRJEB1",
            "export_entity_id": "QS101",
        },
        secondaries=({"bioproject_accession": "PRJNA2"},),
    )
    result = classify_roster([row], _facts())
    assert result == ([_resolved("1", 21, [102])], [])


def test_classify_roster_miss_alongside_hit():
    """Tests the case where one identifier resolves and another, also given,
    names nothing: the miss refuses the row."""
    row = _row(biosample={"export_entity_id": "QB21", "biosample_accession": "SAMN404"})
    result = classify_roster([row], _facts())
    expected = RosterProblem(item_id="1", message="no biosample has biosample_accession 'SAMN404'")
    assert result == ([], [expected])


@pytest.mark.parametrize(
    ("biosample", "primary", "message"),
    [
        (
            {"biosample_accession": "SAMN2", "matrix_tube_id": TUBE},
            PRIMARY,
            "the identifiers name more than one biosample:"
            f" biosample_accession 'SAMN2', matrix_tube_id '{TUBE}'",
        ),
        (
            BIOSAMPLE,
            {"bioproject_accession": "PRJNA2", "export_entity_id": "QS101"},
            "the identifiers name more than one study:"
            " bioproject_accession 'PRJNA2', export_entity_id 'QS101'",
        ),
    ],
)
def test_classify_roster_identifiers_disagree(biosample, primary, message):
    """Tests the case where two identifiers given for one entity name different ones."""
    result = classify_roster([_row(biosample=biosample, primary=primary)], _facts())
    assert result == ([], [RosterProblem(item_id="1", message=message)])


def test_classify_roster_malformed_tube():
    """Tests the case where the tube is not a matrix tube id at all."""
    result = classify_roster([_row(biosample={"matrix_tube_id": "800805.001.V1.Plasma"})], _facts())
    expected = RosterProblem(
        item_id="1", message="'800805.001.V1.Plasma' is not a matrix tube id (1 to 10 digits)"
    )
    assert result == ([], [expected])


def test_classify_roster_repeated_tube():
    """Tests the case where technical replicates repeat one tube: each row resolves."""
    rows = [
        _row("a", biosample={"matrix_tube_id": TUBE}),
        _row("b", biosample={"matrix_tube_id": TUBE_RAW}),
    ]
    result = classify_roster(rows, _facts())
    assert result == ([_resolved("a", 21), _resolved("b", 21)], [])


def test_classify_roster_primary_not_linked():
    """Tests the case where the primary study is not among the biosample's links."""
    row = _row(
        biosample={"biosample_accession": "SAMN2"}, primary={"bioproject_accession": "PRJNA2"}
    )
    result = classify_roster([row], _facts())
    expected = RosterProblem(
        item_id="1",
        message="the biosample with biosample_accession 'SAMN2' is not in the study"
        " with bioproject_accession 'PRJNA2'",
    )
    assert result == ([], [expected])


def test_classify_roster_secondary_not_linked():
    """Tests the case where one secondary study is linked and another is not."""
    row = _row(secondaries=({"bioproject_accession": "PRJNA2"}, {"bioproject_accession": "PRJNA3"}))
    result = classify_roster([row], _facts())
    expected = RosterProblem(
        item_id="1",
        message="the biosample with biosample_accession 'SAMN1' is not in the study"
        " with bioproject_accession 'PRJNA3'",
    )
    assert result == ([], [expected])


def test_classify_roster_request_order():
    """Tests the case where resolved and unresolved rows interleave: each list
    keeps request order."""
    rows = [
        _row("ok1"),
        _row("bad1", biosample={"export_entity_id": "QB404"}),
        _row("ok2", biosample={"biosample_accession": "SAMN2"}),
        _row("bad2", primary={"export_entity_id": "QS404"}),
    ]
    result = classify_roster(rows, _facts())
    expected_problems = [
        RosterProblem(item_id="bad1", message="no biosample has export_entity_id 'QB404'"),
        RosterProblem(item_id="bad2", message="no study has export_entity_id 'QS404'"),
    ]
    assert result == ([_resolved("ok1", 21), _resolved("ok2", 22)], expected_problems)
