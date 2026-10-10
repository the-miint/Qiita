"""Classification rules for resolving a pool roster by tube or accession.

Pure: the facts the database would supply are handed in, so every rule is
exercised without Postgres.
"""

from qiita_common.models import RosterResolvedRow, RosterResolveRow

from qiita_control_plane.roster_resolution import RosterFacts, classify_roster

TUBE = "0363157924"
TUBE_RAW = "363157924"
_NO_REPLICATES = "technical replicates are not supported in a tube-keyed roster"


def _facts(**overrides) -> RosterFacts:
    base = {
        "by_tube": {TUBE: (11, None)},
        "by_biosample_accession": {"SAMN1": 21},
        "by_study_accession": {"PRJNA1": 101, "PRJNA2": 102},
        "study_links": {11: [101], 21: [101]},
    }
    base.update(overrides)
    return RosterFacts(**base)


def _messages(problems) -> list[tuple[str, str]]:
    return [(p.item_id, p.message) for p in problems]


def test_tube_row_resolves_study_from_its_link():
    resolved, problems = classify_roster(
        [RosterResolveRow(item_id="1", matrix_tube_id=TUBE_RAW)], _facts()
    )
    assert problems == []
    assert resolved == [
        RosterResolvedRow(
            item_id="1", biosample_idx=11, primary_study_idx=101, secondary_study_idxs=[]
        )
    ]


def test_tube_row_project_accession_must_name_a_linked_study():
    ok, ok_problems = classify_roster(
        [RosterResolveRow(item_id="1", matrix_tube_id=TUBE, primary_project_accession="PRJNA1")],
        _facts(),
    )
    assert ok_problems == []
    assert ok[0].primary_study_idx == 101

    _, problems = classify_roster(
        [RosterResolveRow(item_id="1", matrix_tube_id=TUBE, primary_project_accession="PRJNA2")],
        _facts(),
    )
    assert _messages(problems) == [
        ("1", f"the biosample with matrix tube {TUBE} is not in study PRJNA2")
    ]


def test_tube_row_with_no_or_several_study_links():
    _, none = classify_roster(
        [RosterResolveRow(item_id="1", matrix_tube_id=TUBE)], _facts(study_links={})
    )
    assert _messages(none) == [
        ("1", f"the biosample with matrix tube {TUBE} belongs to no active study")
    ]
    _, several = classify_roster(
        [RosterResolveRow(item_id="1", matrix_tube_id=TUBE)],
        _facts(study_links={11: [101, 102]}),
    )
    assert _messages(several) == [
        (
            "1",
            f"the biosample with matrix tube {TUBE} belongs to 2 studies; name the"
            " project's bioproject accession to choose one",
        )
    ]


def test_tube_problems_malformed_unknown_duplicate():
    rows = [
        RosterResolveRow(item_id="bad", matrix_tube_id="800805.001.V1.Plasma"),
        RosterResolveRow(item_id="gone", matrix_tube_id="999"),
        RosterResolveRow(item_id="a", matrix_tube_id=TUBE_RAW),
        RosterResolveRow(item_id="b", matrix_tube_id=TUBE),
    ]
    resolved, problems = classify_roster(rows, _facts())
    assert resolved == []
    # A tube is reported in its normalized form, the one a registry holds.
    assert _messages(problems) == [
        ("bad", "'800805.001.V1.Plasma' is not a matrix tube id (1 to 10 digits)"),
        ("gone", "no biosample has matrix tube 0000000999; register it before submitting"),
        ("a", f"matrix tube {TUBE} appears on 2 rows of this roster; {_NO_REPLICATES}"),
        ("b", f"matrix tube {TUBE} appears on 2 rows of this roster; {_NO_REPLICATES}"),
    ]


def test_tube_and_accession_must_agree():
    _, problems = classify_roster(
        [RosterResolveRow(item_id="1", matrix_tube_id=TUBE, biosample_accession="SAMN9")],
        _facts(by_tube={TUBE: (11, "SAMN1")}),
    )
    assert _messages(problems) == [
        ("1", f"matrix tube {TUBE} belongs to a biosample with accession 'SAMN1', not 'SAMN9'")
    ]


def test_accession_row_resolves_by_accessions():
    resolved, problems = classify_roster(
        [
            RosterResolveRow(
                item_id="1",
                biosample_accession="SAMN1",
                primary_project_accession="PRJNA1",
                secondary_project_accessions=["PRJNA2"],
            )
        ],
        _facts(),
    )
    assert problems == []
    assert resolved == [
        RosterResolvedRow(
            item_id="1", biosample_idx=21, primary_study_idx=101, secondary_study_idxs=[102]
        )
    ]


def test_accession_and_identity_problems():
    rows = [
        RosterResolveRow(item_id="none"),
        RosterResolveRow(
            item_id="nobs", biosample_accession="SAMN404", primary_project_accession="PRJNA1"
        ),
        RosterResolveRow(
            item_id="nostudy",
            biosample_accession="SAMN1",
            primary_project_accession="PRJNA404",
            secondary_project_accessions=["PRJNA405"],
        ),
    ]
    _, problems = classify_roster(rows, _facts())
    assert _messages(problems) == [
        ("none", "row carries neither a matrix tube nor a biosample accession"),
        ("nobs", "no biosample has accession 'SAMN404'"),
        ("nostudy", "no study has accession 'PRJNA404'"),
        ("nostudy", "no study has accession 'PRJNA405'"),
    ]


def test_mixed_roster_resolves_in_request_order():
    rows = [
        RosterResolveRow(item_id="t", matrix_tube_id=TUBE),
        RosterResolveRow(
            item_id="a", biosample_accession="SAMN1", primary_project_accession="PRJNA1"
        ),
    ]
    resolved, problems = classify_roster(rows, _facts())
    assert problems == []
    assert [r.item_id for r in resolved] == ["t", "a"]


def test_accession_row_without_a_project_takes_its_one_study():
    resolved, problems = classify_roster(
        [RosterResolveRow(item_id="1", biosample_accession="SAMN1")], _facts()
    )
    assert problems == []
    assert resolved[0].primary_study_idx == 101

    _, several = classify_roster(
        [RosterResolveRow(item_id="1", biosample_accession="SAMN1")],
        _facts(study_links={21: [101, 102]}),
    )
    assert _messages(several) == [
        (
            "1",
            "biosample SAMN1 belongs to 2 studies; name the project's bioproject"
            " accession to choose one",
        )
    ]


def test_tube_whose_biosample_has_no_accession_conflicts_with_a_named_one():
    _, problems = classify_roster(
        [RosterResolveRow(item_id="1", matrix_tube_id=TUBE, biosample_accession="SAMN9")],
        _facts(),
    )
    assert _messages(problems) == [
        ("1", f"matrix tube {TUBE} belongs to a biosample with no accession, not 'SAMN9'")
    ]
