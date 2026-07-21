"""Tests for ENA/SRA accession-type detection and validation
(`ena_import.accession`)."""

import pytest


@pytest.mark.parametrize(
    "accession",
    ["PRJEB11419", "PRJNA555783", "ERP012803", "SRP012345"],
)
def test_detect_accession_kind_study(accession):
    from qiita_control_plane.ena_import.accession import EnaAccessionKind, detect_accession_kind

    assert detect_accession_kind(accession) is EnaAccessionKind.STUDY


@pytest.mark.parametrize(
    "accession",
    ["SAMEA3610311", "SAMN01821487", "SAME1234567", "ERS1234567"],
)
def test_detect_accession_kind_sample(accession):
    from qiita_control_plane.ena_import.accession import EnaAccessionKind, detect_accession_kind

    assert detect_accession_kind(accession) is EnaAccessionKind.SAMPLE


@pytest.mark.parametrize("accession", ["ERR1074767", "SRR1234567", "DRR1234567"])
def test_detect_accession_kind_run(accession):
    from qiita_control_plane.ena_import.accession import EnaAccessionKind, detect_accession_kind

    assert detect_accession_kind(accession) is EnaAccessionKind.RUN


@pytest.mark.parametrize("accession", ["ERX1111111", "SRX1234567"])
def test_detect_accession_kind_experiment(accession):
    from qiita_control_plane.ena_import.accession import EnaAccessionKind, detect_accession_kind

    assert detect_accession_kind(accession) is EnaAccessionKind.EXPERIMENT


def test_detect_accession_kind_rejects_empty():
    from qiita_control_plane.ena_import.accession import (
        InvalidEnaAccessionError,
        detect_accession_kind,
    )

    with pytest.raises(InvalidEnaAccessionError, match="must not be empty"):
        detect_accession_kind("")


def test_detect_accession_kind_rejects_blank():
    from qiita_control_plane.ena_import.accession import (
        InvalidEnaAccessionError,
        detect_accession_kind,
    )

    with pytest.raises(InvalidEnaAccessionError, match="must not be empty"):
        detect_accession_kind("   ")


@pytest.mark.parametrize("accession", ["FOO123", "NOTANACCESSION", "12345"])
def test_detect_accession_kind_rejects_unknown_prefix(accession):
    from qiita_control_plane.ena_import.accession import (
        InvalidEnaAccessionError,
        detect_accession_kind,
    )

    with pytest.raises(InvalidEnaAccessionError, match="does not match a known"):
        detect_accession_kind(accession)


def test_validate_study_accession_returns_trimmed():
    from qiita_control_plane.ena_import.accession import validate_study_accession

    assert validate_study_accession("  PRJEB11419  ") == "PRJEB11419"


def test_validate_study_accession_rejects_non_study_kind():
    from qiita_control_plane.ena_import.accession import (
        InvalidEnaAccessionError,
        validate_study_accession,
    )

    with pytest.raises(InvalidEnaAccessionError, match="not a study accession"):
        validate_study_accession("SAMEA3610311")


def test_validate_study_accession_rejects_invalid_shape():
    from qiita_control_plane.ena_import.accession import (
        InvalidEnaAccessionError,
        validate_study_accession,
    )

    with pytest.raises(InvalidEnaAccessionError):
        validate_study_accession("")
