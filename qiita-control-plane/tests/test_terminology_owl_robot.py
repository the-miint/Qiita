"""Unit tests for qiita_control_plane.terminology_owl_robot — the argv of the
ROBOT export and the dialect of what that export writes."""

import pytest

from qiita_control_plane.terminology_owl_robot import parse_robot_export, robot_export_argv
from qiita_control_plane.testing.terminology import (
    ROBOT_EXPORT_HEADER,
    exported_class,
    write_robot_export_tsv,
)

# =============================================================================
# robot_export_argv
# =============================================================================


def test_robot_export_argv():
    """Tests the case where no runtime is named: the argv runs plain robot
    over the given input and writes the named export."""
    result = robot_export_argv("source.owl", "robot-export.tsv")

    expected = [
        "robot",
        "export",
        "--input",
        "source.owl",
        "--header",
        "|".join(ROBOT_EXPORT_HEADER),
        "--include",
        "classes",
        "--export",
        "robot-export.tsv",
    ]
    assert result == expected


def test_robot_export_argv_executable():
    """Tests the case where a runtime is named: its tokens lead the argv and
    everything after is unchanged."""
    result = robot_export_argv(
        "uberon-base.owl",
        "uberon-export.tsv",
        executable=("apptainer", "exec", "/images/robot.sif", "robot"),
    )

    expected = [
        "apptainer",
        "exec",
        "/images/robot.sif",
        "robot",
        "export",
        "--input",
        "uberon-base.owl",
        "--header",
        "|".join(ROBOT_EXPORT_HEADER),
        "--include",
        "classes",
        "--export",
        "uberon-export.tsv",
    ]
    assert result == expected


# =============================================================================
# parse_robot_export
# =============================================================================


def test_parse_robot_export(tmp_path):
    """Tests the case where cells carry a typed literal, an absent
    annotation, and several values at once."""
    export_path = tmp_path / "robot-export.tsv"
    write_robot_export_tsv(
        export_path,
        [
            ("UBERON:0001", "mouth", "", "", ""),
            ("UBERON:0002", "obsolete tooth", "true^^xsd:boolean", "UBERON:0001", ""),
            ("UBERON:0003", "molar", "false", "", "UBERON:0900|UBERON:0901"),
        ],
    )

    result = parse_robot_export(export_path)

    expected = [
        exported_class("UBERON:0001", "mouth"),
        exported_class(
            "UBERON:0002",
            "obsolete tooth",
            source_deprecated=True,
            asserted_replacement_term_id="UBERON:0001",
        ),
        exported_class("UBERON:0003", "molar", alternative_term_ids=("UBERON:0900", "UBERON:0901")),
    ]
    assert result == expected


def test_parse_robot_export_missing_file(tmp_path):
    """Tests the case where the named export does not exist."""
    with pytest.raises(FileNotFoundError, match="No ROBOT export"):
        parse_robot_export(tmp_path / "robot-export.tsv")


def test_parse_robot_export_missing_column(tmp_path):
    """Tests the case where the export lacks a requested column."""
    export_path = tmp_path / "robot-export.tsv"
    export_path.write_text("ID\tLABEL\nUBERON:0001\tmouth\n")

    with pytest.raises(ValueError, match="missing column"):
        parse_robot_export(export_path)


def test_parse_robot_export_no_term_id(tmp_path):
    """Tests the case where a row carries no term id."""
    export_path = tmp_path / "robot-export.tsv"
    write_robot_export_tsv(export_path, [("", "mouth", "", "", "")])

    with pytest.raises(ValueError, match="no term id"):
        parse_robot_export(export_path)


def test_parse_robot_export_invalid_deprecated(tmp_path):
    """Tests the case where owl:deprecated holds neither true nor false."""
    export_path = tmp_path / "robot-export.tsv"
    write_robot_export_tsv(export_path, [("UBERON:0001", "mouth", "maybe", "", "")])

    with pytest.raises(ValueError, match="invalid owl:deprecated"):
        parse_robot_export(export_path)
