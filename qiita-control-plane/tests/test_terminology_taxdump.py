"""Unit tests for qiita_control_plane.terminology_taxdump — the reading of a
taxdump archive's members and the term rows assembled from them."""

import logging
import zipfile

import pytest
from qiita_common.models import TerminologyTermObsoletionKind

from qiita_control_plane.repositories.terminology import MAX_REPORTED_OFFENDERS
from qiita_control_plane.terminology_taxdump import _read_dmp_rows, build_terms_from_taxdump
from qiita_control_plane.testing.terminology import (
    DELNODES_DMP_MEMBER,
    MERGED_DMP_MEMBER,
    NAMES_DMP_MEMBER,
    TAXDUMP_ARCHIVE_FILENAME,
    parsed_term,
    write_taxdump,
    write_taxdump_zip,
)

_MERGED = TerminologyTermObsoletionKind.SOURCE_MERGED
_DEPRECATED = TerminologyTermObsoletionKind.SOURCE_DEPRECATED


# =============================================================================
# build_terms_from_taxdump
# =============================================================================


def test_build_terms_from_taxdump(tmp_path):
    """Tests the case where the archive records all three kinds of taxon: a
    live one with both names, a live one with only a scientific name, a
    merged-away id, and a deleted id."""
    archive_path = write_taxdump(
        tmp_path,
        names=[
            ("2", "Bacteria", "Bacteria <bacteria>", "scientific name"),
            ("2", "eubacteria", "", "genbank common name"),
            ("9606", "Homo sapiens", "", "scientific name"),
            ("9606", "human", "", "genbank common name"),
            ("1234", "Nonesuch bacterium", "", "scientific name"),
        ],
        merged=[("30", "9606")],
        delnodes=[("777",)],
    )

    result = build_terms_from_taxdump(archive_path)

    expected = [
        parsed_term("2", "Bacteria", alternate_label="eubacteria"),
        parsed_term("9606", "Homo sapiens", alternate_label="human"),
        parsed_term("1234", "Nonesuch bacterium"),
        parsed_term(
            "30",
            None,
            is_obsolete=True,
            replaced_by_term_id="9606",
            obsoletion_kind=_MERGED,
        ),
        parsed_term("777", None, is_obsolete=True, obsoletion_kind=_DEPRECATED),
    ]
    assert result == expected


def test_build_terms_from_taxdump_unconsumed_name_classes(tmp_path):
    """Tests the case where a taxon carries name classes neither name column
    is taken from: they are read past, and only the two consumed classes
    reach the term row."""
    archive_path = write_taxdump(
        tmp_path,
        names=[
            ("1", "all", "", "synonym"),
            ("1", "root", "", "scientific name"),
            ("2", "Bacteria", "Bacteria <bacteria>", "scientific name"),
            ("2", "bacteria", "bacteria <blast name>", "blast name"),
            ("2", "Cavalier-Smith 1987", "", "authority"),
            ("2", "not Bacteria Haeckel 1894", "", "in-part"),
            ("2", "eubacteria", "", "genbank common name"),
        ],
    )

    result = build_terms_from_taxdump(archive_path)

    expected = [
        parsed_term("1", "root"),
        parsed_term("2", "Bacteria", alternate_label="eubacteria"),
    ]
    assert result == expected


def test_build_terms_from_taxdump_duplicate_scientific_name(tmp_path, caplog):
    """Tests the case where a taxon carries two scientific names: the first
    stands and the extra is warned about, because the column holds one."""
    archive_path = write_taxdump(
        tmp_path,
        names=[
            ("2", "Bacteria", "", "scientific name"),
            ("2", "Procaryotae", "", "scientific name"),
        ],
    )

    with caplog.at_level(logging.WARNING):
        result = build_terms_from_taxdump(archive_path)

    assert result == [parsed_term("2", "Bacteria")]
    assert len(caplog.records) == 1
    assert "Procaryotae" in caplog.text


def test_build_terms_from_taxdump_duplicate_genbank_common_name(tmp_path, caplog):
    """Tests the case where a taxon carries two genbank common names: the
    first stands and the extra is warned about."""
    archive_path = write_taxdump(
        tmp_path,
        names=[
            ("2", "Bacteria", "", "scientific name"),
            ("2", "eubacteria", "", "genbank common name"),
            ("2", "prokaryotes", "", "genbank common name"),
        ],
    )

    with caplog.at_level(logging.WARNING):
        result = build_terms_from_taxdump(archive_path)

    assert result == [parsed_term("2", "Bacteria", alternate_label="eubacteria")]
    assert len(caplog.records) == 1
    assert "prokaryotes" in caplog.text


def test_build_terms_from_taxdump_tax_id_in_two_members(tmp_path):
    """Tests the case where one taxon id is recorded both as live and as
    deleted, which no taxdump can mean."""
    archive_path = write_taxdump(
        tmp_path,
        names=[("2", "Bacteria", "", "scientific name")],
        delnodes=[("2",)],
    )

    with pytest.raises(ValueError, match="recorded in more than one member"):
        build_terms_from_taxdump(archive_path)


def test_build_terms_from_taxdump_tax_id_in_every_member_pair(tmp_path):
    """Tests the case where all three pairs of members disagree: every pair is
    named in the one error, not just the first found."""
    archive_path = write_taxdump(
        tmp_path,
        names=[
            ("2", "Bacteria", "", "scientific name"),
            ("3", "Nonesuch bacterium", "", "scientific name"),
        ],
        merged=[("2", "9606"), ("4", "9606")],
        delnodes=[("3",), ("4",)],
    )

    with pytest.raises(ValueError) as raised:
        build_terms_from_taxdump(archive_path)

    message = str(raised.value)
    assert f"{NAMES_DMP_MEMBER} and {MERGED_DMP_MEMBER}: ['2']" in message
    assert f"{NAMES_DMP_MEMBER} and {DELNODES_DMP_MEMBER}: ['3']" in message
    assert f"{MERGED_DMP_MEMBER} and {DELNODES_DMP_MEMBER}: ['4']" in message


def test_build_terms_from_taxdump_member_overlap_over_cap(tmp_path):
    """Tests the case where more taxon ids are recorded two ways than the error
    names: the total is stated and the tail is left unnamed."""
    over_cap_count = MAX_REPORTED_OFFENDERS + 5
    tax_ids = [str(tax_id) for tax_id in range(100, 100 + over_cap_count)]
    archive_path = write_taxdump(
        tmp_path,
        names=[(tax_id, f"Taxon {tax_id}", "", "scientific name") for tax_id in tax_ids],
        delnodes=[(tax_id,) for tax_id in tax_ids],
    )

    with pytest.raises(ValueError) as raised:
        build_terms_from_taxdump(archive_path)

    message = str(raised.value)
    assert f"{over_cap_count} total, first {MAX_REPORTED_OFFENDERS}" in message
    assert tax_ids[-1] not in message


def test_build_terms_from_taxdump_taxon_without_scientific_name(tmp_path):
    """Tests the case where a taxon is named only in the class the second
    name column is taken from, leaving the label column nothing to hold."""
    archive_path = write_taxdump(
        tmp_path,
        names=[("2", "eubacteria", "", "genbank common name")],
    )

    with pytest.raises(ValueError, match="scientific name"):
        build_terms_from_taxdump(archive_path)


def test_build_terms_from_taxdump_unnamed_over_cap(tmp_path):
    """Tests the case where more taxa lack the name the label is taken from than
    the error names: the total is stated and the tail is left unnamed."""
    over_cap_count = MAX_REPORTED_OFFENDERS + 5
    tax_ids = [str(tax_id) for tax_id in range(100, 100 + over_cap_count)]
    archive_path = write_taxdump(
        tmp_path,
        names=[(tax_id, f"common {tax_id}", "", "genbank common name") for tax_id in tax_ids],
    )

    with pytest.raises(ValueError) as raised:
        build_terms_from_taxdump(archive_path)

    message = str(raised.value)
    assert f"{over_cap_count} total, first {MAX_REPORTED_OFFENDERS}" in message
    assert tax_ids[-1] not in message


def test_build_terms_from_taxdump_missing_member(tmp_path):
    """Tests the case where the archive is readable but carries none of the
    deleted taxon ids."""
    archive_path = tmp_path / TAXDUMP_ARCHIVE_FILENAME
    write_taxdump_zip(
        archive_path,
        {
            NAMES_DMP_MEMBER: [("2", "Bacteria", "", "scientific name")],
            MERGED_DMP_MEMBER: [],
        },
    )

    with pytest.raises(FileNotFoundError, match=DELNODES_DMP_MEMBER):
        build_terms_from_taxdump(archive_path)


def test_build_terms_from_taxdump_missing_archive(tmp_path):
    """Tests the case where the named archive does not exist."""
    with pytest.raises(FileNotFoundError, match="No taxdump archive"):
        build_terms_from_taxdump(tmp_path / TAXDUMP_ARCHIVE_FILENAME)


# =============================================================================
# _read_dmp_rows
# =============================================================================


def test__read_dmp_rows_field_count_mismatch(tmp_path):
    """Tests the case where a row carries fewer fields than the member's
    documented column order declares."""
    archive_path = write_taxdump(tmp_path, names=[("2", "Bacteria", "")])

    with zipfile.ZipFile(archive_path) as archive:
        rows = _read_dmp_rows(archive, NAMES_DMP_MEMBER, ("a", "b", "c", "d"))
        with pytest.raises(ValueError, match="expected 4"):
            list(rows)


def test__read_dmp_rows_missing_row_terminator(tmp_path):
    """Tests the case where a row does not end with the terminator every
    taxdump row carries."""
    archive_path = tmp_path / TAXDUMP_ARCHIVE_FILENAME
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(NAMES_DMP_MEMBER, "2\t|\tBacteria\n")

    with zipfile.ZipFile(archive_path) as archive:
        rows = _read_dmp_rows(archive, NAMES_DMP_MEMBER, ("a", "b"))
        with pytest.raises(ValueError, match="row terminator"):
            list(rows)
