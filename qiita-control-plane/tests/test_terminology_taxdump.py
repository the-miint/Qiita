"""Unit tests for qiita_control_plane.terminology_taxdump — the reading of a
taxdump archive's members and the term rows assembled from them."""

import logging

import duckdb
import pytest
from qiita_common.models import TerminologyTermObsoletionKind

from qiita_control_plane.repositories.terminology import MAX_REPORTED_OFFENDERS
from qiita_control_plane.terminology_taxdump import build_terms_from_taxdump
from qiita_control_plane.testing.terminology import (
    FIXTURE_DELNODES_DMP_MEMBER,
    FIXTURE_MERGED_DMP_MEMBER,
    FIXTURE_NAMES_DMP_MEMBER,
    FIXTURE_TAXDUMP_ARCHIVE_FILENAME,
    parsed_term,
    write_taxdump,
    write_taxdump_tar,
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
        parsed_term("1234", "Nonesuch bacterium"),
        parsed_term("9606", "Homo sapiens", alternate_label="human"),
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
    """Tests the case where a taxon carries name classes feeding neither name
    column: the read skips them, and only the two consumed classes reach the
    term row."""
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
    """Tests the case where a taxon carries two scientific names: one stands
    and the read warns about the surplus, because the column holds one."""
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
    assert "tax_id 2 carries 2 'scientific name' names; keeping 'Bacteria'" in caplog.text


def test_build_terms_from_taxdump_duplicate_genbank_common_name(tmp_path, caplog):
    """Tests the case where a taxon carries two genbank common names: one
    stands and the read warns about the surplus."""
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
    assert "tax_id 2 carries 2 'genbank common name' names; keeping 'eubacteria'" in caplog.text


def test_build_terms_from_taxdump_tax_id_in_two_members(tmp_path):
    """Tests the case where the archive records one taxon id both as live and
    as deleted, which no taxdump can mean."""
    archive_path = write_taxdump(
        tmp_path,
        names=[("2", "Bacteria", "", "scientific name")],
        delnodes=[("2",)],
    )

    with pytest.raises(ValueError, match="recorded in more than one member"):
        build_terms_from_taxdump(archive_path)


def test_build_terms_from_taxdump_tax_id_in_every_member_pair(tmp_path):
    """Tests the case where all three pairs of members disagree: one error
    names every pair, not just the first found."""
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
    assert f"{FIXTURE_NAMES_DMP_MEMBER} and {FIXTURE_MERGED_DMP_MEMBER}: ['2']" in message
    assert f"{FIXTURE_NAMES_DMP_MEMBER} and {FIXTURE_DELNODES_DMP_MEMBER}: ['3']" in message
    assert f"{FIXTURE_MERGED_DMP_MEMBER} and {FIXTURE_DELNODES_DMP_MEMBER}: ['4']" in message


def test_build_terms_from_taxdump_member_overlap_over_cap(tmp_path):
    """Tests the case where the archive records more taxon ids two ways than
    the error names: the error states the total and leaves the tail unnamed."""
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
    """Tests the case where the archive names a taxon only in the class feeding
    the second name column, leaving the label column nothing to hold."""
    archive_path = write_taxdump(
        tmp_path,
        names=[("2", "eubacteria", "", "genbank common name")],
    )

    with pytest.raises(ValueError, match="scientific name"):
        build_terms_from_taxdump(archive_path)


def test_build_terms_from_taxdump_unnamed_over_cap(tmp_path):
    """Tests the case where more taxa lack the name the label comes from than
    the error names: the error states the total and leaves the tail unnamed."""
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
    archive_path = tmp_path / FIXTURE_TAXDUMP_ARCHIVE_FILENAME
    write_taxdump_tar(
        archive_path,
        {
            FIXTURE_NAMES_DMP_MEMBER: [("2", "Bacteria", "", "scientific name")],
            FIXTURE_MERGED_DMP_MEMBER: [],
        },
    )

    with pytest.raises(duckdb.Error, match=FIXTURE_DELNODES_DMP_MEMBER):
        build_terms_from_taxdump(archive_path)


def test_build_terms_from_taxdump_missing_archive(tmp_path):
    """Tests the case where the named archive does not exist."""
    with pytest.raises(FileNotFoundError, match="No taxdump archive"):
        build_terms_from_taxdump(tmp_path / FIXTURE_TAXDUMP_ARCHIVE_FILENAME)


def test_build_terms_from_taxdump_not_an_archive(tmp_path):
    """Tests the case where the named path exists but is not a gzipped tar."""
    archive_path = tmp_path / FIXTURE_TAXDUMP_ARCHIVE_FILENAME
    archive_path.write_text("2\t|\tBacteria\t|\n")

    with pytest.raises(duckdb.Error, match="gzip inflate failed"):
        build_terms_from_taxdump(archive_path)


def test_build_terms_from_taxdump_field_count_mismatch(tmp_path):
    """Tests the case where a row carries fewer fields than the member's
    documented column order declares."""
    archive_path = write_taxdump(tmp_path, names=[("2", "Bacteria", "")])

    with pytest.raises(duckdb.Error, match="expected exactly 4"):
        build_terms_from_taxdump(archive_path)
