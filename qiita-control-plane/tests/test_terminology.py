"""Unit tests for qiita_control_plane.terminology — manifest
parser/verifier, and the staging-dir import workflow.

A test that pins the position an error names writes its offending row after
a well-formed one, so a position counted off the wrong origin cannot pass.
"""

import hashlib
import json
from datetime import datetime

import duckdb
import pytest
from pydantic import ValidationError
from qiita_common.models import (
    MAX_TERMINOLOGY_VERSION_LENGTH,
    TerminologyManifest,
    TerminologyManifestFile,
    TerminologyStatus,
    TerminologyTermObsoletionKind,
)

from qiita_control_plane.repositories.terminology import (
    MAX_REPORTED_OFFENDERS,
    ParsedTerm,
    TerminologyImportAnomaly,
    TerminologyImportResult,
    fetch_terminology,
)
from qiita_control_plane.terminology import (
    CLOSURE_TSV_COLUMNS,
    CLOSURE_TSV_FILENAME,
    MANIFEST_FILENAME,
    MAX_TERM_ID_LENGTH,
    MAX_TERM_NAME_LENGTH,
    TERMS_TSV_COLUMNS,
    TERMS_TSV_FILENAME,
    _parse_closure_tsv,
    _parse_terms_tsv,
    import_terminology,
    load_manifest,
    sha256_of_file,
    verify_manifest_checksums,
    write_closure_tsv_stub,
    write_manifest,
    write_terms_tsv,
    write_tsv,
)
from qiita_control_plane.testing.terminology import parsed_term

# =============================================================================
# load_manifest
# =============================================================================


def _write_manifest_json(source_dir, payload: dict) -> None:
    (source_dir / MANIFEST_FILENAME).write_text(json.dumps(payload))


def _manifest_for(
    terms_sha256: str,
    closure_sha256: str,
    *,
    name: str = "uberon",
    version: str = "2026-04-15",
    terms_path: str = TERMS_TSV_FILENAME,
    closure_path: str = CLOSURE_TSV_FILENAME,
) -> TerminologyManifest:
    """A manifest declaring the two release tables at the given digests, under
    the canonical filenames unless the caller names others."""
    return TerminologyManifest(
        name=name,
        version=version,
        terms=TerminologyManifestFile(path=terms_path, sha256=terms_sha256),
        closure=TerminologyManifestFile(path=closure_path, sha256=closure_sha256),
    )


def test_load_manifest(tmp_path):
    """Tests the case where manifest.json is well formed: it parses into a
    TerminologyManifest."""
    payload = {
        "name": "uberon",
        "version": "2026-04-15",
        "terms": {"path": TERMS_TSV_FILENAME, "sha256": "a" * 64},
        "closure": {"path": CLOSURE_TSV_FILENAME, "sha256": "b" * 64},
    }
    _write_manifest_json(tmp_path, payload)

    result = load_manifest(tmp_path)

    expected = _manifest_for("a" * 64, "b" * 64)
    assert result == expected


def test_load_manifest_missing_file(tmp_path):
    """Tests the case where the staging directory holds no manifest.json: the
    read raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_manifest(tmp_path)


def _manifest_payload(version: str) -> dict:
    """A manifest payload differing from the default only in its version."""
    return {
        "name": "uberon",
        "version": version,
        "terms": {"path": TERMS_TSV_FILENAME, "sha256": "a" * 64},
        "closure": {"path": CLOSURE_TSV_FILENAME, "sha256": "b" * 64},
    }


def test_load_manifest_version_at_max_length(tmp_path):
    """Tests the case where the version is exactly as long as the column that
    stores it allows: the manifest parses."""
    version = "v" * MAX_TERMINOLOGY_VERSION_LENGTH
    _write_manifest_json(tmp_path, _manifest_payload(version))

    result = load_manifest(tmp_path)

    assert result.version == version


def test_load_manifest_version_over_max_length(tmp_path):
    """Tests the case where the version is one character longer than the column
    that stores it allows: the parse refuses it up front, rather than passing
    validation and failing against the column mid-load."""
    _write_manifest_json(tmp_path, _manifest_payload("v" * (MAX_TERMINOLOGY_VERSION_LENGTH + 1)))

    with pytest.raises(ValidationError):
        load_manifest(tmp_path)


# =============================================================================
# write_manifest / sha256_of_file
# =============================================================================


def test_write_manifest(tmp_path):
    """Tests the case where a manifest is written: it reads back through
    load_manifest unchanged."""
    manifest = _manifest_for("a" * 64, "b" * 64)

    write_manifest(tmp_path, manifest)

    assert load_manifest(tmp_path) == manifest


def test_sha256_of_file(tmp_path):
    """Tests the case where a file is hashed: the digest matches hashlib's
    over the same bytes."""
    content = b"term_id\tlabel\nUBERON:0001\tmouth\n"
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_bytes(content)

    result = sha256_of_file(path)

    assert result == hashlib.sha256(content).hexdigest()


def test_sha256_of_file_missing(tmp_path):
    """Tests the case where the file does not exist."""
    with pytest.raises(FileNotFoundError):
        sha256_of_file(tmp_path / "absent.tsv")


# =============================================================================
# verify_manifest_checksums
# =============================================================================


def _write_release_tables(source_dir, terms_content: bytes, closure_content: bytes) -> None:
    """Write both release tables with the given bytes."""
    (source_dir / TERMS_TSV_FILENAME).write_bytes(terms_content)
    (source_dir / CLOSURE_TSV_FILENAME).write_bytes(closure_content)


def test_verify_manifest_checksums(tmp_path):
    """Tests the case where both declared digests match the release tables on
    disk."""
    terms_content = b"term_id\tlabel\nUBERON:0001\tmouth\n"
    closure_content = b"ancestor_term_id\tdescendant_term_id\tdistance\n"
    _write_release_tables(tmp_path, terms_content, closure_content)
    manifest = _manifest_for(
        hashlib.sha256(terms_content).hexdigest(),
        hashlib.sha256(closure_content).hexdigest(),
    )

    # No exception is the success criterion.
    verify_manifest_checksums(tmp_path, manifest)


def test_verify_manifest_checksums_mismatch(tmp_path):
    """Tests the case where the terms table does not match its declared
    digest."""
    closure_content = b"ancestor_term_id\tdescendant_term_id\tdistance\n"
    _write_release_tables(tmp_path, b"actual content", closure_content)
    manifest = _manifest_for(
        "b" * 64,
        hashlib.sha256(closure_content).hexdigest(),
    )

    with pytest.raises(ValueError, match=TERMS_TSV_FILENAME):
        verify_manifest_checksums(tmp_path, manifest)


def test_verify_manifest_checksums_closure_mismatch(tmp_path):
    """Tests the case where the terms table verifies but the closure table
    does not, so the check does not skip a mismatch in the second table."""
    terms_content = b"term_id\tlabel\nUBERON:0001\tmouth\n"
    _write_release_tables(tmp_path, terms_content, b"actual content")
    manifest = _manifest_for(hashlib.sha256(terms_content).hexdigest(), "c" * 64)

    with pytest.raises(ValueError, match=CLOSURE_TSV_FILENAME):
        verify_manifest_checksums(tmp_path, manifest)


def test_verify_manifest_checksums_missing_table(tmp_path):
    """Tests the case where a declared release table is absent."""
    manifest = _manifest_for("c" * 64, "d" * 64)

    with pytest.raises(FileNotFoundError):
        verify_manifest_checksums(tmp_path, manifest)


# =============================================================================
# write_tsv
# =============================================================================


def test_write_tsv(tmp_path):
    """Tests the case where a table with cells needing every escape is written:
    the header leads the file, a None cell arrives empty, and a cell holding a
    delimiter, a quote, a newline, or a hash survives it. The hash is quoted
    though the format does not require it, since that decides the digest bytes."""
    path = tmp_path / "table.tsv"

    write_tsv(
        path,
        ("first", "second", "third"),
        [
            ("plain", None, "café"),
            ("has\ttab", 'has "quote"', "has\nnewline"),
            ("clone LGB#32", None, None),
        ],
    )

    expected = (
        'first\tsecond\tthird\nplain\t\tcafé\n"has\ttab"\t"has ""quote"""\t"has\nnewline"\n'
        '"clone LGB#32"\t\t\n'
    )
    assert path.read_text() == expected


def test_write_tsv_no_rows(tmp_path):
    """Tests the case where a table is written with no rows: the file holds its
    header and nothing else."""
    path = tmp_path / "table.tsv"

    write_tsv(path, ("first", "second"))

    assert path.read_text() == "first\tsecond\n"


@pytest.mark.parametrize("row", [("only", "two"), ("one", "two", "three", "four")])
def test_write_tsv_row_of_wrong_width(tmp_path, row):
    """Tests the case where a row does not carry one cell per column: the write
    refuses it, since cells are positional and the surplus or shortfall would
    otherwise land under the wrong column or drop one."""
    path = tmp_path / "table.tsv"

    with pytest.raises(ValueError):
        write_tsv(path, ("first", "second", "third"), [row])

    assert not path.exists()


def test_write_tsv_unwritable_destination(tmp_path):
    """Tests the case where the destination cannot be written: the writer
    reports it rather than leaving the caller a partial file."""
    path = tmp_path / "absent-directory" / "table.tsv"

    with pytest.raises(duckdb.Error):
        write_tsv(path, ("first", "second"), [("a", "b")])


# =============================================================================
# write_terms_tsv / write_closure_tsv_stub
# =============================================================================


def test_write_terms_tsv(tmp_path):
    """Tests the case where terms with and without obsoletion fields and
    with and without a second name are written: the file leads with the
    declared columns and reads back through the parser unchanged."""
    terms = [
        ParsedTerm(
            term_id="UBERON:0001",
            label="mouth",
            alternate_label=None,
            is_obsolete=False,
            replaced_by_term_id=None,
            obsoletion_kind=None,
        ),
        ParsedTerm(
            term_id="NCBI:9606",
            label="Homo sapiens",
            alternate_label="human",
            is_obsolete=False,
            replaced_by_term_id=None,
            obsoletion_kind=None,
        ),
        ParsedTerm(
            term_id="UBERON:0002",
            label="obsolete tooth",
            alternate_label=None,
            is_obsolete=True,
            replaced_by_term_id="UBERON:0001",
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
        ),
        # The writer quotes a hash, so a label carrying one proves the quoting
        # round-trips rather than reaching the database with its quotes.
        ParsedTerm(
            term_id="NCBI:40846",
            label="unidentified eubacterium clone LGB#32",
            alternate_label=None,
            is_obsolete=False,
            replaced_by_term_id=None,
            obsoletion_kind=None,
        ),
    ]
    path = tmp_path / TERMS_TSV_FILENAME

    write_terms_tsv(path, terms)

    assert path.read_text().splitlines()[0] == "\t".join(TERMS_TSV_COLUMNS)
    assert _parse_terms_tsv(path) == terms


def test__parse_terms_tsv_blank_label(tmp_path):
    """Tests the case where the label cell is blank: it parses to None rather
    than an empty string, so a source naming no term stays distinguishable from
    one naming it the empty string, leaving the load to decide what to store."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "NCBI:12\t\t\ttrue\t\tsource_deprecated\n"
    )

    result = _parse_terms_tsv(path)

    expected = [
        parsed_term(
            "NCBI:12",
            None,
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
        )
    ]
    assert result == expected


def test_write_terms_tsv_unnamed_term(tmp_path):
    """Tests the case where a term carries no label: the writer emits an empty
    cell, and the parser reads it back as None."""
    terms = [
        parsed_term(
            "NCBI:12",
            None,
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
        )
    ]
    path = tmp_path / TERMS_TSV_FILENAME

    write_terms_tsv(path, terms)

    assert _parse_terms_tsv(path) == terms


def test__parse_terms_tsv_blank_alternate_label(tmp_path):
    """Tests the case where the alternate_label cell is blank or holds only
    whitespace: both arrive as None, because the database spells an absent
    second name NULL rather than as an empty string."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "NCBI:9606\tHomo sapiens\t\tfalse\t\t\n"
        "NCBI:10090\tMus musculus\t   \tfalse\t\t\n"
    )

    result = _parse_terms_tsv(path)

    expected = [
        parsed_term("NCBI:9606", "Homo sapiens"),
        parsed_term("NCBI:10090", "Mus musculus"),
    ]
    assert result == expected


def test__parse_terms_tsv_strips_key_and_names(tmp_path):
    """Tests the case where the term id and both names arrive padded with
    whitespace: the parse strips each, because a plain btree index holds a
    padded variant as a value distinct from its unpadded twin."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "  NCBI:9606  \t  Homo sapiens  \t  human  \tfalse\t\t\n"
    )

    result = _parse_terms_tsv(path)

    expected = [parsed_term("NCBI:9606", "Homo sapiens", alternate_label="human")]
    assert result == expected


def test__parse_terms_tsv_strips_every_cell(tmp_path):
    """Tests the case where every cell of a row arrives padded with whitespace:
    the parse strips each, so a padded replacement pointer resolves against the
    term it names and a padded flag or kind reads as the value it spells."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "  UBERON:0001  \t  mouth  \t  oral opening  \tfalse\t\t\n"
        "  UBERON:0002  \t  tooth  \t\t  true  \t  UBERON:0001  \t  source_merged  \n"
    )

    result = _parse_terms_tsv(path)

    expected = [
        parsed_term("UBERON:0001", "mouth", alternate_label="oral opening"),
        parsed_term(
            "UBERON:0002",
            "tooth",
            is_obsolete=True,
            replaced_by_term_id="UBERON:0001",
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
        ),
    ]
    assert result == expected


@pytest.mark.parametrize(
    ("row_text", "cells_found"),
    [
        ("UBERON:0001\n", 1),
        ("UBERON:0001\tmouth\n", 2),
        ("UBERON:0001\tmouth\t\n", 3),
        ("UBERON:0001\tmouth\t\tfalse\n", 4),
        ("UBERON:0001\tmouth\t\tfalse\t\n", 5),
    ],
    ids=[
        "stops_before_label",
        "stops_before_alternate_label",
        "stops_before_is_obsolete",
        "stops_before_replaced_by_term_id",
        "stops_before_obsoletion_kind",
    ],
)
def test__parse_terms_tsv_short_row(tmp_path, row_text, cells_found):
    """Tests the case where a terms row carries fewer cells than the table
    declares columns: the parse refuses it, naming the row and both counts,
    rather than reading its later values under the wrong columns."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0009\tnostril\t\tfalse\t\t\n"
        f"{row_text}"
    )

    expected_error = f"(?s)Line: 3.*Expected Number of Columns: 6 Found: {cells_found}"
    with pytest.raises(duckdb.Error, match=expected_error):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_long_row(tmp_path):
    """Tests the case where a terms row runs past the last column: the parse
    refuses it, naming the row and both counts, since cells are positional and
    a surplus means the values sit under the wrong columns."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0009\tnostril\t\tfalse\t\t\n"
        "UBERON:0001\tmouth\t\tfalse\t\t\tEXTRA\n"
    )

    with pytest.raises(duckdb.Error, match="(?s)Line: 3.*Expected Number of Columns: 6 Found: 7"):
        _parse_terms_tsv(path)


@pytest.mark.parametrize(
    ("column", "max_length"),
    [
        ("term_id", MAX_TERM_ID_LENGTH),
        ("label", MAX_TERM_NAME_LENGTH),
        ("alternate_label", MAX_TERM_NAME_LENGTH),
        ("replaced_by_term_id", MAX_TERM_ID_LENGTH),
    ],
)
def test__parse_terms_tsv_over_long_value(tmp_path, column, max_length):
    """Tests the case where a cell holds more than its column can store: the
    parse refuses it, naming the row, the length found, and the length allowed,
    rather than reaching the database and failing there with nothing naming the
    row."""
    cells = {
        "term_id": "UBERON:0001",
        "label": "mouth",
        "alternate_label": "",
        "is_obsolete": "false",
        "replaced_by_term_id": "",
        "obsoletion_kind": "",
    }
    cells[column] = "x" * (max_length + 1)

    # Only an obsolete row carries a replacement pointer, and it must also
    # record why it is obsolete.
    if column == "replaced_by_term_id":
        cells["is_obsolete"] = "true"
        cells["obsoletion_kind"] = "source_merged"

    path = tmp_path / TERMS_TSV_FILENAME
    offending_row = "\t".join(cells[c] for c in TERMS_TSV_COLUMNS)
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0009\tnostril\t\tfalse\t\t\n"
        f"{offending_row}\n"
    )

    expected_error = f"row 2 carries a {column} of {max_length + 1} characters"
    with pytest.raises(ValueError, match=expected_error):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_value_at_the_limit(tmp_path):
    """Tests the case where a term id and both names are exactly as long as
    their columns allow: the row parses, so the bound refuses only what the
    database could not store."""
    term_id = "x" * MAX_TERM_ID_LENGTH
    label = "y" * MAX_TERM_NAME_LENGTH
    alternate_label = "z" * MAX_TERM_NAME_LENGTH
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        f"{term_id}\t{label}\t{alternate_label}\tfalse\t\t\n"
    )

    result = _parse_terms_tsv(path)

    expected = [parsed_term(term_id, label, alternate_label=alternate_label)]
    assert result == expected


def test__parse_terms_tsv_round_trip_normalizes(tmp_path):
    """Tests the case where terms carry an empty, whitespace-only, or padded
    name: the rows read back out of the written table equal the rows written
    into it, so a release assembled in memory and the same release read from
    its files cannot disagree."""
    path = tmp_path / TERMS_TSV_FILENAME
    terms = [
        parsed_term("UBERON:0001", "", alternate_label="   "),
        parsed_term("UBERON:0002", "   ", alternate_label="  oral opening  "),
        parsed_term("UBERON:0003", "  molar  ", replaced_by_term_id="  UBERON:0001  "),
        parsed_term("UBERON:0004", "mouth"),
    ]

    write_terms_tsv(path, terms)
    result = _parse_terms_tsv(path)

    expected = [
        parsed_term("UBERON:0001", None),
        parsed_term("UBERON:0002", None, alternate_label="oral opening"),
        parsed_term("UBERON:0003", "molar", replaced_by_term_id="UBERON:0001"),
        parsed_term("UBERON:0004", "mouth"),
    ]
    assert result == expected
    assert result == terms


def test__parse_terms_tsv_empty_term_id(tmp_path):
    """Tests the case where a term id cell holds nothing but whitespace: the
    parse refuses it, naming the row, since the database keys the row by that
    value and every row referencing the term spells it unpadded."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0009\tnostril\t\tfalse\t\t\n"
        "   \tmouth\t\tfalse\t\t\n"
    )

    with pytest.raises(ValueError, match="row 2 carries an empty term_id"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_invalid_is_obsolete(tmp_path):
    """Tests the case where an is_obsolete cell spells neither boolean value:
    the parse refuses it, naming the row, the value, and the two spellings it
    accepts, rather than coercing the row to not-obsolete."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0001\tmouth\t\tyes\t\t\n"
    )

    with pytest.raises(ValueError, match="row 1.*is_obsolete.*'yes'"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_unrecognized_obsoletion_kind(tmp_path):
    """Tests the case where an obsoletion_kind cell names a kind the enum does
    not carry: the parse refuses it, naming the row and the value, rather than
    leaving the enum cast to fail without saying which row it read."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0002\ttooth\t\ttrue\t\tsource_vanished\n"
    )

    with pytest.raises(ValueError, match="row 1.*obsoletion_kind 'source_vanished'"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_missing_column(tmp_path):
    """Tests the case where the terms table was written against an earlier
    column set: the parse refuses it up front, rather than silently reading
    every row as having no second name."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0001\tmouth\tfalse\t\t\n"
    )

    with pytest.raises(duckdb.Error, match="Expected Number of Columns: 6 Found: 5"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_reordered_header(tmp_path):
    """Tests the case where the terms table names every declared column but in
    another order: the parse refuses it, naming the order found and the order
    expected, since the read maps columns positionally and each cell would
    otherwise land under the wrong one."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "label\tterm_id\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "mouth\tUBERON:0001\t\tfalse\t\t\n"
    )

    with pytest.raises(ValueError, match="is headed by.*expected.*in that order"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_no_header(tmp_path):
    """Tests the case where the terms table is wholly empty: the parse refuses
    it for carrying no header, rather than reading it as a table of no rows."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text("")

    with pytest.raises(ValueError, match="carries no header"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_duplicate_term_id(tmp_path):
    """Tests the case where one term id occupies two rows of the terms table:
    the parse refuses it, naming the id, because the release contradicts itself
    about one term and no upsert can apply two rows to one key."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0001\tmouth\t\tfalse\t\t\n"
        "UBERON:0001\toral opening\t\tfalse\t\t\n"
    )

    with pytest.raises(ValueError, match="UBERON:0001"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_duplicate_term_id_reports_all(tmp_path):
    """Tests the case where several term ids are duplicated: one sorted error
    names every offending id, so a whole table takes one correction pass rather
    than one id per run."""
    path = tmp_path / TERMS_TSV_FILENAME
    header = "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
    body = "".join(
        f"{term_id}\t{term_id} label\t\tfalse\t\t\n"
        for term_id in (
            "UBERON:0003",
            "UBERON:0001",
            "UBERON:0003",
            "UBERON:0001",
            "UBERON:0002",
        )
    )
    path.write_text(header + body)

    with pytest.raises(ValueError) as excinfo:
        _parse_terms_tsv(path)

    message = str(excinfo.value)
    assert "['UBERON:0001', 'UBERON:0003']" in message
    assert "UBERON:0002" not in message


def test__parse_terms_tsv_duplicate_term_id_over_cap(tmp_path):
    """Tests the case where more term ids are duplicated than the error names:
    the error states the total and leaves the tail unnamed, so a wholly corrupt
    table yields a readable error rather than one line per offending id."""
    over_cap_count = MAX_REPORTED_OFFENDERS + 5
    term_ids = [f"UBERON:{i:04d}" for i in range(over_cap_count)]
    path = tmp_path / TERMS_TSV_FILENAME
    header = "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
    body = "".join(f"{term_id}\t{term_id} label\t\tfalse\t\t\n" for term_id in term_ids * 2)
    path.write_text(header + body)

    with pytest.raises(ValueError) as excinfo:
        _parse_terms_tsv(path)

    message = str(excinfo.value)
    assert f"{over_cap_count} total, first {MAX_REPORTED_OFFENDERS}" in message
    assert term_ids[-1] not in message


def test_write_closure_tsv_stub(tmp_path):
    """Tests the case where a closure table is written with no rows: the file
    holds only its header and parses to no closure tuples."""
    path = tmp_path / CLOSURE_TSV_FILENAME

    write_closure_tsv_stub(path)

    assert path.read_text().splitlines() == ["\t".join(CLOSURE_TSV_COLUMNS)]
    assert _parse_closure_tsv(path) == []


def test__parse_closure_tsv_duplicate_pair(tmp_path):
    """Tests the case where one ancestor/descendant pair occupies two rows of
    the closure table: the parse refuses it, naming the pair, because the
    database holds that pair unique."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\n"
        "UBERON:0001\tUBERON:0002\t1\n"
        "UBERON:0001\tUBERON:0002\t1\n"
    )

    with pytest.raises(ValueError, match="UBERON:0002"):
        _parse_closure_tsv(path)


def test__parse_closure_tsv_duplicate_pair_differing_distance(tmp_path):
    """Tests the case where two closure rows name one pair at two distances:
    the parse refuses it just as for an exact repeat, because the database
    holds the pair unique without regard to distance."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\n"
        "UBERON:0001\tUBERON:0002\t1\n"
        "UBERON:0001\tUBERON:0002\t2\n"
    )

    with pytest.raises(ValueError, match="UBERON:0002"):
        _parse_closure_tsv(path)


def test__parse_terms_tsv_obsolete_without_kind(tmp_path):
    """Tests the case where a row is obsolete but names no obsoletion kind: the
    parse refuses it, naming the row and its line, because the database
    requires an obsolete term to record why it is obsolete."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0003\tobsolete molar\t\ttrue\t\t\n"
    )

    with pytest.raises(ValueError, match="row 1.*UBERON:0003"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_kind_on_live_row(tmp_path):
    """Tests the case where a live row carries an obsoletion kind: the parse
    refuses it, naming the row and its line, because the database allows a kind
    only on an obsolete term."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0001\tmouth\t\tfalse\t\tsource_merged\n"
    )

    with pytest.raises(ValueError, match="row 1.*UBERON:0001"):
        _parse_terms_tsv(path)


def test__parse_closure_tsv_missing_column(tmp_path):
    """Tests the case where the closure table was written against a different
    column set: the parse refuses it up front, naming the absent column, rather
    than failing per row on a key it cannot find."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text("ancestor\tdescendant\tdistance\nUBERON:0001\tUBERON:0002\t1\n")

    with pytest.raises(ValueError, match="ancestor_term_id"):
        _parse_closure_tsv(path)


def test__parse_closure_tsv_strips_every_cell(tmp_path):
    """Tests the case where every cell of a row arrives padded with whitespace:
    the parse strips each, so the pair matches the term rows it relates rather
    than the rebuild dropping it for naming a term id nothing spells that way,
    and the distance reads as the number it spells."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\n  UBERON:0001  \t  UBERON:0002  \t  1  \n"
    )

    result = _parse_closure_tsv(path)

    assert result == [("UBERON:0001", "UBERON:0002", 1)]


@pytest.mark.parametrize(
    ("ancestor_cell", "descendant_cell", "expected_column"),
    [
        ("   ", "UBERON:0002", "ancestor_term_id"),
        ("UBERON:0001", "   ", "descendant_term_id"),
    ],
    ids=["ancestor", "descendant"],
)
def test__parse_closure_tsv_empty_endpoint(
    tmp_path, ancestor_cell, descendant_cell, expected_column
):
    """Tests the case where one endpoint cell holds nothing but whitespace: the
    parse refuses it, naming that column and the row it is on, since a closure
    row can only relate terms the release defines."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\n"
        "UBERON:0008\tUBERON:0009\t1\n"
        f"{ancestor_cell}\t{descendant_cell}\t1\n"
    )

    with pytest.raises(ValueError, match=f"row 2 carries an empty {expected_column}"):
        _parse_closure_tsv(path)


def test__parse_closure_tsv_short_row(tmp_path):
    """Tests the case where a closure row stops before its distance: the parse
    refuses it, naming the row and both counts, rather than raising on an
    absent cell."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\n"
        "UBERON:0008\tUBERON:0009\t1\n"
        "UBERON:0001\tUBERON:0002\n"
    )

    with pytest.raises(duckdb.Error, match="(?s)Line: 3.*Expected Number of Columns: 3 Found: 2"):
        _parse_closure_tsv(path)


def test__parse_closure_tsv_long_row(tmp_path):
    """Tests the case where a closure row runs past its distance: the parse
    refuses it, naming the row and both counts, rather than reading a row whose
    endpoints and distance sit under the wrong columns."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\n"
        "UBERON:0008\tUBERON:0009\t1\n"
        "UBERON:0001\tUBERON:0002\t1\t2\n"
    )

    with pytest.raises(duckdb.Error, match="(?s)Line: 3.*Expected Number of Columns: 3 Found: 4"):
        _parse_closure_tsv(path)


def test__parse_closure_tsv_non_integer_distance(tmp_path):
    """Tests the case where a distance cell is not a number: the parse refuses
    it, naming the row and the pair it belongs to."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\nUBERON:0001\tUBERON:0002\tone\n"
    )

    with pytest.raises(ValueError, match="row 1.*UBERON:0002"):
        _parse_closure_tsv(path)


def test__parse_closure_tsv_negative_distance(tmp_path):
    """Tests the case where a distance is negative: the parse refuses it, naming
    the line and the pair, because the database holds distance non-negative."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\nUBERON:0001\tUBERON:0002\t-1\n"
    )

    with pytest.raises(ValueError, match="row 1.*UBERON:0002"):
        _parse_closure_tsv(path)


# =============================================================================
# import_terminology — staging-dir-driven import workflow
# =============================================================================


def _write_staging(
    staging_dir,
    *,
    name: str,
    version: str,
    terms: list[ParsedTerm],
    closure: list[tuple[str, str, int]],
    terms_filename: str = TERMS_TSV_FILENAME,
    closure_filename: str = CLOSURE_TSV_FILENAME,
) -> None:
    """Write the manifest and the two release tables into `staging_dir`, under
    the canonical filenames unless the caller names others. closure rows are
    (ancestor_term_id, descendant_term_id, distance). The manifest declares the
    digests of the two tables actually written, so the checksum verification the
    import performs is real.

    Both headers appear here in full rather than coming from the writers' own
    constants, so a rename of a declared column fails these tests instead of
    passing tautologically.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)

    write_tsv(
        staging_dir / terms_filename,
        (
            "term_id",
            "label",
            "alternate_label",
            "is_obsolete",
            "replaced_by_term_id",
            "obsoletion_kind",
        ),
        [
            (
                term.term_id,
                term.label,
                term.alternate_label,
                "true" if term.is_obsolete else "false",
                term.replaced_by_term_id,
                str(term.obsoletion_kind) if term.obsoletion_kind is not None else None,
            )
            for term in terms
        ],
    )

    write_tsv(
        staging_dir / closure_filename,
        ("ancestor_term_id", "descendant_term_id", "distance"),
        [(ancestor, descendant, str(distance)) for ancestor, descendant, distance in closure],
    )

    # Digest the tables after writing them, so the manifest describes what
    # landed on disk rather than what was intended.
    manifest = _manifest_for(
        sha256_of_file(staging_dir / terms_filename),
        sha256_of_file(staging_dir / closure_filename),
        name=name,
        version=version,
        terms_path=terms_filename,
        closure_path=closure_filename,
    )
    write_manifest(staging_dir, manifest)


async def _read_term_state(pool, terminology_idx: int) -> dict:
    """Returns {term_id: {label, alternate_label, is_obsolete,
    obsoletion_kind, obsoleted_in_version, replaced_by_term_id, notes,
    idx}} for every term in the terminology."""
    rows = await pool.fetch(
        "SELECT t.idx, t.term_id, t.label, t.alternate_label, t.is_obsolete,"
        "       t.obsoletion_kind, t.obsoleted_in_version, t.notes,"
        "       r.term_id AS replaced_by_term_id"
        "  FROM qiita.terminology_term t"
        "  LEFT JOIN qiita.terminology_term r ON t.replaced_by = r.idx"
        " WHERE t.terminology_idx = $1",
        terminology_idx,
    )
    return {row["term_id"]: dict(row) for row in rows}


def _expected_term_state(
    idx_source: dict,
    term_id: str,
    *,
    label: str,
    alternate_label: str | None = None,
    is_obsolete: bool = False,
    obsoletion_kind: TerminologyTermObsoletionKind | None = None,
    obsoleted_in_version: str | None = None,
    replaced_by_term_id: str | None = None,
    notes: str | None = None,
) -> dict:
    """One expected _read_term_state entry, for whole-dict comparison.

    `idx` is auto-generated, so it comes from `idx_source` — the actual state
    whose idxs the comparison should match. Passing an earlier load's state
    there folds idx preservation into the equality.
    """
    return {
        "idx": idx_source[term_id]["idx"],
        "term_id": term_id,
        "label": label,
        "alternate_label": alternate_label,
        "is_obsolete": is_obsolete,
        "obsoletion_kind": obsoletion_kind.value if obsoletion_kind is not None else None,
        "obsoleted_in_version": obsoleted_in_version,
        "notes": notes,
        "replaced_by_term_id": replaced_by_term_id,
    }


def _expected_terminology_row(
    terminology_idx: int,
    *,
    name: str,
    version: str,
    status: TerminologyStatus,
    loaded_at: datetime,
) -> dict:
    """The expected fetch_terminology projection for one terminology row.

    `loaded_at` is a parameter because a load stamps it server-side; pass the
    actual value, or an earlier read's value to assert it did not move.
    """
    return {
        "terminology_idx": terminology_idx,
        "name": name,
        "version": version,
        "loaded_at": loaded_at,
        "status": status.value,
    }


@pytest.mark.db
async def test_import_terminology(postgres_pool, created_terminologies, tmp_path):
    """Tests the case where a brand-new staging directory is loaded: the load
    inserts every term, writes every closure row, and leaves the terminology
    row in ACTIVE."""
    _write_staging(
        tmp_path,
        name="ldt_brand_new",
        version="1.0.0",
        terms=[
            parsed_term("UBERON:0001", "mouth"),
            parsed_term("UBERON:0002", "tooth"),
            parsed_term("UBERON:0003", "molar"),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
            ("UBERON:0003", "UBERON:0003", 0),
            ("UBERON:0001", "UBERON:0002", 1),
            ("UBERON:0001", "UBERON:0003", 2),
            ("UBERON:0002", "UBERON:0003", 1),
        ],
    )

    result = await import_terminology(postgres_pool, tmp_path)
    created_terminologies.append(result.terminology_idx)

    expected = TerminologyImportResult(
        terminology_idx=result.terminology_idx,
        terms_inserted=3,
        terms_label_updated=0,
        terms_alternate_label_updated=0,
        terms_newly_obsoleted=0,
        terms_newly_merged=0,
        terms_silently_dropped=0,
        closure_rows=6,
    )
    assert result == expected

    terminology_row = await fetch_terminology(postgres_pool, result.terminology_idx)
    expected_row = _expected_terminology_row(
        result.terminology_idx,
        name="ldt_brand_new",
        version="1.0.0",
        status=TerminologyStatus.ACTIVE,
        loaded_at=terminology_row["loaded_at"],
    )
    assert dict(terminology_row) == expected_row


@pytest.mark.db
async def test_import_terminology_reload_preserves_idx(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a terminology is reloaded at a new version:
    terminology_idx and the per-term idxs survive, and a relabeled term counts
    as terms_label_updated."""
    # First load establishes the terminology and term idxs.
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_reload",
        version="1.0.0",
        terms=[
            parsed_term("UBERON:0001", "mouth"),
            parsed_term("UBERON:0002", "tooth"),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0), ("UBERON:0002", "UBERON:0002", 0)],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)
    v1_state = await _read_term_state(postgres_pool, v1_result.terminology_idx)

    # Second load relabels one term; terminology_idx and per-term idxs persist.
    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_reload",
        version="2.0.0",
        terms=[
            parsed_term("UBERON:0001", "oral opening"),
            parsed_term("UBERON:0002", "tooth"),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0), ("UBERON:0002", "UBERON:0002", 0)],
    )
    v2_result = await import_terminology(postgres_pool, v2_dir)

    assert v2_result.terminology_idx == v1_result.terminology_idx
    expected = TerminologyImportResult(
        terminology_idx=v1_result.terminology_idx,
        terms_inserted=0,
        terms_label_updated=1,
        terms_alternate_label_updated=0,
        terms_newly_obsoleted=0,
        terms_newly_merged=0,
        terms_silently_dropped=0,
        closure_rows=2,
    )
    assert v2_result == expected

    # The idxs come from v1_state, so this equality also proves they persisted.
    v2_state = await _read_term_state(postgres_pool, v2_result.terminology_idx)
    expected_v2_state = {
        "UBERON:0001": _expected_term_state(v1_state, "UBERON:0001", label="oral opening"),
        "UBERON:0002": _expected_term_state(v1_state, "UBERON:0002", label="tooth"),
    }
    assert v2_state == expected_v2_state

    terminology_row = await fetch_terminology(postgres_pool, v2_result.terminology_idx)
    expected_row = _expected_terminology_row(
        v2_result.terminology_idx,
        name="ldt_reload",
        version="2.0.0",
        status=TerminologyStatus.ACTIVE,
        loaded_at=terminology_row["loaded_at"],
    )
    assert dict(terminology_row) == expected_row


@pytest.mark.db
async def test_import_terminology_alternate_label(postgres_pool, created_terminologies, tmp_path):
    """Tests the case where a release supplies a second name for some of its
    terms: the value lands on those rows and the terms without one keep
    NULL, so absence stays distinguishable from an empty name."""
    _write_staging(
        tmp_path,
        name="ldt_alternate_label",
        version="1.0.0",
        terms=[
            parsed_term("9606", "Homo sapiens", alternate_label="human"),
            parsed_term("10090", "Mus musculus", alternate_label="house mouse"),
            parsed_term("256318", "metagenome"),
        ],
        closure=[],
    )

    result = await import_terminology(postgres_pool, tmp_path)
    created_terminologies.append(result.terminology_idx)

    state = await _read_term_state(postgres_pool, result.terminology_idx)
    expected_state = {
        "9606": _expected_term_state(state, "9606", label="Homo sapiens", alternate_label="human"),
        "10090": _expected_term_state(
            state, "10090", label="Mus musculus", alternate_label="house mouse"
        ),
        "256318": _expected_term_state(state, "256318", label="metagenome"),
    }
    assert state == expected_state


@pytest.mark.db
async def test_import_terminology_alternate_label_release_authoritative(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a second release changes one term's second name
    and supplies none for another that had one: the load overwrites the changed
    value and clears the omitted one to NULL.

    The release is authoritative for alternate_label exactly as it is for
    label, so a source that stops offering a second name can say so. A term the
    release leaves untouched keeps the value it already had.
    """
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_alternate_label_authoritative",
        version="1.0.0",
        terms=[
            parsed_term("9606", "Homo sapiens", alternate_label="human"),
            parsed_term("10090", "Mus musculus", alternate_label="house mouse"),
            parsed_term("9598", "Pan troglodytes", alternate_label="chimpanzee"),
        ],
        closure=[],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)

    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_alternate_label_authoritative",
        version="2.0.0",
        terms=[
            parsed_term("9606", "Homo sapiens", alternate_label="man"),
            parsed_term("10090", "Mus musculus"),
            parsed_term("9598", "Pan troglodytes", alternate_label="chimpanzee"),
        ],
        closure=[],
    )
    await import_terminology(postgres_pool, v2_dir)

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {
        "9606": _expected_term_state(state, "9606", label="Homo sapiens", alternate_label="man"),
        "10090": _expected_term_state(state, "10090", label="Mus musculus"),
        "9598": _expected_term_state(
            state, "9598", label="Pan troglodytes", alternate_label="chimpanzee"
        ),
    }
    assert state == expected_state


@pytest.mark.db
async def test_import_terminology_obsoleted_in_version_set_once(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a term is active in v1, obsoleted in v2, and still
    obsolete in v3: v2 stamps obsoleted_in_version, and no subsequent reload
    advances it."""
    for version, terms in [
        ("1.0.0", [parsed_term("UBERON:0001", "mouth")]),
        (
            "2.0.0",
            [
                parsed_term(
                    "UBERON:0001",
                    "mouth",
                    is_obsolete=True,
                    obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
                )
            ],
        ),
        (
            "3.0.0",
            [
                parsed_term(
                    "UBERON:0001",
                    "mouth",
                    is_obsolete=True,
                    obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
                )
            ],
        ),
    ]:
        version_dir = tmp_path / version
        _write_staging(
            version_dir,
            name="ldt_set_once",
            version=version,
            terms=terms,
            closure=[("UBERON:0001", "UBERON:0001", 0)],
        )
        result = await import_terminology(postgres_pool, version_dir)
        created_terminologies.append(result.terminology_idx)

    state = await _read_term_state(postgres_pool, result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(
            state,
            "UBERON:0001",
            label="mouth",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
            obsoleted_in_version="2.0.0",
        )
    }
    assert state == expected_state


@pytest.mark.db
async def test_import_terminology_un_obsoletion_clears_columns(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a term obsolete in v1 is reloaded as non-obsolete in
    v2: the load clears is_obsolete, obsoletion_kind, obsoleted_in_version, and
    replaced_by."""
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_un_obsolete",
        version="1.0.0",
        terms=[
            parsed_term(
                "UBERON:0001",
                "mouth",
                is_obsolete=True,
                obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
            ),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)

    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_un_obsolete",
        version="2.0.0",
        terms=[parsed_term("UBERON:0001", "mouth")],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )
    await import_terminology(postgres_pool, v2_dir)

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {"UBERON:0001": _expected_term_state(state, "UBERON:0001", label="mouth")}
    assert state == expected_state


@pytest.mark.db
async def test_import_terminology_re_obsoletion_stamps_new_version(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a term is obsoleted, un-obsoleted, then obsoleted
    again, and the second obsoletion stamps obsoleted_in_version with the
    later release rather than restoring the earlier one.

    The set-once rule holds within one obsoletion episode, not across the
    row's whole lifetime: un-obsoletion NULLs the column, so the COALESCE in
    the UPSERT finds nothing to preserve on the next obsoletion and takes the
    loading version. Asserting the v2 state before v3 runs keeps a v2 that
    failed to stamp from letting the v4 expectation pass vacuously.
    """
    obsolete_term = parsed_term(
        "UBERON:0001",
        "mouth",
        is_obsolete=True,
        obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
    )
    active_term = parsed_term("UBERON:0001", "mouth")

    async def _load(version: str, term) -> int:
        version_dir = tmp_path / version
        _write_staging(
            version_dir,
            name="ldt_re_obsolete",
            version=version,
            terms=[term],
            closure=[("UBERON:0001", "UBERON:0001", 0)],
        )
        result = await import_terminology(postgres_pool, version_dir)
        return result.terminology_idx

    # v1 active, then v2 obsoletes it — stamping this episode's first version.
    terminology_idx = await _load("1.0.0", active_term)
    created_terminologies.append(terminology_idx)
    await _load("2.0.0", obsolete_term)

    after_v2 = await _read_term_state(postgres_pool, terminology_idx)
    expected_after_v2 = {
        "UBERON:0001": _expected_term_state(
            after_v2,
            "UBERON:0001",
            label="mouth",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
            obsoleted_in_version="2.0.0",
        )
    }
    assert after_v2 == expected_after_v2

    # v3 un-obsoletes (clearing the stamp), v4 obsoletes again.
    await _load("3.0.0", active_term)
    await _load("4.0.0", obsolete_term)

    after_v4 = await _read_term_state(postgres_pool, terminology_idx)
    expected_after_v4 = {
        "UBERON:0001": _expected_term_state(
            after_v4,
            "UBERON:0001",
            label="mouth",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
            obsoleted_in_version="4.0.0",
        )
    }
    assert after_v4 == expected_after_v4


@pytest.mark.db
async def test_import_terminology_kind_and_replaced_by_change(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a term obsoleted in v1 as 'deprecated' is reloaded
    in v2 as 'merged' into a new survivor: obsoletion_kind flips, replaced_by
    points at the survivor, obsoleted_in_version remains v1, and
    terms_newly_merged counts the kind flip even though the row was already
    obsolete."""
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_kind_change",
        version="1.0.0",
        terms=[
            parsed_term(
                "UBERON:0001",
                "old",
                is_obsolete=True,
                obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
            ),
            parsed_term("UBERON:0002", "sibling"),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
        ],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)

    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_kind_change",
        version="2.0.0",
        terms=[
            parsed_term(
                "UBERON:0001",
                "old",
                is_obsolete=True,
                replaced_by_term_id="UBERON:0003",
                obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
            parsed_term("UBERON:0002", "sibling"),
            parsed_term("UBERON:0003", "survivor"),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
            ("UBERON:0003", "UBERON:0003", 0),
        ],
    )
    v2_result = await import_terminology(postgres_pool, v2_dir)

    # terms_newly_merged counts the kind flip on UBERON:0001 even though the
    # row was already obsolete from v1, so terms_newly_obsoleted stays 0.
    expected = TerminologyImportResult(
        terminology_idx=v1_result.terminology_idx,
        terms_inserted=1,
        terms_label_updated=0,
        terms_alternate_label_updated=0,
        terms_newly_obsoleted=0,
        terms_newly_merged=1,
        terms_silently_dropped=0,
        closure_rows=3,
    )
    assert v2_result == expected

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(
            state,
            "UBERON:0001",
            label="old",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            obsoleted_in_version="1.0.0",
            replaced_by_term_id="UBERON:0003",
        ),
        "UBERON:0002": _expected_term_state(state, "UBERON:0002", label="sibling"),
        "UBERON:0003": _expected_term_state(state, "UBERON:0003", label="survivor"),
    }
    assert state == expected_state


@pytest.mark.db
async def test_import_terminology_cross_terminology_closure_untouched(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a second, unrelated terminology is loaded: the
    closure rows of a previously loaded sentinel terminology survive
    untouched."""
    sentinel_dir = tmp_path / "sentinel"
    _write_staging(
        sentinel_dir,
        name="ldt_sentinel",
        version="1.0.0",
        terms=[
            parsed_term("SENT:0001", "alpha"),
            parsed_term("SENT:0002", "beta"),
        ],
        closure=[
            ("SENT:0001", "SENT:0001", 0),
            ("SENT:0002", "SENT:0002", 0),
            ("SENT:0001", "SENT:0002", 1),
        ],
    )
    sentinel_result = await import_terminology(postgres_pool, sentinel_dir)
    created_terminologies.append(sentinel_result.terminology_idx)

    other_dir = tmp_path / "other"
    _write_staging(
        other_dir,
        name="ldt_other",
        version="1.0.0",
        terms=[parsed_term("OTHER:0001", "gamma")],
        closure=[("OTHER:0001", "OTHER:0001", 0)],
    )
    other_result = await import_terminology(postgres_pool, other_dir)
    created_terminologies.append(other_result.terminology_idx)

    sentinel_closure_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.terminology_closure WHERE terminology_idx = $1",
        sentinel_result.terminology_idx,
    )
    assert sentinel_closure_count == 3


@pytest.mark.db
async def test_import_terminology_silent_drops_raise_and_preserve_state(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a v2 staging directory omits a term present in v1
    with no explicit deprecation marker: the load raises
    TerminologyImportAnomaly listing the silently-dropped term, and the v1 row
    and term set survive intact because the v2 transaction rolls back."""
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_silent_drops",
        version="1.0.0",
        terms=[
            parsed_term("UBERON:0001", "mouth"),
            parsed_term("UBERON:0002", "tooth"),
            parsed_term("UBERON:0003", "molar"),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
            ("UBERON:0003", "UBERON:0003", 0),
        ],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)
    pre_load_row = await fetch_terminology(postgres_pool, v1_result.terminology_idx)

    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_silent_drops",
        version="2.0.0",
        terms=[
            parsed_term("UBERON:0001", "mouth"),
            parsed_term("UBERON:0002", "tooth"),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
        ],
    )

    with pytest.raises(TerminologyImportAnomaly) as exc_info:
        await import_terminology(postgres_pool, v2_dir)
    assert exc_info.value.silently_dropped_term_ids == ["UBERON:0003"]

    # The transaction rolled back: status, version, and loaded_at stand as v1
    # left them, and UBERON:0003 is still present and active.
    post_attempt_row = await fetch_terminology(postgres_pool, v1_result.terminology_idx)
    expected_row = _expected_terminology_row(
        v1_result.terminology_idx,
        name="ldt_silent_drops",
        version="1.0.0",
        status=TerminologyStatus.ACTIVE,
        loaded_at=pre_load_row["loaded_at"],
    )
    assert dict(post_attempt_row) == expected_row

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(state, "UBERON:0001", label="mouth"),
        "UBERON:0002": _expected_term_state(state, "UBERON:0002", label="tooth"),
        "UBERON:0003": _expected_term_state(state, "UBERON:0003", label="molar"),
    }
    assert state == expected_state


@pytest.mark.db
async def test_import_terminology_unresolved_replaced_by_raises(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a term names a replaced_by target declared nowhere
    in the TSV: TerminologyImportAnomaly carries the exact (obsolete_term_id,
    attempted_replaced_by) pair, and nothing reaches the database."""
    staging_dir = tmp_path / "stage"
    _write_staging(
        staging_dir,
        name="ldt_unresolved",
        version="1.0.0",
        terms=[
            parsed_term(
                "UBERON:0001",
                "orphan",
                is_obsolete=True,
                replaced_by_term_id="UBERON:9999",
                obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )

    with pytest.raises(TerminologyImportAnomaly) as exc_info:
        await import_terminology(postgres_pool, staging_dir)
    assert exc_info.value.unresolved_replaced_by == [("UBERON:0001", "UBERON:9999")]

    # Nothing landed — the name lookup returns no row.
    row = await postgres_pool.fetchrow(
        "SELECT idx FROM qiita.terminology WHERE name = $1", "ldt_unresolved"
    )
    assert row is None


@pytest.mark.db
async def test_import_terminology_unresolved_closure_endpoint_raises(postgres_pool, tmp_path):
    """Tests the case where a closure row names a term the release does not
    define: the load refuses it, naming the pair, and writes nothing, because a
    closure can only relate terms of the terminology that carries it."""
    staging_dir = tmp_path / "stage"
    _write_staging(
        staging_dir,
        name="ldt_closure_dangling",
        version="1.0.0",
        terms=[parsed_term("UBERON:0001", "mouth")],
        closure=[("UBERON:0001", "UBERON:0001", 0), ("UBERON:0001", "UBERON:9999", 1)],
    )

    with pytest.raises(TerminologyImportAnomaly) as exc_info:
        await import_terminology(postgres_pool, staging_dir)
    assert exc_info.value.unresolved_closure_endpoints == [("UBERON:0001", "UBERON:9999")]

    row = await postgres_pool.fetchrow(
        "SELECT idx FROM qiita.terminology WHERE name = $1", "ldt_closure_dangling"
    )
    assert row is None


@pytest.mark.db
async def test_import_terminology_tolerate_unresolved_closure_endpoint(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where the same dangling closure row is tolerated: the load
    succeeds and the reported closure count covers only the rows that resolved."""
    staging_dir = tmp_path / "stage"
    _write_staging(
        staging_dir,
        name="ldt_closure_tolerated",
        version="1.0.0",
        terms=[parsed_term("UBERON:0001", "mouth")],
        closure=[("UBERON:0001", "UBERON:0001", 0), ("UBERON:0001", "UBERON:9999", 1)],
    )

    result = await import_terminology(postgres_pool, staging_dir, tolerate_anomalies=True)
    created_terminologies.append(result.terminology_idx)

    expected = TerminologyImportResult(
        terminology_idx=result.terminology_idx,
        terms_inserted=1,
        terms_label_updated=0,
        terms_alternate_label_updated=0,
        terms_newly_obsoleted=0,
        terms_newly_merged=0,
        terms_silently_dropped=0,
        closure_rows=1,
    )
    assert result == expected


@pytest.mark.db
async def test_import_terminology_declared_table_paths(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where the manifest declares filenames other than the
    canonical ones: the load reads the tables from the declared paths, the same
    paths the digest check covered."""
    staging_dir = tmp_path / "stage"
    _write_staging(
        staging_dir,
        name="ldt_declared_paths",
        version="1.0.0",
        terms=[parsed_term("UBERON:0001", "mouth")],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
        terms_filename="uberon-terms.tsv",
        closure_filename="uberon-closure.tsv",
    )

    result = await import_terminology(postgres_pool, staging_dir)
    created_terminologies.append(result.terminology_idx)

    expected = TerminologyImportResult(
        terminology_idx=result.terminology_idx,
        terms_inserted=1,
        terms_label_updated=0,
        terms_alternate_label_updated=0,
        terms_newly_obsoleted=0,
        terms_newly_merged=0,
        terms_silently_dropped=0,
        closure_rows=1,
    )
    assert result == expected


def _misaligned_staging(staging_dir, *, name: str) -> None:
    """Stage a release whose live term carries a replacement pointer, which
    only an obsolete term may do."""
    _write_staging(
        staging_dir,
        name=name,
        version="1.0.0",
        terms=[
            parsed_term("UBERON:0001", "mouth", replaced_by_term_id="UBERON:0002"),
            parsed_term("UBERON:0002", "tooth"),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0), ("UBERON:0002", "UBERON:0002", 0)],
    )


@pytest.mark.db
async def test_import_terminology_misaligned_replaced_by_raises(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a term that is not obsolete names a replacement:
    TerminologyImportAnomaly carries the exact (term_id, attempted_target)
    pair and nothing reaches the database, because only an obsolete term may
    point at a successor."""
    staging_dir = tmp_path / "stage"
    _misaligned_staging(staging_dir, name="ldt_misaligned")

    with pytest.raises(TerminologyImportAnomaly) as exc_info:
        await import_terminology(postgres_pool, staging_dir)
    assert exc_info.value.misaligned_replaced_by == [("UBERON:0001", "UBERON:0002")]

    # Nothing landed — the name lookup returns no row.
    row = await postgres_pool.fetchrow(
        "SELECT idx FROM qiita.terminology WHERE name = $1", "ldt_misaligned"
    )
    assert row is None


@pytest.mark.db
async def test_import_terminology_tolerate_misaligned_replaced_by_raises(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where the same misalignment is loaded with anomalies
    tolerated: it still raises, because a live term carrying a replacement is
    malformed source data rather than an anomaly a load may absorb."""
    staging_dir = tmp_path / "stage"
    _misaligned_staging(staging_dir, name="ldt_misaligned_tolerated")

    with pytest.raises(TerminologyImportAnomaly) as exc_info:
        await import_terminology(postgres_pool, staging_dir, tolerate_anomalies=True)
    assert exc_info.value.misaligned_replaced_by == [("UBERON:0001", "UBERON:0002")]

    row = await postgres_pool.fetchrow(
        "SELECT idx FROM qiita.terminology WHERE name = $1", "ldt_misaligned_tolerated"
    )
    assert row is None


@pytest.mark.db
async def test_import_terminology_tolerate_silent_drops(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a previously existing term is left out of a
    new release with no explicit deprecation marker, and tolerate mode
    auto-obsoletes it instead of refusing the load.

    The dropped row carries both of its prior names forward, picks up
    obsoletion_kind=silently_dropped, and takes the new release as its
    obsoleted_in_version. The result's terms_silently_dropped counter carries
    the count, and terms_newly_obsoleted also reflects the event because a
    silent drop is a kind of obsoletion.
    """
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_tolerate_silent",
        version="1.0.0",
        terms=[
            parsed_term("UBERON:0001", "mouth"),
            parsed_term("UBERON:0002", "tooth"),
            parsed_term("UBERON:0003", "molar", alternate_label="grinder"),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
            ("UBERON:0003", "UBERON:0003", 0),
        ],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)

    # v2 omits UBERON:0003; tolerate mode auto-obsoletes instead of raising.
    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_tolerate_silent",
        version="2.0.0",
        terms=[
            parsed_term("UBERON:0001", "mouth"),
            parsed_term("UBERON:0002", "tooth"),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
        ],
    )
    v2_result = await import_terminology(postgres_pool, v2_dir, tolerate_anomalies=True)

    expected = TerminologyImportResult(
        terminology_idx=v1_result.terminology_idx,
        terms_inserted=0,
        terms_label_updated=0,
        terms_alternate_label_updated=0,
        terms_newly_obsoleted=1,
        terms_newly_merged=0,
        terms_silently_dropped=1,
        closure_rows=2,
    )
    assert v2_result == expected

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(state, "UBERON:0001", label="mouth"),
        "UBERON:0002": _expected_term_state(state, "UBERON:0002", label="tooth"),
        "UBERON:0003": _expected_term_state(
            state,
            "UBERON:0003",
            label="molar",
            alternate_label="grinder",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SILENTLY_DROPPED,
            obsoleted_in_version="2.0.0",
        ),
    }
    assert state == expected_state


@pytest.mark.db
async def test_import_terminology_tolerate_unresolved_replaced_by(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where an obsolete term names a replacement CURIE
    that isn't present anywhere else in the same batch, and tolerate
    mode records the attempted CURIE in the notes column rather than
    refusing the load.

    Fail mode treats this as a structural anomaly because no in-batch idx can
    populate replaced_by. Tolerate mode lands the row with replaced_by=NULL so
    the structural CHECKs hold, and writes a free-text audit line into notes
    recording the exact CURIE it attempted, so an operator inspecting the row
    later recovers the source's stated intent without re-parsing the staging
    dir.
    """
    staging_dir = tmp_path / "stage"
    _write_staging(
        staging_dir,
        name="ldt_tolerate_unresolved",
        version="1.0.0",
        terms=[
            parsed_term(
                "UBERON:0001",
                "orphan",
                is_obsolete=True,
                replaced_by_term_id="UBERON:9999",
                obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )

    result = await import_terminology(postgres_pool, staging_dir, tolerate_anomalies=True)
    created_terminologies.append(result.terminology_idx)

    state = await _read_term_state(postgres_pool, result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(
            state,
            "UBERON:0001",
            label="orphan",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            obsoleted_in_version="1.0.0",
            notes="v1.0.0: attempted replaced_by=UBERON:9999 unresolved",
        )
    }
    assert state == expected_state


@pytest.mark.db
async def test_import_terminology_tolerate_un_obsoletion_clears_silent_drop(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a term silently dropped in one tolerate-mode
    release reappears as a normal active row in a later release, and
    every obsoletion column on the row clears.

    A silently-dropped row is not a special kind of zombie: the same
    un-obsoletion path the existing UPSERT uses for source_deprecated
    and source_merged terms must clear is_obsolete, obsoletion_kind,
    obsoleted_in_version, and replaced_by when the term comes back as
    active. v1 has two terms; v2 tolerate-mode drops one (so it lands
    at is_obsolete=true, obsoletion_kind=silently_dropped,
    obsoleted_in_version=v2); v3 re-includes the same term as a regular
    active row."""
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_tolerate_un_obsolete",
        version="1.0.0",
        terms=[
            parsed_term("UBERON:0001", "mouth"),
            parsed_term("UBERON:0002", "tooth"),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
        ],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)

    # v2 tolerate: silently drop UBERON:0002.
    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_tolerate_un_obsolete",
        version="2.0.0",
        terms=[parsed_term("UBERON:0001", "mouth")],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )
    await import_terminology(postgres_pool, v2_dir, tolerate_anomalies=True)

    # v3: UBERON:0002 reappears as a normal active term.
    v3_dir = tmp_path / "v3"
    _write_staging(
        v3_dir,
        name="ldt_tolerate_un_obsolete",
        version="3.0.0",
        terms=[
            parsed_term("UBERON:0001", "mouth"),
            parsed_term("UBERON:0002", "tooth"),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
        ],
    )
    await import_terminology(postgres_pool, v3_dir)

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(state, "UBERON:0001", label="mouth"),
        "UBERON:0002": _expected_term_state(state, "UBERON:0002", label="tooth"),
    }
    assert state == expected_state


@pytest.mark.db
async def test_import_terminology_tolerate_notes_accumulate(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where two sequential tolerate-mode loads each produce an
    unresolved-replaced_by audit line for the same row, and both lines remain
    present in notes (the first preserved, the second appended on a new line).

    The notes column is shared between the loader's audit lines and
    operator-added content, so the loader must not wipe it on reload. The
    trade-off is that audit lines from prior loads stay even after the row's
    structural state has moved on; in practice tolerate-mode runs should be rare
    enough that the column does not bloat. v1 carries an obsolete row pointing
    at an absent UBERON:9999; v2 reloads the same row pointing at a different
    absent UBERON:8888.
    """
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_tolerate_notes",
        version="1.0.0",
        terms=[
            parsed_term(
                "UBERON:0001",
                "orphan",
                is_obsolete=True,
                replaced_by_term_id="UBERON:9999",
                obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir, tolerate_anomalies=True)
    created_terminologies.append(v1_result.terminology_idx)

    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_tolerate_notes",
        version="2.0.0",
        terms=[
            parsed_term(
                "UBERON:0001",
                "orphan",
                is_obsolete=True,
                replaced_by_term_id="UBERON:8888",
                obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )
    await import_terminology(postgres_pool, v2_dir, tolerate_anomalies=True)

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_notes = (
        "v1.0.0: attempted replaced_by=UBERON:9999 unresolved\n"
        "v2.0.0: attempted replaced_by=UBERON:8888 unresolved"
    )
    expected_state = {
        "UBERON:0001": _expected_term_state(
            state,
            "UBERON:0001",
            label="orphan",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            obsoleted_in_version="1.0.0",
            notes=expected_notes,
        )
    }
    assert state == expected_state
