"""Unit tests for qiita_control_plane.terminology — manifest
parser/verifier, and the staging-dir import workflow."""

import csv
import hashlib
import json
from datetime import datetime

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
    that stores it allows: the manifest is refused up front, rather than passing
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
    does not, so a mismatch in the second table is not skipped."""
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
# write_terms_tsv / write_closure_tsv_stub
# =============================================================================


def test_write_terms_tsv(tmp_path):
    """Tests the case where terms with and without obsoletion fields and
    with and without a second name are written: the file is headed by the
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
    ]
    path = tmp_path / TERMS_TSV_FILENAME

    write_terms_tsv(path, terms)

    assert path.read_text().splitlines()[0] == "\t".join(TERMS_TSV_COLUMNS)
    assert _parse_terms_tsv(path) == terms


def test__parse_terms_tsv_blank_label(tmp_path):
    """Tests the case where the label cell is blank: it parses to None rather
    than an empty string, so a source that names no term is distinguishable
    from one that names it the empty string, and the load can decide what to
    store."""
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
    """Tests the case where a term carries no label: it is written as an empty
    cell and reads back through the parser as None."""
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
    whitespace: both arrive as None, because an absent second name is
    spelled NULL in the database rather than as an empty string."""
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
    whitespace: each is stripped, because the index over them is a plain btree
    that would hold a padded variant as a value distinct from its unpadded
    twin."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "  NCBI:9606  \t  Homo sapiens  \t  human  \tfalse\t\t\n"
    )

    result = _parse_terms_tsv(path)

    expected = [parsed_term("NCBI:9606", "Homo sapiens", alternate_label="human")]
    assert result == expected


@pytest.mark.parametrize(
    ("row_text", "expected_column"),
    [
        ("UBERON:0001\n", "label"),
        ("UBERON:0001\tmouth\n", "alternate_label"),
        ("UBERON:0001\tmouth\t\n", "is_obsolete"),
    ],
    ids=["stops_before_label", "stops_before_alternate_label", "stops_before_is_obsolete"],
)
def test__parse_terms_tsv_short_row(tmp_path, row_text, expected_column):
    """Tests the case where a terms row stops before a cell that is always read:
    the parse is refused naming the absent column, rather than failing on
    whatever the missing value was asked to do."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        f"{row_text}"
    )

    with pytest.raises(ValueError, match=f"no {expected_column}"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_empty_term_id(tmp_path):
    """Tests the case where a term id cell holds nothing but whitespace: the
    parse is refused, since the database keys the row by that value and every
    row referencing the term spells it unpadded."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "   \tmouth\t\tfalse\t\t\n"
    )

    with pytest.raises(ValueError, match="empty term_id"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_missing_column(tmp_path):
    """Tests the case where the terms table was written against an earlier
    column set: the parse is refused up front naming the absent column,
    rather than silently reading every row as having no second name."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0001\tmouth\tfalse\t\t\n"
    )

    with pytest.raises(ValueError, match="alternate_label"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_duplicate_term_id(tmp_path):
    """Tests the case where one term id occupies two rows of the terms table:
    the parse is refused naming the id, because the release contradicts itself
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
    """Tests the case where several term ids are duplicated: every offending id
    is named in one sorted error, so a whole table is corrected in one pass
    rather than one id per run."""
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
    the total is stated and the tail is left unnamed, so a wholly corrupt table
    yields a readable error rather than one line per offending id."""
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
    the closure table: the parse is refused naming the pair, because the
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
    the parse is refused just as for an exact repeat, because the database
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
    parse is refused naming the row, because the database requires an obsolete
    term to record why it is obsolete."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0003\tobsolete molar\t\ttrue\t\t\n"
    )

    with pytest.raises(ValueError, match="UBERON:0003"):
        _parse_terms_tsv(path)


def test__parse_terms_tsv_kind_on_live_row(tmp_path):
    """Tests the case where a live row carries an obsoletion kind: the parse is
    refused naming the row, because the database allows a kind only on a term
    that is obsolete."""
    path = tmp_path / TERMS_TSV_FILENAME
    path.write_text(
        "term_id\tlabel\talternate_label\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n"
        "UBERON:0001\tmouth\t\tfalse\t\tsource_merged\n"
    )

    with pytest.raises(ValueError, match="UBERON:0001"):
        _parse_terms_tsv(path)


def test__parse_closure_tsv_missing_column(tmp_path):
    """Tests the case where the closure table was written against a different
    column set: the parse is refused up front naming the absent column, rather
    than failing per row on a key it cannot find."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text("ancestor\tdescendant\tdistance\nUBERON:0001\tUBERON:0002\t1\n")

    with pytest.raises(ValueError, match="ancestor_term_id"):
        _parse_closure_tsv(path)


def test__parse_closure_tsv_strips_endpoints(tmp_path):
    """Tests the case where both endpoints arrive padded with whitespace: each
    is stripped, so the pair matches the term rows it relates rather than being
    dropped by the rebuild for naming a term id nothing spells that way."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\n  UBERON:0001  \t  UBERON:0002  \t1\n"
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
    parse is refused naming that column, since a closure row can only relate
    terms the release defines."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        f"ancestor_term_id\tdescendant_term_id\tdistance\n{ancestor_cell}\t{descendant_cell}\t1\n"
    )

    with pytest.raises(ValueError, match=f"empty {expected_column}"):
        _parse_closure_tsv(path)


def test__parse_closure_tsv_short_row(tmp_path):
    """Tests the case where a closure row stops before its distance: the parse
    is refused naming the pair, rather than raising on an absent cell."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text("ancestor_term_id\tdescendant_term_id\tdistance\nUBERON:0001\tUBERON:0002\n")

    with pytest.raises(ValueError, match="UBERON:0002"):
        _parse_closure_tsv(path)


def test__parse_closure_tsv_non_integer_distance(tmp_path):
    """Tests the case where a distance cell is not a number: the parse is
    refused naming the pair it belongs to."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\nUBERON:0001\tUBERON:0002\tone\n"
    )

    with pytest.raises(ValueError, match="UBERON:0002"):
        _parse_closure_tsv(path)


def test__parse_closure_tsv_negative_distance(tmp_path):
    """Tests the case where a distance is negative: the parse is refused naming
    the pair, because the database holds distance non-negative."""
    path = tmp_path / CLOSURE_TSV_FILENAME
    path.write_text(
        "ancestor_term_id\tdescendant_term_id\tdistance\nUBERON:0001\tUBERON:0002\t-1\n"
    )

    with pytest.raises(ValueError, match="UBERON:0002"):
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

    Both headers are spelled out here rather than taken from the constants
    the writers use, so a rename of a declared column fails these tests
    instead of passing tautologically.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)

    with (staging_dir / terms_filename).open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(
            [
                "term_id",
                "label",
                "alternate_label",
                "is_obsolete",
                "replaced_by_term_id",
                "obsoletion_kind",
            ]
        )
        for term in terms:
            writer.writerow(
                [
                    term.term_id,
                    term.label,
                    term.alternate_label or "",
                    "true" if term.is_obsolete else "false",
                    term.replaced_by_term_id or "",
                    str(term.obsoletion_kind) if term.obsoletion_kind is not None else "",
                ]
            )

    with (staging_dir / closure_filename).open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(["ancestor_term_id", "descendant_term_id", "distance"])
        for ancestor, descendant, distance in closure:
            writer.writerow([ancestor, descendant, str(distance)])

    # Digest the tables after writing them, so the manifest describes what is
    # actually on disk rather than what was intended.
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

    `idx` is auto-generated, so it is copied from `idx_source` — the actual
    state whose idxs the comparison should match. Passing an earlier load's
    state there is what folds idx preservation into the equality.
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
    """Tests the case where a brand-new staging directory is loaded: every term
    is inserted, every closure row is written, and the terminology row ends in
    ACTIVE."""
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
    terminology_idx and the per-term idxs are preserved, and a relabeled term
    counts as terms_label_updated."""
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

    # Second load relabels one term; terminology_idx and per-term idxs must persist.
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

    # idxs are sourced from v1_state, so this equality also proves they persisted.
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
    and supplies none for another that had one: the changed value is
    overwritten and the omitted one is cleared to NULL.

    The release is authoritative for alternate_label exactly as it is for
    label, so a source that stops offering a second name is able to say so.
    A term the release leaves untouched keeps the value it already had.
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
    obsolete in v3: obsoleted_in_version is stamped at v2 and never advances on
    subsequent reloads."""
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
    v2: is_obsolete, obsoletion_kind, obsoleted_in_version, and replaced_by are
    all cleared."""
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
    version being loaded. The v2 state is asserted before v3 runs, so a v2
    that failed to stamp cannot let the v4 expectation pass vacuously.
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

    # terms_newly_merged counts the kind flip on UBERON:0001 even though
    # the row was already obsolete from v1, so terms_newly_obsoleted stays 0.
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
    closure rows of a previously loaded sentinel terminology are left
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
    with no explicit deprecation marker: TerminologyImportAnomaly is raised
    listing the silently-dropped term, and the v1 row and term set survive
    intact because the v2 transaction rolls back."""
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

    # Transaction must have rolled back: status, version, loaded_at
    # unchanged from v1, and UBERON:0003 still present and active.
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
    attempted_replaced_by) pair, and nothing is written to the database."""
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

    # Nothing was inserted — name lookup returns no row.
    row = await postgres_pool.fetchrow(
        "SELECT idx FROM qiita.terminology WHERE name = $1", "ldt_unresolved"
    )
    assert row is None


@pytest.mark.db
async def test_import_terminology_unresolved_closure_endpoint_raises(postgres_pool, tmp_path):
    """Tests the case where a closure row names a term the release does not
    define: the load is refused naming the pair and writes nothing, because a
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
    canonical ones: the tables are read from the declared paths, which are the
    same paths their digests were checked against."""
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
    pair and nothing is written, because only an obsolete term may point at
    a successor."""
    staging_dir = tmp_path / "stage"
    _misaligned_staging(staging_dir, name="ldt_misaligned")

    with pytest.raises(TerminologyImportAnomaly) as exc_info:
        await import_terminology(postgres_pool, staging_dir)
    assert exc_info.value.misaligned_replaced_by == [("UBERON:0001", "UBERON:0002")]

    # Nothing was inserted — name lookup returns no row.
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
    obsoletion_kind=silently_dropped, and is stamped with the new
    release as its obsoleted_in_version. The result's
    terms_silently_dropped counter surfaces the count for the caller to
    log; terms_newly_obsoleted also reflects the event because a silent
    drop is a kind of obsoletion."""
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

    Fail mode treats this as a structural anomaly because there is no
    in-batch idx the loader can use to populate replaced_by. Tolerate
    mode lands the row with replaced_by=NULL on the DB side so the
    structural CHECKs hold, and writes a free-text audit line into
    notes recording the exact CURIE that was attempted so an operator
    later inspecting the row can recover the source's stated intent
    without re-parsing the staging dir."""
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
    """Tests the case where two sequential tolerate-mode loads each
    produce an unresolved-replaced_by audit line for the same row, and
    both lines remain present in notes (the first preserved, the second
    appended on a new line).

    The notes column is shared between the loader's audit lines and
    operator-added content, so the loader is not allowed to wipe it on
    reload. The trade-off is that audit lines from prior loads stay
    even after the row's structural state has moved on; in practice
    tolerate-mode runs should be rare enough that the column does not
    bloat. v1 carries an obsolete row pointing at an absent UBERON:9999;
    v2 reloads the same row pointing at a different absent UBERON:8888."""
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
