"""The NCBI taxdump's own dialect: the members of a taxdump archive a
release is read from, and the term rows that reading yields.

Everything specific to the taxdump is confined here — which members it
carries, the delimiters between its rows and fields, and which of its name
classes feed which name column. The rows handed back carry no trace of it.
"""

from __future__ import annotations

import io
import logging
import zipfile
from collections.abc import Iterator
from pathlib import Path

from qiita_common.models import TerminologyTermObsoletionKind

from .repositories.terminology import ParsedTerm, format_offenders

_log = logging.getLogger(__name__)

# The archive members a release is read from.
NAMES_DMP_MEMBER = "names.dmp"
MERGED_DMP_MEMBER = "merged.dmp"
DELNODES_DMP_MEMBER = "delnodes.dmp"

# How a member delimits its rows and the fields within them: a row ends with
# the leading half of the field separator, which is stripped before the row
# is split.
_DMP_FIELD_SEPARATOR = "\t|\t"
_DMP_ROW_SUFFIX = "\t|"
# Declared rather than left to the platform default, which varies by host.
_DMP_ENCODING = "utf-8"

# The documented column order of each member. A row is zipped against the
# tuple for its member, so every field is read by name and a member whose
# layout changed fails loudly instead of silently shifting fields.
_COLUMN_TAX_ID = "tax_id"
_COLUMN_NAME_TXT = "name_txt"
_COLUMN_NAME_CLASS = "name_class"
_COLUMN_OLD_TAX_ID = "old_tax_id"
_COLUMN_NEW_TAX_ID = "new_tax_id"
_NAMES_COLUMNS = (_COLUMN_TAX_ID, _COLUMN_NAME_TXT, "unique_name", _COLUMN_NAME_CLASS)
_MERGED_COLUMNS = (_COLUMN_OLD_TAX_ID, _COLUMN_NEW_TAX_ID)
_DELNODES_COLUMNS = (_COLUMN_TAX_ID,)

# The two name classes a term's two name columns are taken from. The
# taxdump's remaining classes are citations, set relations, or multi-valued
# where the column holding them is not, so they are read past.
_NAME_CLASS_SCIENTIFIC = "scientific name"
_NAME_CLASS_GENBANK_COMMON = "genbank common name"
_CONSUMED_NAME_CLASSES = (_NAME_CLASS_SCIENTIFIC, _NAME_CLASS_GENBANK_COMMON)


def build_terms_from_taxdump(taxdump_zip: Path) -> list[ParsedTerm]:
    """Read the term rows of a release from the taxdump archive at
    `taxdump_zip`.

    Raises FileNotFoundError when the archive or a member it must carry is
    absent, zipfile.BadZipFile when the file is not an archive, and
    ValueError when a member's layout or content contradicts what the
    taxdump documents.
    """
    if not taxdump_zip.exists():
        raise FileNotFoundError(f"No taxdump archive at {taxdump_zip}")

    with zipfile.ZipFile(taxdump_zip) as archive:
        taxon_names = _read_taxon_names(archive)
        merges = _read_merges(archive)
        deleted_tax_ids = _read_deleted_tax_ids(archive)

    terms = _assemble_terms(taxon_names, merges, deleted_tax_ids)
    return terms


def _read_dmp_rows(
    archive: zipfile.ZipFile,
    member: str,
    columns: tuple[str, ...],
) -> Iterator[dict[str, str]]:
    """Yield one `columns`-keyed dict per row of `member`.

    Raises FileNotFoundError when the archive carries no such member, and
    ValueError naming the member and line number when a row does not end
    with the row terminator or its field count does not match `columns`.
    """
    if member not in archive.namelist():
        raise FileNotFoundError(f"taxdump archive {archive.filename} carries no {member}")

    with archive.open(member) as raw:
        # newline="\n" leaves the bytes untranslated, so a stray carriage
        # return survives into the row and trips the terminator check below
        # instead of being silently absorbed.
        text = io.TextIOWrapper(raw, encoding=_DMP_ENCODING, newline="\n")
        for line_number, line in enumerate(text, start=1):
            row = line.rstrip("\n")
            if not row.endswith(_DMP_ROW_SUFFIX):
                raise ValueError(
                    f"{member} line {line_number} does not end with the row"
                    f" terminator {_DMP_ROW_SUFFIX!r}"
                )

            # Zipping strictly asserts the field count as it names the fields,
            # so a member whose layout changed cannot read as a shifted row.
            fields = row.removesuffix(_DMP_ROW_SUFFIX).split(_DMP_FIELD_SEPARATOR)
            try:
                named_fields = dict(zip(columns, fields, strict=True))
            except ValueError as exc:
                raise ValueError(
                    f"{member} line {line_number} carries {len(fields)} field(s);"
                    f" expected {len(columns)} ({list(columns)})"
                ) from exc
            yield named_fields


def _read_taxon_names(archive: zipfile.ZipFile) -> dict[str, dict[str, str]]:
    """Map each taxon id to the names it carries in the classes a term's two
    name columns are taken from, keyed by class.

    A taxon carrying a second name of a class it already has keeps the first
    and the extra is warned about, since each column holds one name.
    """
    names: dict[str, dict[str, str]] = {}
    for row in _read_dmp_rows(archive, NAMES_DMP_MEMBER, _NAMES_COLUMNS):
        name_class = row[_COLUMN_NAME_CLASS]
        if name_class not in _CONSUMED_NAME_CLASSES:
            continue

        # The first name of a class stands, so a taxon named twice in one
        # class is reported rather than having its stored name overwritten.
        tax_id = row[_COLUMN_TAX_ID]
        for_taxon = names.get(tax_id, {})
        kept = for_taxon.get(name_class)
        if kept is not None:
            _log.warning(
                "tax_id %s carries more than one %r (%r and %r); keeping %r",
                tax_id,
                name_class,
                kept,
                row[_COLUMN_NAME_TXT],
                kept,
            )
            continue
        for_taxon[name_class] = row[_COLUMN_NAME_TXT]
        names[tax_id] = for_taxon
    return names


def _read_merges(archive: zipfile.ZipFile) -> dict[str, str]:
    """Map each merged-away taxon id to the id of the taxon it merged into."""
    merges: dict[str, str] = {}
    for row in _read_dmp_rows(archive, MERGED_DMP_MEMBER, _MERGED_COLUMNS):
        merges[row[_COLUMN_OLD_TAX_ID]] = row[_COLUMN_NEW_TAX_ID]
    return merges


def _read_deleted_tax_ids(archive: zipfile.ZipFile) -> list[str]:
    """Return the taxon ids the archive records as deleted, in member order."""
    rows = _read_dmp_rows(archive, DELNODES_DMP_MEMBER, _DELNODES_COLUMNS)
    deleted_tax_ids = [row[_COLUMN_TAX_ID] for row in rows]
    return deleted_tax_ids


def _check_taxon_records(
    taxon_names: dict[str, dict[str, str]],
    merges: dict[str, str],
    deleted_tax_ids: list[str],
) -> None:
    """Enforce that every taxon the archive names carries the name a label is
    taken from, and that no taxon id is recorded in more than one of the
    three ways.

    Raises ValueError naming the offending ids: a taxon named in no class a
    label can come from leaves that column nothing to hold, and an id
    recorded two ways is a taxdump contradicting itself.
    """
    unnamed_tax_ids = sorted(
        tax_id for tax_id, by_class in taxon_names.items() if _NAME_CLASS_SCIENTIFIC not in by_class
    )
    if unnamed_tax_ids:
        raise ValueError(
            f"{NAMES_DMP_MEMBER} carries no {_NAME_CLASS_SCIENTIFIC!r} for"
            f" tax_id(s) {format_offenders(unnamed_tax_ids)}"
        )

    # Every pair of id sets is checked before anything raises, so one read of
    # the error names every pair that disagrees rather than only the first.
    live_ids = set(taxon_names)
    merged_ids = set(merges)
    deleted_ids = set(deleted_tax_ids)
    member_id_sets = (
        (NAMES_DMP_MEMBER, live_ids),
        (MERGED_DMP_MEMBER, merged_ids),
        (DELNODES_DMP_MEMBER, deleted_ids),
    )
    overlap_reports: list[str] = []
    for position, (member, ids) in enumerate(member_id_sets):
        for other_member, other_ids in member_id_sets[position + 1 :]:
            overlap = sorted(ids & other_ids)
            if overlap:
                overlap_reports.append(f"{member} and {other_member}: {format_offenders(overlap)}")
    if overlap_reports:
        raise ValueError(
            "tax_id(s) recorded in more than one member; a taxon is live, merged away,"
            f" or deleted, never more than one of them — {'; '.join(overlap_reports)}"
        )


def _assemble_terms(
    taxon_names: dict[str, dict[str, str]],
    merges: dict[str, str],
    deleted_tax_ids: list[str],
) -> list[ParsedTerm]:
    """Turn what the archive records into the term rows of a release: a live
    taxon named by its two name classes, a merged-away id replaced by the
    taxon it merged into, and a deleted id retired with no replacement.

    A merged-away or deleted id is left unnamed, because the taxdump names
    neither and what to store in its place depends on whether the term is
    already known, which is not visible from here.
    """
    _check_taxon_records(taxon_names, merges, deleted_tax_ids)

    terms = [
        ParsedTerm(
            term_id=tax_id,
            label=by_class[_NAME_CLASS_SCIENTIFIC],
            alternate_label=by_class.get(_NAME_CLASS_GENBANK_COMMON),
            is_obsolete=False,
            replaced_by_term_id=None,
            obsoletion_kind=None,
        )
        for tax_id, by_class in taxon_names.items()
    ]

    # A merge and a deletion differ only in whether a replacement is named
    # and which kind records the retirement, so both are built from one
    # (retired id, replacement) sequence apiece.
    retirements = (
        (list(merges.items()), TerminologyTermObsoletionKind.SOURCE_MERGED),
        (
            [(tax_id, None) for tax_id in deleted_tax_ids],
            TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
        ),
    )
    for retired_pairs, obsoletion_kind in retirements:
        terms += [
            ParsedTerm(
                term_id=retired_tax_id,
                label=None,
                alternate_label=None,
                is_obsolete=True,
                replaced_by_term_id=replacement_tax_id,
                obsoletion_kind=obsoletion_kind,
            )
            for retired_tax_id, replacement_tax_id in retired_pairs
        ]
    return terms
