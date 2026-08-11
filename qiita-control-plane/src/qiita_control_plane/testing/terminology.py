"""Test support for the terminology subsystem: standing up terminology rows,
building the records a release is read into, and writing the source forms
those records are read from."""

import csv
import zipfile
from collections.abc import Sequence
from pathlib import Path

import pytest_asyncio
from qiita_common.models import TerminologyTermObsoletionKind

from qiita_control_plane.repositories.terminology import ParsedTerm
from qiita_control_plane.terminology_owl import ExportedClass
from qiita_control_plane.testing.db_seeds import delete_terminology_cascade

# The columns a ROBOT export carries, spelled out independently of the module
# that requests them so a change to the selection has to be made deliberately
# in both places instead of passing tautologically.
ROBOT_EXPORT_HEADER = ("ID", "LABEL", "owl:deprecated", "IAO:0100001", "oboInOwl:hasAlternativeId")

TAXDUMP_ARCHIVE_FILENAME = "new_taxdump.zip"

# The taxdump members and delimiters, spelled out independently of the module
# that reads them for the same reason as the ROBOT header above.
NAMES_DMP_MEMBER = "names.dmp"
MERGED_DMP_MEMBER = "merged.dmp"
DELNODES_DMP_MEMBER = "delnodes.dmp"
_DMP_FIELD_SEPARATOR = "\t|\t"
_DMP_ROW_TERMINATOR = "\t|\n"


def write_robot_export_tsv(path: Path, rows: list[tuple[str, str, str, str, str]]) -> None:
    """Write `rows` as a ROBOT export at `path`. Each row is
    (ID, LABEL, owl:deprecated, IAO:0100001, oboInOwl:hasAlternativeId)."""
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(ROBOT_EXPORT_HEADER)
        writer.writerows(rows)


def write_taxdump_zip(path: Path, members: dict[str, list[tuple[str, ...]]]) -> None:
    """Write `members` as a taxdump archive at `path`, each key a member name
    and its value that member's rows. A member left out of `members` is
    absent from the archive."""
    with zipfile.ZipFile(path, "w") as archive:
        for member, rows in members.items():
            body = "".join(_DMP_FIELD_SEPARATOR.join(row) + _DMP_ROW_TERMINATOR for row in rows)
            archive.writestr(member, body)


def write_taxdump(
    directory: Path,
    *,
    names: Sequence[tuple[str, ...]] = (),
    merged: Sequence[tuple[str, ...]] = (),
    delnodes: Sequence[tuple[str, ...]] = (),
) -> Path:
    """Write a complete taxdump archive into `directory` and return its path.
    Every member is present, holding no rows unless given, so a caller names
    only the rows its case is about."""
    archive_path = directory / TAXDUMP_ARCHIVE_FILENAME
    write_taxdump_zip(
        archive_path,
        {
            NAMES_DMP_MEMBER: list(names),
            MERGED_DMP_MEMBER: list(merged),
            DELNODES_DMP_MEMBER: list(delnodes),
        },
    )
    return archive_path


def exported_class(
    term_id: str,
    label: str,
    *,
    source_deprecated: bool = False,
    asserted_replacement_term_id: str | None = None,
    alternative_term_ids: tuple[str, ...] = (),
) -> ExportedClass:
    """One complete ExportedClass, with the un-annotated defaults filled
    in."""
    return ExportedClass(
        term_id=term_id,
        label=label,
        source_deprecated=source_deprecated,
        asserted_replacement_term_id=asserted_replacement_term_id,
        alternative_term_ids=alternative_term_ids,
    )


def parsed_term(
    term_id: str,
    label: str | None,
    *,
    alternate_label: str | None = None,
    is_obsolete: bool = False,
    replaced_by_term_id: str | None = None,
    obsoletion_kind: TerminologyTermObsoletionKind | None = None,
) -> ParsedTerm:
    """One complete ParsedTerm, with the non-obsolete defaults and the
    alternate_label filled in."""
    return ParsedTerm(
        term_id=term_id,
        label=label,
        alternate_label=alternate_label,
        is_obsolete=is_obsolete,
        replaced_by_term_id=replaced_by_term_id,
        obsoletion_kind=obsoletion_kind,
    )


@pytest_asyncio.fixture
async def created_terminologies(postgres_pool):
    """Yields a list the test appends terminology idxs to; teardown removes
    each one along with its term and closure rows."""
    created: list[int] = []
    yield created
    for terminology_idx in created:
        await delete_terminology_cascade(postgres_pool, terminology_idx)
