"""Pytest fixtures for tests that create terminology rows, builders for the
term records an OWL release is read into, and a writer for the ROBOT export
those records are read from."""

import csv
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


def write_robot_export_tsv(path: Path, rows: list[tuple[str, str, str, str, str]]) -> None:
    """Write `rows` as a ROBOT export at `path`. Each row is
    (ID, LABEL, owl:deprecated, IAO:0100001, oboInOwl:hasAlternativeId)."""
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(ROBOT_EXPORT_HEADER)
        writer.writerows(rows)


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
    label: str,
    *,
    is_obsolete: bool = False,
    replaced_by_term_id: str | None = None,
    obsoletion_kind: TerminologyTermObsoletionKind | None = None,
) -> ParsedTerm:
    """One complete ParsedTerm, with the non-obsolete defaults filled in."""
    return ParsedTerm(
        term_id=term_id,
        label=label,
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
