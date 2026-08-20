"""The NCBI taxdump's own dialect: what the members of a taxdump archive
record about a taxon, and the term rows that reading them yields.

This module confines everything specific to the taxdump — which members a
release reads, which of its name classes feed which name column, and what its
records say about a taxon's fate. The rows handed back carry no trace of it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb
from qiita_common.duckdb_miint import MIINT_EXTENSION_DIRECTORY_VAR
from qiita_common.models import TerminologyTermObsoletionKind

from .miint import connect_with_miint
from .repositories.terminology import ParsedTerm, format_offenders

_log = logging.getLogger(__name__)

# The archive members a release is read from, named in errors about them.
_NAMES_DMP_MEMBER = "names.dmp"
_MERGED_DMP_MEMBER = "merged.dmp"
_DELNODES_DMP_MEMBER = "delnodes.dmp"

# The two name classes feeding a term's two name columns. The taxdump's
# remaining classes are citations, set relations, or multi-valued where the
# column is not, so the read skips them.
_NAME_CLASS_SCIENTIFIC = "scientific name"
_NAME_CLASS_GENBANK_COMMON = "genbank common name"

# One statement per member, ordered by taxon id so term rows land in a fixed
# order: the manifest digests that file, and an unordered read would change it
# per run. Each name class is counted so a taxon named twice can be reported.
# Names come from the per-member reader rather than from the joined
# read_ncbi_taxdump, which hands back the scientific name alone and so leaves
# the second name column nothing to draw on.
_TAXON_NAMES_SQL = f"""
    SELECT
        taxid,
        min(CASE WHEN name_class = '{_NAME_CLASS_SCIENTIFIC}' THEN name END),
        count(CASE WHEN name_class = '{_NAME_CLASS_SCIENTIFIC}' THEN 1 END),
        min(CASE WHEN name_class = '{_NAME_CLASS_GENBANK_COMMON}' THEN name END),
        count(CASE WHEN name_class = '{_NAME_CLASS_GENBANK_COMMON}' THEN 1 END)
    FROM read_ncbi_taxdump_names(?)
    GROUP BY taxid
    ORDER BY taxid
"""
_MERGES_SQL = "SELECT old_taxid, new_taxid FROM read_ncbi_taxdump_merged(?) ORDER BY old_taxid"
_DELETED_SQL = "SELECT taxid FROM read_ncbi_taxdump_deleted(?) ORDER BY taxid"

# A full release reads long enough for DuckDB to draw its progress bar, which
# it writes to stdout; stdout must stay free of it.
_SILENCE_PROGRESS_SQL = "SET enable_progress_bar = false"


@dataclass(frozen=True)
class _TaxonNames:
    """The names one taxon carries in the two classes feeding a term's name
    columns, each None when the archive names it in no such class."""

    scientific: str | None
    genbank_common: str | None


def build_terms_from_taxdump(taxdump_tar_gz: Path) -> list[ParsedTerm]:
    """Read the term rows of a release from the taxdump archive at
    `taxdump_tar_gz`, a gzipped tar holding its members at the top level.

    Raises FileNotFoundError when the archive is absent, duckdb.Error when
    DuckDB cannot read it, when it carries no member a release needs, or when a
    row's field count contradicts what the taxdump documents, and ValueError
    when the path is not a file or the archive contradicts itself.
    """
    if not taxdump_tar_gz.exists():
        raise FileNotFoundError(f"No taxdump archive at {taxdump_tar_gz}")

    # A directory of extracted members also reads, which is not the shape a
    # release takes; refusing it keeps the accepted form the documented one
    # rather than one that happens to work.
    if not taxdump_tar_gz.is_file():
        raise ValueError(f"Not a taxdump archive file: {taxdump_tar_gz}")

    # Every member is read through one connection, and the whole archive is
    # read before anything is assembled, so a member that cannot be read
    # refuses the release rather than yielding a partial one.
    #
    # The members are read relationally and handed back as objects, so a release
    # is bounded by the memory that its rows occupy rather than by what DuckDB
    # can stream. Copying the rows straight into the release table would lift
    # that bound, at the cost of a release-writing path that no longer serves a
    # source whose rows are decided per-class in Python.
    source = str(taxdump_tar_gz)
    conn = connect_with_miint()
    try:
        conn.execute(_SILENCE_PROGRESS_SQL)
        taxon_names = _read_taxon_names(conn, source)
        merge_cursor = conn.execute(_MERGES_SQL, [source])
        merge_rows = merge_cursor.fetchall()
        merges = {str(retired): str(survivor) for retired, survivor in merge_rows}
        deleted_cursor = conn.execute(_DELETED_SQL, [source])
        deleted_rows = deleted_cursor.fetchall()
        deleted_tax_ids = [str(row[0]) for row in deleted_rows]
    except duckdb.CatalogException as exc:
        # A cached extension predating these readers reports only the missing
        # name, pointing at this code rather than at the cache holding it.
        raise duckdb.CatalogException(
            f"{exc} A cached miint build older than this reader would report"
            f" exactly this; check {MIINT_EXTENSION_DIRECTORY_VAR} or clear the"
            " extension cache and retry."
        ) from exc
    finally:
        conn.close()

    terms = _assemble_terms(taxon_names, merges, deleted_tax_ids)
    return terms


def _read_taxon_names(
    conn: duckdb.DuckDBPyConnection,
    source: str,
) -> dict[str, _TaxonNames]:
    """Map each taxon id the archive names to the names it carries in the
    classes feeding a term's two name columns.

    A taxon carrying two names of one class keeps one and warns about the
    surplus, since each column holds a single name.
    """
    cursor = conn.execute(_TAXON_NAMES_SQL, [source])
    named_rows = cursor.fetchall()

    names: dict[str, _TaxonNames] = {}
    for taxid, scientific, scientific_count, genbank_common, genbank_common_count in named_rows:
        # One name of the class stands, so warn about a taxon named twice
        # rather than absorbing the surplus silently.
        tax_id = str(taxid)
        by_class = (
            (_NAME_CLASS_SCIENTIFIC, scientific, scientific_count),
            (_NAME_CLASS_GENBANK_COMMON, genbank_common, genbank_common_count),
        )
        for name_class, kept, carried_count in by_class:
            if carried_count > 1:
                _log.warning(
                    "tax_id %s carries %d %r names; keeping %r",
                    tax_id,
                    carried_count,
                    name_class,
                    kept,
                )
        names[tax_id] = _TaxonNames(scientific=scientific, genbank_common=genbank_common)
    return names


def _check_taxon_records(
    taxon_names: dict[str, _TaxonNames],
    merges: dict[str, str],
    deleted_tax_ids: list[str],
) -> None:
    """Enforce that every taxon the archive names carries the name a label
    comes from, and that no taxon id appears in more than one of the three
    sources.

    Raises ValueError naming the offending ids: a taxon named in no class a
    label can come from leaves that column nothing to hold, and an id recorded
    two ways is a taxdump contradicting itself.
    """
    unnamed_tax_ids = sorted(
        tax_id for tax_id, names in taxon_names.items() if names.scientific is None
    )
    if unnamed_tax_ids:
        raise ValueError(
            f"{_NAMES_DMP_MEMBER} carries no {_NAME_CLASS_SCIENTIFIC!r} for"
            f" tax_id(s) {format_offenders(unnamed_tax_ids)}"
        )

    # Check every pair of id sets before raising, so one error names every
    # disagreeing pair rather than only the first.
    live_ids = set(taxon_names)
    merged_ids = set(merges)
    deleted_ids = set(deleted_tax_ids)
    member_id_sets = (
        (_NAMES_DMP_MEMBER, live_ids),
        (_MERGED_DMP_MEMBER, merged_ids),
        (_DELNODES_DMP_MEMBER, deleted_ids),
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
    taxon_names: dict[str, _TaxonNames],
    merges: dict[str, str],
    deleted_tax_ids: list[str],
) -> list[ParsedTerm]:
    """Turn what the archive records into the term rows of a release: a live
    taxon named by its two name classes, a merged-away id replaced by the
    taxon it merged into, and a deleted id retired with no replacement.

    A merged-away or deleted id stays unnamed: the taxdump names neither, and
    what to store in its place depends on whether the term is already known,
    which this module cannot see.
    """
    _check_taxon_records(taxon_names, merges, deleted_tax_ids)

    terms = [
        ParsedTerm(
            term_id=tax_id,
            label=names.scientific,
            alternate_label=names.genbank_common,
            is_obsolete=False,
            replaced_by_term_id=None,
            obsoletion_kind=None,
        )
        for tax_id, names in taxon_names.items()
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
            ParsedTerm.retired(
                retired_tax_id,
                replaced_by_term_id=replacement_tax_id,
                obsoletion_kind=obsoletion_kind,
            )
            for retired_tax_id, replacement_tax_id in retired_pairs
        ]
    return terms
