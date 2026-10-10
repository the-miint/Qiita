"""Resolve a pool roster to biosamples and studies, by matrix tube or accession.

A row carrying a matrix tube resolves by the tube: tubes are unique across
Qiita (`qiita.biosample.matrix_tube_id` is UNIQUE), so the tube alone names the
biosample, and its study comes from the biosample's active study links. A
project accession on such a row is a cross-check, not the key. A row without a
tube resolves by biosample and project accession, through the same repository
reads the accession lookup routes use.

Every problem across the roster is collected before anything is refused, so
one response names them all. `classify_roster` holds the rules and is pure;
`fetch_roster_facts` gathers what it needs in a fixed number of queries.
"""

from collections import Counter
from dataclasses import dataclass

import asyncpg
from qiita_common.models import (
    RosterProblem,
    RosterResolvedRow,
    RosterResolveRow,
    normalize_matrix_tube_id,
)

from qiita_control_plane.repositories.biosample import (
    fetch_active_study_links,
    fetch_biosample_identity_by_tube,
    fetch_biosample_idxs_by_natural_key,
)
from qiita_control_plane.repositories.study import fetch_study_idxs_by_accession


@dataclass(frozen=True)
class RosterFacts:
    """What the database knows about a roster's identifiers.

    `by_tube` maps a normalized tube to (biosample_idx, biosample_accession);
    `study_links` maps a biosample_idx to its active study_idxs, ascending.
    Only non-retired biosamples appear.
    """

    by_tube: dict[str, tuple[int, str | None]]
    by_biosample_accession: dict[str, int]
    by_study_accession: dict[str, int]
    study_links: dict[int, list[int]]


def _normalized_tube(row: RosterResolveRow) -> str | None:
    """The row's tube in stored form, or None if absent or malformed."""
    if row.matrix_tube_id is None:
        return None
    try:
        return normalize_matrix_tube_id(row.matrix_tube_id)
    except ValueError:
        return None


def _resolve_biosample(
    row: RosterResolveRow, facts: RosterFacts, tube_counts: Counter[str], problems: list[str]
) -> int | None:
    """The row's biosample_idx, or None after appending why to `problems`."""
    if row.matrix_tube_id is None:
        if row.biosample_accession is None:
            problems.append("row carries neither a matrix tube nor a biosample accession")
            return None
        biosample_idx = facts.by_biosample_accession.get(row.biosample_accession)
        if biosample_idx is None:
            problems.append(f"no biosample has accession {row.biosample_accession!r}")
        return biosample_idx

    try:
        tube = normalize_matrix_tube_id(row.matrix_tube_id)
    except ValueError as exc:
        problems.append(str(exc))
        return None
    if tube_counts[tube] > 1:
        # A technical replicate is a second prep_sample of one biosample, and so
        # repeats its tube. A tube-keyed roster does not support replicates yet.
        problems.append(
            f"matrix tube {tube} appears on {tube_counts[tube]} rows of this roster;"
            " technical replicates are not supported in a tube-keyed roster"
        )
        return None
    if tube not in facts.by_tube:
        problems.append(f"no biosample has matrix tube {tube}; register it before submitting")
        return None
    biosample_idx, registered = facts.by_tube[tube]
    if row.biosample_accession is not None and row.biosample_accession != registered:
        held = "no accession" if registered is None else f"accession {registered!r}"
        problems.append(
            f"matrix tube {tube} belongs to a biosample with {held},"
            f" not {row.biosample_accession!r}"
        )
        return None
    return biosample_idx


def _resolve_primary_study(
    row: RosterResolveRow, biosample_idx: int | None, facts: RosterFacts, problems: list[str]
) -> int | None:
    """The row's primary study_idx, or None after appending why to `problems`.

    A named project accession must exist, and on a tube row must name one of the
    biosample's studies. With none named, the biosample's one active study is used.
    """
    biosample = (
        f"the biosample with matrix tube {_normalized_tube(row)}"
        if row.matrix_tube_id is not None
        else f"biosample {row.biosample_accession}"
    )
    links = facts.study_links.get(biosample_idx, []) if biosample_idx is not None else []
    accession = row.primary_project_accession
    if accession is not None:
        study_idx = facts.by_study_accession.get(accession)
        if study_idx is None:
            problems.append(f"no study has accession {accession!r}")
            return None
        if row.matrix_tube_id is not None and biosample_idx is not None and study_idx not in links:
            problems.append(f"{biosample} is not in study {accession}")
            return None
        return study_idx
    if biosample_idx is None:
        return None
    if len(links) == 1:
        return links[0]
    if not links:
        problems.append(f"{biosample} belongs to no active study")
    else:
        problems.append(
            f"{biosample} belongs to {len(links)} studies; name the project's bioproject"
            " accession to choose one"
        )
    return None


def classify_roster(
    rows: list[RosterResolveRow], facts: RosterFacts
) -> tuple[list[RosterResolvedRow], list[RosterProblem]]:
    """Apply the resolution rules to every row.

    Returns (resolved, problems): `resolved` in request order for the rows
    that resolved cleanly, `problems` in request order for the rest. A caller
    refuses the roster when `problems` is non-empty.
    """
    tube_counts = Counter(t for t in map(_normalized_tube, rows) if t is not None)
    resolved: list[RosterResolvedRow] = []
    problems: list[RosterProblem] = []
    for row in rows:
        row_problems: list[str] = []
        biosample_idx = _resolve_biosample(row, facts, tube_counts, row_problems)
        primary_study_idx = _resolve_primary_study(row, biosample_idx, facts, row_problems)
        # Secondary studies are not checked against the biosample's links, on a
        # tube row as on an accession row: they name the other projects sharing a
        # control's plate, which the control need not belong to.
        secondary_study_idxs = []
        for accession in row.secondary_project_accessions:
            study_idx = facts.by_study_accession.get(accession)
            if study_idx is None:
                row_problems.append(f"no study has accession {accession!r}")
            else:
                secondary_study_idxs.append(study_idx)

        if row_problems:
            problems.extend(RosterProblem(item_id=row.item_id, message=m) for m in row_problems)
        else:
            assert biosample_idx is not None and primary_study_idx is not None
            resolved.append(
                RosterResolvedRow(
                    item_id=row.item_id,
                    biosample_idx=biosample_idx,
                    primary_study_idx=primary_study_idx,
                    secondary_study_idxs=secondary_study_idxs,
                )
            )
    return resolved, problems


async def fetch_roster_facts(
    conn: asyncpg.Pool | asyncpg.Connection, rows: list[RosterResolveRow]
) -> RosterFacts:
    """Gather every fact `classify_roster` needs: one query per identifier
    kind, then one for the study links of every biosample found."""
    tubes = sorted({t for t in map(_normalized_tube, rows) if t is not None})
    biosample_accessions = sorted(
        {r.biosample_accession for r in rows if r.matrix_tube_id is None and r.biosample_accession}
    )
    study_accessions = sorted(
        {r.primary_project_accession for r in rows if r.primary_project_accession}
        | {a for r in rows for a in r.secondary_project_accessions}
    )
    by_tube = await fetch_biosample_identity_by_tube(conn, tubes)
    by_biosample_accession = await fetch_biosample_idxs_by_natural_key(
        conn, key="biosample_accession", values=biosample_accessions
    )
    by_study_accession = await fetch_study_idxs_by_accession(conn, values=study_accessions)
    biosample_idxs = sorted(
        {idx for idx, _ in by_tube.values()} | set(by_biosample_accession.values())
    )
    study_links = await fetch_active_study_links(conn, biosample_idxs)
    return RosterFacts(
        by_tube=by_tube,
        by_biosample_accession=by_biosample_accession,
        by_study_accession=by_study_accession,
        study_links=study_links,
    )
