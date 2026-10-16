"""Resolve a pool roster to biosamples and studies by their external identifiers.

Every problem across the roster is collected before anything is refused, so
one response names them all. `classify_roster` holds the rules and is pure;
`fetch_roster_facts` gathers what it needs in a fixed number of queries.
"""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field

import asyncpg
from qiita_common.models import (
    BiosampleRef,
    RosterProblem,
    RosterResolvedRow,
    RosterResolveRow,
    StudyRef,
    normalize_matrix_tube_id,
)

from qiita_control_plane.repositories.biosample import (
    BiosampleLookupKey,
    fetch_active_study_links,
    fetch_biosample_idxs_by_natural_key,
)
from qiita_control_plane.repositories.exported_entity import EntityKind
from qiita_control_plane.repositories.study import StudyLookupKey, fetch_study_idxs_by_natural_key


@dataclass(frozen=True)
class _RefSpec:
    """How one kind of entity is resolved from a ref whose fields are lookup keys."""

    noun: EntityKind
    ref_model: type[BiosampleRef] | type[StudyRef]
    fetch: Callable[..., Awaitable[dict[str, int]]]
    normalizers: Mapping[str, Callable[[str], str]] = field(default_factory=dict)
    miss_messages: Mapping[str, str] = field(default_factory=dict)


_BIOSAMPLE_SPEC = _RefSpec(
    noun="biosample",
    ref_model=BiosampleRef,
    fetch=fetch_biosample_idxs_by_natural_key,
    normalizers={"matrix_tube_id": normalize_matrix_tube_id},
    miss_messages={"matrix_tube_id": "no {noun} has {label}; register it before submitting"},
)
_STUDY_SPEC = _RefSpec(noun="study", ref_model=StudyRef, fetch=fetch_study_idxs_by_natural_key)


@dataclass(frozen=True)
class RosterFacts:
    """What the database knows about a roster's identifiers.

    Each `*_by_key` maps a lookup key to {normalized value: idx}; `study_links`
    maps a biosample_idx to its active study_idxs, ascending. Only non-retired
    biosamples appear.
    """

    biosample_by_key: dict[BiosampleLookupKey, dict[str, int]]
    study_by_key: dict[StudyLookupKey, dict[str, int]]
    study_links: dict[int, list[int]]


@dataclass(frozen=True)
class _Resolved:
    idx: int
    description: str


def _normalize(spec: _RefSpec, key: str, raw: str) -> str:
    normalizer = spec.normalizers.get(key)
    return raw if normalizer is None else normalizer(raw)


def _resolve_ref(
    ref: BiosampleRef | StudyRef,
    spec: _RefSpec,
    by_key: Mapping[str, dict[str, int]],
    problems: list[str],
) -> _Resolved | None:
    """The entity every identifier on `ref` names, or None after appending why
    to `problems`."""
    problem_count = len(problems)
    hits: dict[str, int] = {}
    for key in spec.ref_model.model_fields:
        raw = getattr(ref, key)
        if raw is None:
            continue
        try:
            value = _normalize(spec, key, raw)
        except ValueError as exc:
            problems.append(str(exc))
            continue
        label = f"{key} {value!r}"
        idx = by_key.get(key, {}).get(value)
        if idx is not None:
            hits[label] = idx
        elif key in spec.miss_messages:
            problems.append(spec.miss_messages[key].format(noun=spec.noun, label=label))
        else:
            problems.append(f"no {spec.noun} has {label}")
    if len(set(hits.values())) > 1:
        problems.append(f"the identifiers name more than one {spec.noun}: {', '.join(hits)}")
    if len(problems) > problem_count:
        return None
    (idx,) = set(hits.values())
    return _Resolved(idx=idx, description=f"the {spec.noun} with {' and '.join(hits)}")


def classify_roster(
    rows: list[RosterResolveRow], facts: RosterFacts
) -> tuple[list[RosterResolvedRow], list[RosterProblem]]:
    """Apply the resolution rules to every row.

    Returns (resolved, problems): `resolved` in request order for the rows
    that resolved cleanly, `problems` in request order for the rest. A caller
    refuses the roster when `problems` is non-empty.
    """
    resolved: list[RosterResolvedRow] = []
    problems: list[RosterProblem] = []
    for row in rows:
        row_problems: list[str] = []
        biosample = _resolve_ref(
            row.biosample, _BIOSAMPLE_SPEC, facts.biosample_by_key, row_problems
        )
        studies = [
            _resolve_ref(project, _STUDY_SPEC, facts.study_by_key, row_problems)
            for project in (row.primary_project, *row.secondary_projects)
        ]
        if biosample is not None:
            links = facts.study_links.get(biosample.idx, [])
            for study in studies:
                if study is not None and study.idx not in links:
                    row_problems.append(f"{biosample.description} is not in {study.description}")

        if row_problems:
            problems.extend(RosterProblem(item_id=row.item_id, message=m) for m in row_problems)
            continue
        assert biosample is not None and None not in studies
        primary, *secondaries = studies
        resolved.append(
            RosterResolvedRow(
                item_id=row.item_id,
                biosample_idx=biosample.idx,
                primary_study_idx=primary.idx,
                secondary_study_idxs=[s.idx for s in secondaries],
            )
        )
    return resolved, problems


async def _fetch_by_key(
    conn: asyncpg.Pool | asyncpg.Connection,
    spec: _RefSpec,
    refs: list[BiosampleRef] | list[StudyRef],
) -> dict[str, dict[str, int]]:
    """{lookup key: {normalized value: idx}} over every identifier on `refs`."""
    by_key: dict[str, dict[str, int]] = {}
    for key in spec.ref_model.model_fields:
        values: set[str] = set()
        for ref in refs:
            raw = getattr(ref, key)
            if raw is None:
                continue
            try:
                values.add(_normalize(spec, key, raw))
            except ValueError:
                continue  # classify_roster reports it
        by_key[key] = await spec.fetch(conn, key=key, values=sorted(values))
    return by_key


async def fetch_roster_facts(
    conn: asyncpg.Pool | asyncpg.Connection, rows: list[RosterResolveRow]
) -> RosterFacts:
    """Gather every fact `classify_roster` needs: one query per lookup key
    present, then one for the study links of every biosample found."""
    biosample_by_key = await _fetch_by_key(conn, _BIOSAMPLE_SPEC, [r.biosample for r in rows])
    study_refs = [p for r in rows for p in (r.primary_project, *r.secondary_projects)]
    study_by_key = await _fetch_by_key(conn, _STUDY_SPEC, study_refs)
    biosample_idxs = sorted({idx for hits in biosample_by_key.values() for idx in hits.values()})
    study_links = await fetch_active_study_links(conn, biosample_idxs)
    facts = RosterFacts(
        biosample_by_key=biosample_by_key,
        study_by_key=study_by_key,
        study_links=study_links,
    )
    return facts
