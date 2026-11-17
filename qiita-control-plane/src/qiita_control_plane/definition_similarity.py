"""Similarity between definitions of one kind (masks, alignments, processings): the
change between two definitions, the groups that output-equivalence verdicts form, and
every refusal. The rules are stated here and nowhere else.

**Verdicts.** An output equivalence is `pending` or decided. A decided verdict either
joins (`JOINING_VERDICTS`: `same`, `similar`) or does not (`NON_JOINING_VERDICTS`:
`different`, `not_benchmarked`). Every rule below reads those two sets, never a single
verdict: the two joining verdicts behave identically everywhere, as do the two
non-joining ones. "Joining" and "non-joining" below mean those sets.

**Change.** `param_diff` compares two definitions' stored params. Nested objects are
walked into paths of keys, written as JSON Pointers (RFC 6901:
`/resolved_qc/min_length`, with `~` and `/` in a key escaped as `~0` and `~1`), so
any key, a `.` included, has one path; lists compare whole; an absent key and a JSON
null are the same value; values compare as canonical JSON, so `1`, `1.0` and `true`
differ, as they do in the params hash.

**Upstream references.** A processing's `mask_idx`, an alignment's `mask_idx`, and a
de novo alignment's `processing_idx` (a de novo alignment is the one whose params
carry `subject`) name the definition that the run consumed. An output equivalence whose
change touches only such paths is `derived_only`: it is grouped by derivation once its
upstreams are, so it can be decided only with a non-joining verdict.

**Groups**, computed on read from the definitions and the live (not detached) output
equivalences:
  - masks: connected components over joining output equivalences;
  - processings, then alignments: a derived edge joins two definitions whose params
    differ only in upstream references whose referents are in one group (mask
    groups; for de novo alignments also processing groups), plus joining edges.
    When a non-joining output equivalence's two sides are still connected, both sides
    lose their derived edges (their joining edges stay), repeated until no non-joining
    one loses more. A non-joining output equivalence still connected after that is a
    contradiction.
Deprecation does not change membership.

**Public API** for other modules:
  - `async load_groups(conn, *, as_of_event=None) -> Groups` (the caller holds the
    group lock exclusive or reads at REPEATABLE READ; `as_of_event` takes the output
    equivalences' verdicts from an earlier decision set, `decision_set`), then
    `Groups.group_of(kind, idx) -> frozenset[int]` and
    `Groups.same_group(kind, a, b) -> bool`. A definition that no output equivalence or
    derivation joins is a group of one, including an id that the snapshot does not know.
  - `compute_groups(definitions, equivalences) -> Groups`, the pure core.
  - `param_diff(low_params, high_params)`, `upstream_changes(...)`,
    `is_derived_only(...)`.
  - `async open_equivalence`, `decide_equivalence`, `revoke_equivalence`, and
    `refuse_deprecating_with_joining_equivalences` (call it before deprecating a
    definition).
  - Errors: `EquivalenceRefused` (409, with an `OutputEquivalenceRefusal` code and the
    output equivalences it names), `EquivalenceNotFound`, `DefinitionNotFound`, and the
    repository's `GroupLockBusy` (retryable).

**Refusals** (`EquivalenceRefused.reason`):
  - `cross_scope`, `no_difference`: opening an output equivalence across comparison
    scopes (mask: filter workflow; processing: workflow; block alignment: reference and
    aligner; de novo alignment: workflow, aligner and subject), or between definitions
    whose params do not differ.
  - `detached`: deciding or revoking an output equivalence one of whose definitions was
    deleted.
  - `deprecated_side`, `shard_ids_change`, `syndna_reference_change`, `derived_only`,
    `upstream_not_grouped`: deciding a joining verdict when either side is deprecated,
    when the change includes the reference's shard set, when two masks count SynDNA
    against different references (or one counts it and the other does not: a
    group's SynDNA table holds counts against one reference), when the output
    equivalence is derived_only, or (alignment, processing) when its changed upstream
    references are not already grouped. Alignment and processing params carry no
    SynDNA reference.
  - `contradiction`: a decide or revoke after which a non-joining output equivalence
    would sit in one group that does not today.
  - `relied_on`: a decide or revoke after which a joining alignment or processing output
    equivalence whose upstream references are grouped today would have them in
    different groups. This covers chains (mask -> processing -> de novo alignment).
  - `not_decided`: revoking a pending output equivalence.
  - `joining_equivalences`: deprecating a definition named by a joining output
    equivalence.
Each refusal names, in `output_equivalence_idx`, the output equivalence acted on;
`contradiction` names the non-joining ones, `relied_on` the downstream joining ones and
`joining_equivalences` the joining ones naming the definition instead, and
`cross_scope` / `no_difference` name none.
"""

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import asyncpg
from qiita_common.models import (
    JOINING_VERDICTS,
    NON_JOINING_VERDICTS,
    DefinitionKind,
    DeprecatedUpstream,
    OutputEquivalenceRefusal,
    OutputEquivalenceVerdict,
)

from .decision_set import DecisionSet, load_decision_set
from .repositories import (
    READ_COMMITTED,
    SNAPSHOT_ISOLATION_LEVELS,
    require_transaction,
    transaction_isolation,
)
from .repositories.mask_definition import RESOLVED_SYNDNA_KEY, SYNDNA_REFERENCE_IDX_KEY
from .repositories.output_equivalence import (
    DEFINITION_TABLES,
    DefinitionRow,
    GroupLockBusy,
    fetch_definition,
    fetch_definitions,
    fetch_equivalence,
    fetch_joining_equivalences_naming,
    fetch_live_equivalences,
    holds_group_lock_exclusive,
    insert_equivalence,
    lock_groups_exclusive,
    lock_groups_shared,
    record_decision,
    record_revoke,
)
from .repositories.output_equivalence import decode_jsonb as _decode_jsonb

__all__ = [
    "DefinitionNotFound",
    "GroupLockBusy",
    "Groups",
    "EquivalenceNotFound",
    "EquivalenceRefused",
    "EquivalenceState",
    "canonical_json",
    "compute_groups",
    "decide_equivalence",
    "deprecated_upstreams",
    "is_definition_ref",
    "is_derived_only",
    "load_groups",
    "open_equivalence",
    "param_diff",
    "param_path",
    "refuse_deprecating_with_joining_equivalences",
    "revoke_equivalence",
    "upstream_changes",
    "upstream_leaves",
]

# The block alignment's reference shard set (`align_planner` hashes it). A change in
# it is reference growth, which never joins.
_SHARD_IDS_KEY = "shard_ids"
# Present only in a de novo alignment's params.
_DENOVO_SUBJECT_KEY = "subject"
# Kinds in dependency order: each kind's derived edges read the groups of the kinds
# before it.
_KIND_ORDER = (DefinitionKind.MASK, DefinitionKind.PROCESSING, DefinitionKind.ALIGNMENT)
# The two verdict sets as refusal messages word them ("same or similar").
_JOINING_WORDS = " or ".join(sorted(v.value for v in JOINING_VERDICTS))
_NON_JOINING_WORDS = " or ".join(sorted(v.value for v in NON_JOINING_VERDICTS))


class EquivalenceRefused(Exception):
    """An output-equivalence operation or a deprecation that the rules refuse. `reason`
    is the wire code; `output_equivalence_idx` names the output equivalences that the
    refusal is about (the module docstring's Refusals list says which, per code)."""

    def __init__(
        self,
        reason: OutputEquivalenceRefusal,
        message: str,
        *,
        output_equivalence_idx: Sequence[int] = (),
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.output_equivalence_idx = list(output_equivalence_idx)


class EquivalenceNotFound(Exception):
    def __init__(self, output_equivalence_idx: int) -> None:
        super().__init__(f"output_equivalence {output_equivalence_idx} not found")
        self.output_equivalence_idx = output_equivalence_idx


class DefinitionNotFound(Exception):
    def __init__(self, kind: DefinitionKind, idx: int) -> None:
        super().__init__(f"{kind.value} definition {idx} not found")
        self.kind = kind
        self.idx = idx


# ---------------------------------------------------------------------------
# Change
# ---------------------------------------------------------------------------


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _flatten(params: Mapping[str, Any], prefix: tuple[str, ...] = ()) -> dict[tuple[str, ...], Any]:
    """Leaf values by key path. Nulls are dropped, so absent and null are one value;
    an empty object contributes nothing."""
    out: dict[tuple[str, ...], Any] = {}
    for key, value in params.items():
        path = (*prefix, key)
        if isinstance(value, dict):
            out.update(_flatten(value, path))
        elif value is not None:
            out[path] = value
    return out


def param_path(path: Sequence[str]) -> str:
    """`path` as a JSON Pointer (RFC 6901), the form that `change` is keyed by."""
    return "".join("/" + key.replace("~", "~0").replace("/", "~1") for key in path)


def param_diff(low_params: Mapping[str, Any], high_params: Mapping[str, Any]) -> dict[str, list]:
    """`{param_path(path): [low value, high value]}` for every path whose values
    differ, in path order. A missing side is `None`."""
    low, high = _flatten(low_params), _flatten(high_params)
    return {
        param_path(path): [low.get(path), high.get(path)]
        for path in sorted(low.keys() | high.keys())
        if canonical_json(low.get(path)) != canonical_json(high.get(path))
    }


def upstream_leaves(kind: DefinitionKind, params: Mapping[str, Any]) -> dict[str, DefinitionKind]:
    """The top-level param keys of a `kind` definition that name an upstream
    definition, and that definition's kind."""
    if kind is DefinitionKind.PROCESSING:
        return {"mask_idx": DefinitionKind.MASK}
    if kind is DefinitionKind.ALIGNMENT:
        leaves = {"mask_idx": DefinitionKind.MASK}
        if params.get(_DENOVO_SUBJECT_KEY) is not None:
            leaves["processing_idx"] = DefinitionKind.PROCESSING
        return leaves
    return {}


def is_definition_ref(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def upstream_changes(
    kind: DefinitionKind,
    low_params: Mapping[str, Any],
    high_params: Mapping[str, Any],
    change: Mapping[str, Sequence[Any]],
) -> dict[str, tuple[DefinitionKind, int, int]]:
    """The entries of `change` that swap one upstream definition for another:
    `{path: (upstream kind, low ref, high ref)}`. A path counts when both sides
    treat it as an upstream reference and both values are ids."""
    low_leaves = {param_path((k,)): v for k, v in upstream_leaves(kind, low_params).items()}
    high_leaves = {param_path((k,)): v for k, v in upstream_leaves(kind, high_params).items()}
    out: dict[str, tuple[DefinitionKind, int, int]] = {}
    for path, (low_value, high_value) in change.items():
        up_kind = low_leaves.get(path)
        if up_kind is None or high_leaves.get(path) is not up_kind:
            continue
        if is_definition_ref(low_value) and is_definition_ref(high_value):
            out[path] = (up_kind, low_value, high_value)
    return out


def is_derived_only(
    kind: DefinitionKind,
    low_params: Mapping[str, Any],
    high_params: Mapping[str, Any],
    change: Mapping[str, Sequence[Any]],
) -> bool:
    """True when `change` is non-empty and every entry is an upstream swap."""
    return bool(change) and len(upstream_changes(kind, low_params, high_params, change)) == len(
        change
    )


def comparison_scope(definition: DefinitionRow) -> tuple[str, ...]:
    """Two definitions may be compared by hand only when their scopes are equal."""
    if definition.kind is DefinitionKind.ALIGNMENT:
        params = definition.params
        if params.get(_DENOVO_SUBJECT_KEY) is not None:
            return (
                "alignment-denovo",
                canonical_json(params.get("workflow")),
                canonical_json(params.get("aligner")),
                canonical_json(params.get(_DENOVO_SUBJECT_KEY)),
            )
        return (
            "alignment-block",
            canonical_json(params.get("reference_idx")),
            canonical_json(params.get("aligner")),
        )
    return (definition.kind.value, canonical_json(definition.workflow))


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EquivalenceState:
    """What the group rules read of one live output equivalence."""

    idx: int
    kind: DefinitionKind
    low: int
    high: int
    verdict: OutputEquivalenceVerdict
    change: Mapping[str, Sequence[Any]]
    derived_only: bool


def equivalence_state(row: Mapping[str, Any]) -> EquivalenceState:
    """An output-equivalence row (the repository's projection) as an EquivalenceState."""
    change = row["change"]
    return EquivalenceState(
        idx=row["output_equivalence_idx"],
        kind=DefinitionKind(row["definition_kind"]),
        low=row["low_definition_idx"],
        high=row["high_definition_idx"],
        verdict=OutputEquivalenceVerdict(row["verdict"]),
        change=json.loads(change) if isinstance(change, str) else change,
        derived_only=row["derived_only"],
    )


class _UnionFind:
    def __init__(self, nodes: Iterable[int]) -> None:
        self._parent = {n: n for n in nodes}

    def find(self, node: int) -> int:
        self._parent.setdefault(node, node)
        root = node
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[node] != root:
            self._parent[node], node = root, self._parent[node]
        return root

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[max(ra, rb)] = min(ra, rb)

    def components(self) -> dict[int, frozenset[int]]:
        by_root: dict[int, set[int]] = {}
        for node in list(self._parent):
            by_root.setdefault(self.find(node), set()).add(node)
        return {node: frozenset(members) for members in by_root.values() for node in members}


@dataclass(frozen=True)
class Groups:
    """Group membership of every kind, computed from one snapshot of definitions and
    live output equivalences. `contradictions` holds the non-joining ones whose sides
    share a group."""

    definitions: Mapping[tuple[DefinitionKind, int], DefinitionRow]
    equivalences: tuple[EquivalenceState, ...]
    _members: Mapping[DefinitionKind, Mapping[int, frozenset[int]]] = field(repr=False)
    contradictions: frozenset[int]
    # The decision set the equivalences' verdicts were read under; set by `load_groups`.
    decisions: DecisionSet | None = None

    def group_of(self, kind: DefinitionKind, idx: int) -> frozenset[int]:
        return self._members[kind].get(idx, frozenset({idx}))

    def same_group(self, kind: DefinitionKind, a: int, b: int) -> bool:
        return b in self.group_of(kind, a)

    def params_of(self, kind: DefinitionKind, idx: int) -> Mapping[str, Any]:
        definition = self.definitions.get((kind, idx))
        return {} if definition is None else definition.params

    def equivalence_upstream_changes(
        self, equivalence: EquivalenceState
    ) -> dict[str, tuple[DefinitionKind, int, int]]:
        return upstream_changes(
            equivalence.kind,
            self.params_of(equivalence.kind, equivalence.low),
            self.params_of(equivalence.kind, equivalence.high),
            equivalence.change,
        )

    def upstream_grouped(self, equivalence: EquivalenceState) -> bool:
        """True when every upstream reference the equivalence's change swaps names two
        definitions of one group."""
        return all(
            self.same_group(up_kind, low, high)
            for up_kind, low, high in self.equivalence_upstream_changes(equivalence).values()
        )


def _derived_key(definition: DefinitionRow, members: Mapping[DefinitionKind, Mapping]) -> str:
    """Equal for two definitions exactly when their params differ only in upstream
    references whose referents share a group."""
    leaves = upstream_leaves(definition.kind, definition.params)
    items: list[list[Any]] = []
    for path, value in sorted(_flatten(definition.params).items()):
        up_kind = leaves.get(path[0]) if len(path) == 1 else None
        if up_kind is not None and is_definition_ref(value):
            group = members[up_kind].get(value, frozenset({value}))
            # An object never appears as a flattened leaf, so it cannot collide
            # with a plain value at the same path.
            items.append([list(path), {"upstream_group": min(group)}])
        else:
            items.append([list(path), value])
    return canonical_json(items)


def _compute_kind(
    kind: DefinitionKind,
    definitions: Sequence[DefinitionRow],
    equivalences: Sequence[EquivalenceState],
    members: Mapping[DefinitionKind, Mapping[int, frozenset[int]]],
) -> tuple[dict[int, frozenset[int]], set[int]]:
    """One kind's membership and its contradicting non-joining output equivalences."""
    joining = [(p.low, p.high) for p in equivalences if p.verdict in JOINING_VERDICTS]
    non_joining = [p for p in equivalences if p.verdict in NON_JOINING_VERDICTS]

    derived_classes: dict[str, list[int]] = {}
    if kind is not DefinitionKind.MASK:
        for d in definitions:
            derived_classes.setdefault(_derived_key(d, members), []).append(d.idx)
    with_derived_edges = {
        idx for members_ in derived_classes.values() if len(members_) > 1 for idx in members_
    }

    excluded: set[int] = set()
    while True:
        uf = _UnionFind(d.idx for d in definitions)
        for class_members in derived_classes.values():
            kept = [idx for idx in class_members if idx not in excluded]
            for idx in kept[1:]:
                uf.union(kept[0], idx)
        for low, high in joining:
            uf.union(low, high)
        newly = {
            side
            for p in non_joining
            if uf.find(p.low) == uf.find(p.high)
            for side in (p.low, p.high)
            if side in with_derived_edges and side not in excluded
        }
        if not newly:
            break
        excluded |= newly

    contradictions = {p.idx for p in non_joining if uf.find(p.low) == uf.find(p.high)}
    return uf.components(), contradictions


def compute_groups(
    definitions: Iterable[DefinitionRow], equivalences: Iterable[EquivalenceState]
) -> Groups:
    """Groups of every kind from `definitions` and the live `equivalences`."""
    definitions = list(definitions)
    equivalences = tuple(equivalences)
    members: dict[DefinitionKind, Mapping[int, frozenset[int]]] = {}
    contradictions: set[int] = set()
    for kind in _KIND_ORDER:
        kind_members, kind_contradictions = _compute_kind(
            kind,
            [d for d in definitions if d.kind is kind],
            [p for p in equivalences if p.kind is kind],
            members,
        )
        members[kind] = kind_members
        contradictions |= kind_contradictions
    return Groups(
        definitions={(d.kind, d.idx): d for d in definitions},
        equivalences=equivalences,
        _members=members,
        contradictions=frozenset(contradictions),
    )


async def load_groups(conn: asyncpg.Connection, *, as_of_event: str | None = None) -> Groups:
    """Groups as of the caller's transaction, or, with `as_of_event`, from the equivalences'
    verdicts at that decision set (`decision_set`; raises `DecisionSetUnknown`) with
    everything else current. `Groups.decisions` records which. The guards never pass
    `as_of_event`. The caller either holds the group lock
    EXCLUSIVE (a decision or revoke) at READ COMMITTED, so that it reads every
    decision committed before it took the lock (`lock_groups_exclusive`), or reads at
    REPEATABLE READ without it: there the transaction's one snapshot keeps the groups
    consistent with the gates and tickets read beside them, and a shared group lock
    would add nothing, since a reader that waited on it would still read the snapshot
    taken before the wait."""
    require_transaction(conn)
    isolation = await transaction_isolation(conn)
    if await holds_group_lock_exclusive(conn):
        if isolation != READ_COMMITTED:
            raise RuntimeError(
                "load_groups under the group lock EXCLUSIVE needs a READ COMMITTED"
                f" transaction; this one is {isolation!r}, whose snapshot can predate"
                " a decision that committed before the lock was taken"
            )
    elif isolation not in SNAPSHOT_ISOLATION_LEVELS:
        raise RuntimeError(
            "load_groups needs the group lock EXCLUSIVE or a REPEATABLE READ transaction;"
            f" this one is {isolation!r} without the lock"
        )
    definitions = await fetch_definitions(conn)
    decisions = await load_decision_set(conn, as_of_event)
    equivalences = [equivalence_state(r) for r in await fetch_live_equivalences(conn)]
    if decisions.as_of:
        equivalences = [replace(p, verdict=decisions.verdict_of(p.idx)) for p in equivalences]
    return replace(compute_groups(definitions, equivalences), decisions=decisions)


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def _with_verdict(
    groups: Groups, equivalence_idx: int, verdict: OutputEquivalenceVerdict
) -> Groups:
    equivalences = [
        replace(p, verdict=verdict) if p.idx == equivalence_idx else p for p in groups.equivalences
    ]
    return compute_groups(groups.definitions.values(), equivalences)


def _refuse_transition(
    current: Groups, proposed: Groups, *, equivalence: EquivalenceState, action: str
) -> None:
    """Refuse a change of `equivalence`'s verdict that makes a new contradiction or
    leaves the upstream definitions of a joining output equivalence ungrouped."""
    new_contradictions = sorted(proposed.contradictions - current.contradictions)
    if new_contradictions:
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.CONTRADICTION,
            f"{action} output equivalence {equivalence.idx} would put the two sides of"
            f" {_NON_JOINING_WORDS} output equivalence(s) {new_contradictions} in one group",
            output_equivalence_idx=new_contradictions,
        )
    broken = sorted(
        p.idx
        for p in proposed.equivalences
        if p.verdict in JOINING_VERDICTS
        and p.idx != equivalence.idx
        and current.upstream_grouped(p)
        and not proposed.upstream_grouped(p)
    )
    if broken:
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.RELIED_ON,
            f"{action} output equivalence {equivalence.idx} would leave the upstream definitions"
            f" of {_JOINING_WORDS} output equivalence(s) {broken} in different groups; revoke or"
            " re-decide those first",
            output_equivalence_idx=broken,
        )


def _syndna_reference(params: Mapping[str, Any]) -> int | None:
    """A mask's SynDNA reference_idx, or None when it ran without SynDNA. Raises on a
    stored value of another shape."""
    resolved = params.get(RESOLVED_SYNDNA_KEY)
    if resolved is None:
        return None
    if not isinstance(resolved, dict) or not is_definition_ref(
        resolved.get(SYNDNA_REFERENCE_IDX_KEY)
    ):
        raise ValueError(f"mask params carry a malformed {RESOLVED_SYNDNA_KEY}: {resolved!r}")
    return resolved[SYNDNA_REFERENCE_IDX_KEY]


def _refuse_joining(groups: Groups, equivalence: EquivalenceState) -> None:
    """The refusals of a joining verdict, which a non-joining one does not meet."""
    deprecated = [
        idx
        for idx in (equivalence.low, equivalence.high)
        if (d := groups.definitions.get((equivalence.kind, idx))) is not None and d.deprecated
    ]
    if deprecated:
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.DEPRECATED_SIDE,
            f"{equivalence.kind.value} {deprecated} is deprecated; an output equivalence with a"
            f" deprecated side can be decided {_NON_JOINING_WORDS} only",
            output_equivalence_idx=[equivalence.idx],
        )
    if param_path((_SHARD_IDS_KEY,)) in equivalence.change:
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.SHARD_IDS_CHANGE,
            f"output equivalence {equivalence.idx} differs in the reference shard set; reference"
            f" growth can be decided {_NON_JOINING_WORDS} only",
            output_equivalence_idx=[equivalence.idx],
        )
    if equivalence.kind is DefinitionKind.MASK:
        references = [
            _syndna_reference(groups.params_of(DefinitionKind.MASK, idx))
            for idx in (equivalence.low, equivalence.high)
        ]
        if references[0] != references[1]:
            raise EquivalenceRefused(
                OutputEquivalenceRefusal.SYNDNA_REFERENCE_CHANGE,
                f"output equivalence {equivalence.idx} counts SynDNA against different references"
                f" ({references[0]} and {references[1]}; None is no SynDNA); a group's"
                " SynDNA counts are against one reference, so it can be decided"
                f" {_NON_JOINING_WORDS} only",
                output_equivalence_idx=[equivalence.idx],
            )
    if equivalence.derived_only:
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.DERIVED_ONLY,
            f"output equivalence {equivalence.idx} differs only in upstream references, so it is"
            f" grouped by derivation when those are; it can be decided {_NON_JOINING_WORDS} only",
            output_equivalence_idx=[equivalence.idx],
        )
    ungrouped = sorted(
        path
        for path, (up_kind, low, high) in groups.equivalence_upstream_changes(equivalence).items()
        if not groups.same_group(up_kind, low, high)
    )
    if ungrouped:
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.UPSTREAM_NOT_GROUPED,
            f"output equivalence {equivalence.idx} swaps upstream references {ungrouped} whose"
            f" definitions are not in one group; decide those {_JOINING_WORDS} first",
            output_equivalence_idx=[equivalence.idx],
        )


def check_decision(
    groups: Groups, equivalence: EquivalenceState, verdict: OutputEquivalenceVerdict
) -> None:
    """Raise EquivalenceRefused unless `equivalence` may be decided `verdict` against
    `groups`. `equivalence` must be live (in `groups.equivalences`)."""
    if verdict in JOINING_VERDICTS:
        _refuse_joining(groups, equivalence)
    elif verdict not in NON_JOINING_VERDICTS:
        raise ValueError(
            f"a decision is {_JOINING_WORDS} or {_NON_JOINING_WORDS}, not {verdict.value!r};"
            " revoke returns an output equivalence to pending"
        )
    _refuse_transition(
        groups,
        _with_verdict(groups, equivalence.idx, verdict),
        equivalence=equivalence,
        action="deciding",
    )


def check_revoke(groups: Groups, equivalence: EquivalenceState) -> None:
    """Raise EquivalenceRefused unless `equivalence`'s decision may be revoked against
    `groups`."""
    if equivalence.verdict is OutputEquivalenceVerdict.PENDING:
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.NOT_DECIDED,
            f"output equivalence {equivalence.idx} is pending; there is no decision to revoke",
            output_equivalence_idx=[equivalence.idx],
        )
    _refuse_transition(
        groups,
        _with_verdict(groups, equivalence.idx, OutputEquivalenceVerdict.PENDING),
        equivalence=equivalence,
        action="revoking",
    )


def check_open(low: DefinitionRow, high: DefinitionRow) -> dict[str, list]:
    """Return the change between two definitions of one kind, or raise EquivalenceRefused
    when they may not be compared."""
    if comparison_scope(low) != comparison_scope(high):
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.CROSS_SCOPE,
            f"{low.kind.value} {low.idx} and {high.idx} are in different comparison scopes"
            f" ({comparison_scope(low)} vs {comparison_scope(high)})",
        )
    change = param_diff(low.params, high.params)
    if not change:
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.NO_DIFFERENCE,
            f"{low.kind.value} {low.idx} and {high.idx} have the same params",
        )
    return change


# ---------------------------------------------------------------------------
# Operations (each in the caller's transaction)
# ---------------------------------------------------------------------------


async def open_equivalence(
    conn: asyncpg.Connection,
    *,
    kind: DefinitionKind,
    definition_idxs: Sequence[int],
    principal_idx: int,
) -> tuple[asyncpg.Record, bool]:
    """Open a pending output equivalence of two `kind` definitions, or return the one
    already recorded. Returns `(row, created)`."""
    require_transaction(conn)
    low_idx, high_idx = sorted(definition_idxs)
    if low_idx == high_idx:
        raise ValueError("an output equivalence names two different definitions")
    rows = []
    for idx in (low_idx, high_idx):
        row = await fetch_definition(conn, kind, idx)
        if row is None:
            raise DefinitionNotFound(kind, idx)
        rows.append(row)
    low, high = rows
    change = check_open(low, high)
    return await insert_equivalence(
        conn,
        kind=kind,
        low_idx=low_idx,
        high_idx=high_idx,
        change=change,
        derived_only=is_derived_only(kind, low.params, high.params, change),
        opened_by_idx=principal_idx,
    )


async def _locked_live_equivalence(
    conn: asyncpg.Connection, output_equivalence_idx: int
) -> EquivalenceState:
    """Take the group lock EXCLUSIVE (raising GroupLockBusy), then the output-equivalence
    row; refuse a detached one."""
    await lock_groups_exclusive(conn)
    row = await fetch_equivalence(conn, output_equivalence_idx, for_update=True)
    if row is None:
        raise EquivalenceNotFound(output_equivalence_idx)
    if row["detached_at"] is not None:
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.DETACHED,
            f"output equivalence {output_equivalence_idx} is detached: one of its definitions"
            " was deleted",
            output_equivalence_idx=[output_equivalence_idx],
        )
    return equivalence_state(row)


async def decide_equivalence(
    conn: asyncpg.Connection,
    *,
    output_equivalence_idx: int,
    verdict: OutputEquivalenceVerdict,
    reason: str,
    principal_idx: int,
) -> asyncpg.Record:
    """Record a decision on a live output equivalence, or raise EquivalenceRefused /
    EquivalenceNotFound / GroupLockBusy. Returns the updated row."""
    require_transaction(conn)
    equivalence = await _locked_live_equivalence(conn, output_equivalence_idx)
    check_decision(await load_groups(conn), equivalence, verdict)
    return await record_decision(
        conn,
        output_equivalence_idx=output_equivalence_idx,
        verdict=verdict,
        reason=reason,
        principal_idx=principal_idx,
    )


async def revoke_equivalence(
    conn: asyncpg.Connection, *, output_equivalence_idx: int, reason: str, principal_idx: int
) -> asyncpg.Record:
    """Return a decided live output equivalence to pending, or raise EquivalenceRefused /
    EquivalenceNotFound / GroupLockBusy. Returns the updated row."""
    require_transaction(conn)
    equivalence = await _locked_live_equivalence(conn, output_equivalence_idx)
    check_revoke(await load_groups(conn), equivalence)
    return await record_revoke(
        conn,
        output_equivalence_idx=output_equivalence_idx,
        reason=reason,
        principal_idx=principal_idx,
    )


async def refuse_deprecating_with_joining_equivalences(
    conn: asyncpg.Connection, kind: DefinitionKind, idx: int
) -> None:
    """Take the group lock SHARED, then raise EquivalenceRefused (`joining_equivalences`)
    if a live joining output equivalence names `idx`. A definition grouped only by derivation is not
    refused. Call before taking the definition's row lock."""
    require_transaction(conn)
    await lock_groups_shared(conn)
    equivalences = await fetch_joining_equivalences_naming(conn, kind, idx)
    if equivalences:
        raise EquivalenceRefused(
            OutputEquivalenceRefusal.JOINING_EQUIVALENCES,
            f"{kind.value} {idx} is a side of {_JOINING_WORDS} output equivalence(s)"
            f" {equivalences}; revoke them or re-decide them {_NON_JOINING_WORDS} before"
            " deprecating it",
            output_equivalence_idx=equivalences,
        )


async def deprecated_upstreams(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection, alignment: asyncpg.Record
) -> list[DeprecatedUpstream]:
    """The deprecated definitions `alignment` read: its mask and, for a de novo
    alignment, its assembly run and that run's mask, in that order, each once."""
    out: list[DeprecatedUpstream] = []
    seen: set[tuple[DefinitionKind, int]] = set()
    stack = [(DefinitionKind.ALIGNMENT, _decode_jsonb(alignment["params"]))]
    while stack:
        kind, params = stack.pop(0)
        for path, up_kind in upstream_leaves(kind, params).items():
            idx = params.get(path)
            if not is_definition_ref(idx) or (up_kind, idx) in seen:
                continue
            seen.add((up_kind, idx))
            table = DEFINITION_TABLES[up_kind]
            row = await pool_or_conn.fetchrow(
                f"SELECT status, deprecation_reason, params, params_hash FROM {table.table}"
                f" WHERE {table.id_column} = $1",
                idx,
            )
            if row is None:
                # `params` names its upstream by id, and a definition named by
                # another's params is not deleted by any route.
                raise DefinitionNotFound(up_kind, idx)
            if row["status"] == table.deprecated_status:
                out.append(
                    DeprecatedUpstream(
                        definition_kind=up_kind,
                        definition_idx=idx,
                        params_hash=bytes(row["params_hash"]).hex(),
                        deprecation_reason=row["deprecation_reason"],
                    )
                )
            stack.append((up_kind, _decode_jsonb(row["params"])))
    return out
