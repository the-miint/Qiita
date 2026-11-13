"""Similarity between definitions of one kind (masks, alignments, processings):
the change between two definitions, the groups that pair decisions form, and every
refusal. The rules are stated here and nowhere else.

**Change.** `param_diff` compares two definitions' stored params. Nested objects are
walked into dotted paths (`resolved_qc.min_length`); lists compare whole; an absent
key and a JSON null are the same value; values compare as canonical JSON, so `1`,
`1.0` and `true` differ, as they do in the params hash.

**Upstream references.** A processing's `mask_idx`, an alignment's `mask_idx`, and a
de novo alignment's `processing_idx` (a de novo alignment is the one whose params
carry `subject`) name the definition that the run consumed. A pair whose change
touches only such paths is `derived_only`: it is grouped by derivation once its
upstreams are, so it can be decided `not_similar` only.

**Groups**, computed on read from the definitions and the live (not detached) pairs
(that is, the groups that pair decisions form):
  - masks: connected components over `similar` pairs;
  - processings, then alignments: a derived edge joins two definitions whose params
    differ only in upstream references whose referents are in one group (mask
    groups; for de novo alignments also processing groups), plus `similar` edges.
    When a `not_similar` pair's two sides are still connected, both sides lose
    their derived edges (their `similar` edges stay), repeated until no `not_similar`
    pair loses more. A `not_similar` pair still connected after that is a
    contradiction.
Deprecation does not change membership.

**Public API** for other modules:
  - `async load_groups(conn) -> Groups` (takes the group lock SHARED in the
    caller's transaction), then `Groups.group_of(kind, idx) -> frozenset[int]` and
    `Groups.same_group(kind, a, b) -> bool`. A definition that no pair or derivation
    joins is a group of one, including an id that the snapshot does not know.
  - `compute_groups(definitions, pairs) -> Groups`, the pure core.
  - `param_diff(low_params, high_params)`, `upstream_changes(...)`,
    `is_derived_only(...)`.
  - `async open_pair`, `decide_pair`, `revoke_pair`, and
    `refuse_deprecating_with_similar_pairs` (call it before deprecating a definition).
  - Errors: `PairRefused` (409, with a `DefinitionPairRefusal` code and the pairs it
    names), `PairNotFound`, `DefinitionNotFound`, and the repository's
    `GroupLockBusy` (retryable).

**Refusals** (`PairRefused.reason`):
  - `cross_scope`, `no_difference`: opening a pair across comparison scopes (mask:
    filter workflow; processing: workflow; block alignment: reference and aligner;
    de novo alignment: workflow, aligner and subject), or between definitions whose
    params do not differ.
  - `detached`: deciding or revoking a pair one of whose definitions was deleted.
  - `deprecated_side`, `shard_ids_change`, `derived_only`, `upstream_not_grouped`:
    deciding `similar` when either side is deprecated, when the change includes
    the reference's shard set, when the pair is derived_only, or (alignment,
    processing) when its changed upstream references are not already grouped.
  - `contradiction`: a decide or revoke after which a `not_similar` pair would sit in
    one group that does not today.
  - `relied_on`: a decide or revoke after which an alignment or processing
    `similar` pair whose upstream references are grouped today would have them in
    different groups. This covers chains (mask -> processing -> de novo alignment).
  - `not_decided`: revoking a pending pair.
  - `similar_pairs`: deprecating a definition named by a `similar` pair.
Each refusal names, in `definition_pair_idx`, the pair acted on; `contradiction`
names the `not_similar` pairs, `relied_on` the downstream `similar` pairs and
`similar_pairs` the `similar` pairs naming the definition instead, and
`cross_scope` / `no_difference` name none.
"""

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import asyncpg
from qiita_common.models import DefinitionKind, DefinitionPairRefusal, DefinitionPairVerdict

from .repositories import require_transaction
from .repositories.definition_pair import (
    DefinitionRow,
    GroupLockBusy,
    fetch_definition,
    fetch_definitions,
    fetch_live_pairs,
    fetch_pair,
    fetch_similar_pairs_naming,
    insert_pair,
    lock_groups_exclusive,
    lock_groups_shared,
    record_decision,
    record_revoke,
)

__all__ = [
    "DefinitionNotFound",
    "GroupLockBusy",
    "Groups",
    "PairNotFound",
    "PairRefused",
    "PairState",
    "canonical_json",
    "compute_groups",
    "decide_pair",
    "is_definition_ref",
    "is_derived_only",
    "load_groups",
    "open_pair",
    "param_diff",
    "refuse_deprecating_with_similar_pairs",
    "revoke_pair",
    "upstream_changes",
    "upstream_leaves",
]

# The block alignment's reference shard set (`align_planner` hashes it). A change in
# it is reference growth, which is never `similar`.
_SHARD_IDS_PATH = "shard_ids"
# Present only in a de novo alignment's params.
_DENOVO_SUBJECT_KEY = "subject"
# Kinds in dependency order: each kind's derived edges read the groups of the kinds
# before it.
_KIND_ORDER = (DefinitionKind.MASK, DefinitionKind.PROCESSING, DefinitionKind.ALIGNMENT)


class PairRefused(Exception):
    """A pair operation or a deprecation that the rules refuse. `reason` is the wire
    code; `definition_pair_idx` names the pairs that the refusal is about (the module
    docstring's Refusals list says which, per code)."""

    def __init__(
        self,
        reason: DefinitionPairRefusal,
        message: str,
        *,
        definition_pair_idx: Sequence[int] = (),
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.definition_pair_idx = list(definition_pair_idx)


class PairNotFound(Exception):
    def __init__(self, definition_pair_idx: int) -> None:
        super().__init__(f"definition_pair {definition_pair_idx} not found")
        self.definition_pair_idx = definition_pair_idx


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


def _flatten(params: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    """Leaf values by dotted path. Nulls are dropped, so absent and null are one value;
    an empty object contributes nothing."""
    out: dict[str, Any] = {}
    for key, value in params.items():
        if "." in key:
            raise ValueError(
                f"param key {prefix}{key!r} contains '.', which the dotted change paths"
                " cannot represent unambiguously"
            )
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            out.update(_flatten(value, f"{path}."))
        elif value is not None:
            out[path] = value
    return out


def param_diff(low_params: Mapping[str, Any], high_params: Mapping[str, Any]) -> dict[str, list]:
    """`{dotted path: [low value, high value]}` for every path whose values differ,
    sorted by path. A missing side is `None`."""
    low, high = _flatten(low_params), _flatten(high_params)
    return {
        path: [low.get(path), high.get(path)]
        for path in sorted(low.keys() | high.keys())
        if canonical_json(low.get(path)) != canonical_json(high.get(path))
    }


def upstream_leaves(kind: DefinitionKind, params: Mapping[str, Any]) -> dict[str, DefinitionKind]:
    """The param paths of a `kind` definition that name an upstream definition, and
    that definition's kind."""
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
    low_leaves = upstream_leaves(kind, low_params)
    high_leaves = upstream_leaves(kind, high_params)
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
    """Two definitions may be paired by hand only when their scopes are equal."""
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
class PairState:
    """What the group rules read of one live pair."""

    idx: int
    kind: DefinitionKind
    low: int
    high: int
    verdict: DefinitionPairVerdict
    change: Mapping[str, Sequence[Any]]
    derived_only: bool


def pair_state(row: Mapping[str, Any]) -> PairState:
    """A pair row (the repository's projection) as a PairState."""
    change = row["change"]
    return PairState(
        idx=row["definition_pair_idx"],
        kind=DefinitionKind(row["definition_kind"]),
        low=row["low_definition_idx"],
        high=row["high_definition_idx"],
        verdict=DefinitionPairVerdict(row["verdict"]),
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
    live pairs. `contradictions` holds the `not_similar` pairs whose sides share a
    group."""

    definitions: Mapping[tuple[DefinitionKind, int], DefinitionRow]
    pairs: tuple[PairState, ...]
    _members: Mapping[DefinitionKind, Mapping[int, frozenset[int]]] = field(repr=False)
    contradictions: frozenset[int]

    def group_of(self, kind: DefinitionKind, idx: int) -> frozenset[int]:
        return self._members[kind].get(idx, frozenset({idx}))

    def same_group(self, kind: DefinitionKind, a: int, b: int) -> bool:
        return b in self.group_of(kind, a)

    def params_of(self, kind: DefinitionKind, idx: int) -> Mapping[str, Any]:
        definition = self.definitions.get((kind, idx))
        return {} if definition is None else definition.params

    def pair_upstream_changes(self, pair: PairState) -> dict[str, tuple[DefinitionKind, int, int]]:
        return upstream_changes(
            pair.kind,
            self.params_of(pair.kind, pair.low),
            self.params_of(pair.kind, pair.high),
            pair.change,
        )

    def upstream_grouped(self, pair: PairState) -> bool:
        """True when every upstream reference the pair's change swaps names two
        definitions of one group."""
        return all(
            self.same_group(up_kind, low, high)
            for up_kind, low, high in self.pair_upstream_changes(pair).values()
        )


def _derived_key(definition: DefinitionRow, members: Mapping[DefinitionKind, Mapping]) -> str:
    """Equal for two definitions exactly when their params differ only in upstream
    references whose referents share a group."""
    leaves = upstream_leaves(definition.kind, definition.params)
    items: dict[str, Any] = {}
    for path, value in _flatten(definition.params).items():
        up_kind = leaves.get(path)
        if up_kind is not None and is_definition_ref(value):
            group = members[up_kind].get(value, frozenset({value}))
            # An object never appears as a flattened leaf, so it cannot collide
            # with a plain value at the same path.
            items[path] = {"upstream_group": min(group)}
        else:
            items[path] = value
    return canonical_json(items)


def _compute_kind(
    kind: DefinitionKind,
    definitions: Sequence[DefinitionRow],
    pairs: Sequence[PairState],
    members: Mapping[DefinitionKind, Mapping[int, frozenset[int]]],
) -> tuple[dict[int, frozenset[int]], set[int]]:
    """One kind's membership and its contradicting `not_similar` pairs."""
    similar = [(p.low, p.high) for p in pairs if p.verdict is DefinitionPairVerdict.SIMILAR]
    not_similar = [p for p in pairs if p.verdict is DefinitionPairVerdict.NOT_SIMILAR]

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
        for low, high in similar:
            uf.union(low, high)
        newly = {
            side
            for p in not_similar
            if uf.find(p.low) == uf.find(p.high)
            for side in (p.low, p.high)
            if side in with_derived_edges and side not in excluded
        }
        if not newly:
            break
        excluded |= newly

    contradictions = {p.idx for p in not_similar if uf.find(p.low) == uf.find(p.high)}
    return uf.components(), contradictions


def compute_groups(definitions: Iterable[DefinitionRow], pairs: Iterable[PairState]) -> Groups:
    """Groups of every kind from `definitions` and the live `pairs`."""
    definitions = list(definitions)
    pairs = tuple(pairs)
    members: dict[DefinitionKind, Mapping[int, frozenset[int]]] = {}
    contradictions: set[int] = set()
    for kind in _KIND_ORDER:
        kind_members, kind_contradictions = _compute_kind(
            kind,
            [d for d in definitions if d.kind is kind],
            [p for p in pairs if p.kind is kind],
            members,
        )
        members[kind] = kind_members
        contradictions |= kind_contradictions
    return Groups(
        definitions={(d.kind, d.idx): d for d in definitions},
        pairs=pairs,
        _members=members,
        contradictions=frozenset(contradictions),
    )


async def load_groups(conn: asyncpg.Connection) -> Groups:
    """Groups as of the caller's transaction (its snapshot, at REPEATABLE READ). Takes
    the group lock SHARED until the transaction ends; `lock_groups_shared` says what
    that holds off."""
    require_transaction(conn)
    await lock_groups_shared(conn)
    definitions = await fetch_definitions(conn)
    pairs = [pair_state(r) for r in await fetch_live_pairs(conn)]
    return compute_groups(definitions, pairs)


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def _with_verdict(groups: Groups, pair_idx: int, verdict: DefinitionPairVerdict) -> Groups:
    pairs = [replace(p, verdict=verdict) if p.idx == pair_idx else p for p in groups.pairs]
    return compute_groups(groups.definitions.values(), pairs)


def _refuse_transition(current: Groups, proposed: Groups, *, pair: PairState, action: str) -> None:
    """Refuse a change of `pair`'s verdict that makes a new contradiction or leaves an
    upstream `similar` reliance ungrouped."""
    new_contradictions = sorted(proposed.contradictions - current.contradictions)
    if new_contradictions:
        raise PairRefused(
            DefinitionPairRefusal.CONTRADICTION,
            f"{action} pair {pair.idx} would put the two sides of not_similar pair(s)"
            f" {new_contradictions} in one group",
            definition_pair_idx=new_contradictions,
        )
    broken = sorted(
        p.idx
        for p in proposed.pairs
        if p.verdict is DefinitionPairVerdict.SIMILAR
        and p.idx != pair.idx
        and current.upstream_grouped(p)
        and not proposed.upstream_grouped(p)
    )
    if broken:
        raise PairRefused(
            DefinitionPairRefusal.RELIED_ON,
            f"{action} pair {pair.idx} would leave the upstream definitions of similar"
            f" pair(s) {broken} in different groups; revoke or re-decide those first",
            definition_pair_idx=broken,
        )


def _refuse_similar(groups: Groups, pair: PairState) -> None:
    deprecated = [
        idx
        for idx in (pair.low, pair.high)
        if (d := groups.definitions.get((pair.kind, idx))) is not None and d.deprecated
    ]
    if deprecated:
        raise PairRefused(
            DefinitionPairRefusal.DEPRECATED_SIDE,
            f"{pair.kind.value} {deprecated} is deprecated; a pair with a deprecated side"
            " can be decided not_similar only",
            definition_pair_idx=[pair.idx],
        )
    if _SHARD_IDS_PATH in pair.change:
        raise PairRefused(
            DefinitionPairRefusal.SHARD_IDS_CHANGE,
            f"pair {pair.idx} differs in the reference shard set; reference growth is"
            " never similar",
            definition_pair_idx=[pair.idx],
        )
    if pair.derived_only:
        raise PairRefused(
            DefinitionPairRefusal.DERIVED_ONLY,
            f"pair {pair.idx} differs only in upstream references, so it is grouped by"
            " derivation when those are; it can be decided not_similar only",
            definition_pair_idx=[pair.idx],
        )
    ungrouped = sorted(
        path
        for path, (up_kind, low, high) in groups.pair_upstream_changes(pair).items()
        if not groups.same_group(up_kind, low, high)
    )
    if ungrouped:
        raise PairRefused(
            DefinitionPairRefusal.UPSTREAM_NOT_GROUPED,
            f"pair {pair.idx} swaps upstream references {ungrouped} whose definitions are"
            " not in one group; decide those similar first",
            definition_pair_idx=[pair.idx],
        )


def check_decision(groups: Groups, pair: PairState, verdict: DefinitionPairVerdict) -> None:
    """Raise PairRefused unless `pair` may be decided `verdict` against `groups`.
    `pair` must be live (in `groups.pairs`)."""
    if verdict is DefinitionPairVerdict.PENDING:
        raise ValueError("a decision is similar or not_similar; revoke returns a pair to pending")
    if verdict is DefinitionPairVerdict.SIMILAR:
        _refuse_similar(groups, pair)
    _refuse_transition(
        groups, _with_verdict(groups, pair.idx, verdict), pair=pair, action="deciding"
    )


def check_revoke(groups: Groups, pair: PairState) -> None:
    """Raise PairRefused unless `pair`'s decision may be revoked against `groups`."""
    if pair.verdict is DefinitionPairVerdict.PENDING:
        raise PairRefused(
            DefinitionPairRefusal.NOT_DECIDED,
            f"pair {pair.idx} is pending; there is no decision to revoke",
            definition_pair_idx=[pair.idx],
        )
    _refuse_transition(
        groups,
        _with_verdict(groups, pair.idx, DefinitionPairVerdict.PENDING),
        pair=pair,
        action="revoking",
    )


def check_open(low: DefinitionRow, high: DefinitionRow) -> dict[str, list]:
    """Return the change between two definitions of one kind, or raise PairRefused
    when they may not be paired."""
    if comparison_scope(low) != comparison_scope(high):
        raise PairRefused(
            DefinitionPairRefusal.CROSS_SCOPE,
            f"{low.kind.value} {low.idx} and {high.idx} are in different comparison scopes"
            f" ({comparison_scope(low)} vs {comparison_scope(high)})",
        )
    change = param_diff(low.params, high.params)
    if not change:
        raise PairRefused(
            DefinitionPairRefusal.NO_DIFFERENCE,
            f"{low.kind.value} {low.idx} and {high.idx} have the same params",
        )
    return change


# ---------------------------------------------------------------------------
# Operations (each in the caller's transaction)
# ---------------------------------------------------------------------------


async def open_pair(
    conn: asyncpg.Connection,
    *,
    kind: DefinitionKind,
    definition_idxs: Sequence[int],
    principal_idx: int,
) -> tuple[asyncpg.Record, bool]:
    """Open a pending pair of two `kind` definitions, or return the one already
    recorded. Returns `(row, created)`."""
    require_transaction(conn)
    low_idx, high_idx = sorted(definition_idxs)
    if low_idx == high_idx:
        raise ValueError("a pair names two different definitions")
    rows = []
    for idx in (low_idx, high_idx):
        row = await fetch_definition(conn, kind, idx)
        if row is None:
            raise DefinitionNotFound(kind, idx)
        rows.append(row)
    low, high = rows
    change = check_open(low, high)
    return await insert_pair(
        conn,
        kind=kind,
        low_idx=low_idx,
        high_idx=high_idx,
        change=change,
        derived_only=is_derived_only(kind, low.params, high.params, change),
        opened_by_idx=principal_idx,
    )


async def _locked_live_pair(conn: asyncpg.Connection, definition_pair_idx: int) -> PairState:
    """Take the group lock EXCLUSIVE (raising GroupLockBusy), then the pair row; refuse
    a detached pair."""
    await lock_groups_exclusive(conn)
    row = await fetch_pair(conn, definition_pair_idx, for_update=True)
    if row is None:
        raise PairNotFound(definition_pair_idx)
    if row["detached_at"] is not None:
        raise PairRefused(
            DefinitionPairRefusal.DETACHED,
            f"pair {definition_pair_idx} is detached: one of its definitions was deleted",
            definition_pair_idx=[definition_pair_idx],
        )
    return pair_state(row)


async def decide_pair(
    conn: asyncpg.Connection,
    *,
    definition_pair_idx: int,
    verdict: DefinitionPairVerdict,
    reason: str,
    principal_idx: int,
) -> asyncpg.Record:
    """Record a decision on a live pair, or raise PairRefused / PairNotFound /
    GroupLockBusy. Returns the updated pair row."""
    require_transaction(conn)
    pair = await _locked_live_pair(conn, definition_pair_idx)
    check_decision(await load_groups(conn), pair, verdict)
    return await record_decision(
        conn,
        definition_pair_idx=definition_pair_idx,
        verdict=verdict,
        reason=reason,
        principal_idx=principal_idx,
    )


async def revoke_pair(
    conn: asyncpg.Connection, *, definition_pair_idx: int, reason: str, principal_idx: int
) -> asyncpg.Record:
    """Return a decided live pair to pending, or raise PairRefused / PairNotFound /
    GroupLockBusy. Returns the updated pair row."""
    require_transaction(conn)
    pair = await _locked_live_pair(conn, definition_pair_idx)
    check_revoke(await load_groups(conn), pair)
    return await record_revoke(
        conn, definition_pair_idx=definition_pair_idx, reason=reason, principal_idx=principal_idx
    )


async def refuse_deprecating_with_similar_pairs(
    conn: asyncpg.Connection, kind: DefinitionKind, idx: int
) -> None:
    """Take the group lock SHARED, then raise PairRefused (`similar_pairs`) if a live
    `similar` pair names `idx`. A definition grouped only by derivation is not
    refused. Call before taking the definition's row lock."""
    require_transaction(conn)
    await lock_groups_shared(conn)
    pairs = await fetch_similar_pairs_naming(conn, kind, idx)
    if pairs:
        raise PairRefused(
            DefinitionPairRefusal.SIMILAR_PAIRS,
            f"{kind.value} {idx} is a side of similar pair(s) {pairs}; revoke them or"
            " re-decide them not_similar before deprecating it",
            definition_pair_idx=pairs,
        )
