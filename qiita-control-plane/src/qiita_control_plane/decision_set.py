"""The decision set: every decided pair's verdict at one point of the pair history,
named by a content hash so that a published manifest can record which decisions it
was built under without carrying a pair or event idx, and a caller can rebuild under
the same decisions later (`as_of_event`).

**Canonical form.** The pairs whose verdict is not `pending`, each as the JSON array
`[definition_kind, low_definition_ref, high_definition_ref, verdict]`, sorted, then
serialized as compact JSON (`separators=(",", ":")`, no whitespace) and hashed with
SHA-256; the token is `sha256:` and the lowercase hex digest. No decision at all hashes
`[]`. This is the one copy of the form; every other site points here.

**History.** `qiita.definition_pair_event` is append-only and each row carries the
pair's verdict after it, so the state after event E is, per pair, the verdict of its
newest event at or before E. Every pair is replayed, detached ones included, so the
token of a state does not change when a definition is deleted later. The current
state is the state after the newest event.

**As of a token.** The newest event whose state hashes to the token. States repeat (a
decide then a revoke returns to the earlier state), and any event with that state
gives the same verdicts, so the newest is as good as any. A token no state hashes to
is refused (`DecisionSetUnknown`); a state not yet reached is one of them. The `[]`
state is accepted: it is the state before any decision. Only verdicts are taken from
the history: whether a pair is detached, and every definition's params and status,
are read as they are now.

**Pairs used.** The pairs that shaped the groups the resolution read: `similar` in
the decision set, not detached now, and naming a definition the resolution read
(`definition_resolver.needed_definitions`; both sides of such a pair are in one group
read). `not_similar` and `pending` pairs join nothing and are not counted. Hashed in
the same canonical form; empty for an exact read, which forms no group.
"""

import hashlib
import json
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime

import asyncpg
from qiita_common.models import DecisionProvenance, DefinitionKind, DefinitionPairVerdict

__all__ = [
    "DecisionSet",
    "DecisionSetUnknown",
    "decision_set_token",
    "load_decision_set",
]


class DecisionSetUnknown(Exception):
    """No state of the pair history hashes to the token."""

    def __init__(self, token: str) -> None:
        super().__init__(
            f"as_of_event {token} names no decision set of the pair history: it was not"
            " recorded by this server, or names a state not reached"
        )
        self.token = token


@dataclass(frozen=True)
class _Pair:
    kind: DefinitionKind
    low: int
    high: int
    detached: bool


def decision_set_token(decided: Iterable[tuple[str, int, int, str]]) -> str:
    """The token of `decided`'s `[kind, low, high, verdict]` entries, pending ones
    excluded (module docstring: canonical form)."""
    entries = sorted(
        [kind, low, high, verdict]
        for kind, low, high, verdict in decided
        if verdict != DefinitionPairVerdict.PENDING.value
    )
    body = json.dumps(entries, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(body.encode()).hexdigest()


@dataclass(frozen=True)
class DecisionSet:
    """The verdicts a resolution is computed under. `verdicts` maps every pair the
    history names to its verdict at that point; a pair opened later is absent and
    reads as `pending`."""

    token: str
    newest_event_at: datetime | None
    as_of: bool
    verdicts: Mapping[int, DefinitionPairVerdict]
    _pairs: Mapping[int, _Pair]

    def verdict_of(self, pair_idx: int) -> DefinitionPairVerdict:
        return self.verdicts.get(pair_idx, DefinitionPairVerdict.PENDING)

    def provenance(
        self, read: Mapping[DefinitionKind, Collection[int]] | None
    ) -> DecisionProvenance:
        """The wire record. `read` is the definitions the resolution read by kind, or
        None for an exact read; the pairs used are as the module docstring says."""
        used = [
            (pair.kind.value, pair.low, pair.high, self.verdict_of(idx).value)
            for idx, pair in self._pairs.items()
            if read is not None
            and self.verdict_of(idx) is DefinitionPairVerdict.SIMILAR
            and not pair.detached
            and (pair.low in read.get(pair.kind, ()) or pair.high in read.get(pair.kind, ()))
        ]
        return DecisionProvenance(
            decision_set=self.token,
            newest_event_at=self.newest_event_at,
            as_of=self.as_of,
            pairs_used=len(used),
            pairs_used_set=decision_set_token(used),
        )


async def load_decision_set(conn: asyncpg.Connection, as_of_event: str | None) -> DecisionSet:
    """The current decision set, or the one `as_of_event` names, replayed from the pair
    history in the caller's transaction. Raises DecisionSetUnknown for a token no
    state hashes to."""
    rows = await conn.fetch(
        "SELECT e.definition_pair_idx, e.verdict, e.occurred_at,"
        "       p.definition_kind, p.low_definition_ref, p.high_definition_ref,"
        "       p.detached_at IS NOT NULL AS detached"
        "  FROM qiita.definition_pair_event e"
        "  JOIN qiita.definition_pair p USING (definition_pair_idx)"
        " ORDER BY e.definition_pair_event_idx"
    )
    pairs: dict[int, _Pair] = {}
    for r in rows:
        pairs.setdefault(
            r["definition_pair_idx"],
            _Pair(
                DefinitionKind(r["definition_kind"]),
                r["low_definition_ref"],
                r["high_definition_ref"],
                r["detached"],
            ),
        )

    def replay(through: int) -> dict[int, DefinitionPairVerdict]:
        """The verdicts after the first `through` events."""
        return {
            r["definition_pair_idx"]: DefinitionPairVerdict(r["verdict"]) for r in rows[:through]
        }

    def token_of(verdicts: Mapping[int, DefinitionPairVerdict]) -> str:
        return decision_set_token(
            (pairs[i].kind.value, pairs[i].low, pairs[i].high, v.value) for i, v in verdicts.items()
        )

    def at(through: int) -> datetime | None:
        return rows[through - 1]["occurred_at"] if through else None

    if as_of_event is None:
        verdicts = replay(len(rows))
        return DecisionSet(token_of(verdicts), at(len(rows)), False, verdicts, pairs)
    verdicts: dict[int, DefinitionPairVerdict] = {}
    tokens = [token_of(verdicts)]
    for r in rows:
        verdicts[r["definition_pair_idx"]] = DefinitionPairVerdict(r["verdict"])
        tokens.append(token_of(verdicts))
    for through in range(len(rows), -1, -1):
        if tokens[through] == as_of_event:
            return DecisionSet(as_of_event, at(through), True, replay(through), pairs)
    raise DecisionSetUnknown(as_of_event)
