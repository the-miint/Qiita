"""The decision set: every decided output equivalence's verdict at one point of the
output-equivalence history, named by a content hash so that a published manifest can
record which decisions it was built under without carrying an output-equivalence or
event idx, and a caller can rebuild under the same decisions later (`as_of_event`).

**Canonical form.** The output equivalences whose verdict is not `pending`, each as the
JSON array `[definition_kind, low_definition_ref, high_definition_ref, verdict]`,
sorted, then serialized as compact JSON (`separators=(",", ":")`, no whitespace) and
hashed with SHA-256; the token is `sha256:` and the lowercase hex digest. No decision at
all hashes `[]`. The verdict is its stored value, so a `same` and a `similar` decision
on the same two definitions give different tokens, though both join. This is the one
copy of the form; every other site points here.

**History.** `qiita.output_equivalence_event` is append-only and each row carries the
output equivalence's verdict after it, so the state after event E is, per output
equivalence, the verdict of its newest event at or before E. Every output equivalence is
replayed, detached ones included, so the token of a state does not change when a
definition is deleted later. The current state is the state after the newest event.

**As of a token.** The newest event whose state hashes to the token. States repeat (a
decide then a revoke returns to the earlier state), and any event with that state
gives the same verdicts, so the newest is as good as any. A token no state hashes to
is refused (`DecisionSetUnknown`); a state not yet reached is one of them. The `[]`
state is accepted: it is the state before any decision. Only verdicts are taken from
the history: whether an output equivalence is detached, and every definition's params
and status, are read as they are now.

**Equivalences used.** The output equivalences that shaped the groups the resolution
read: a joining verdict (`JOINING_VERDICTS`) in the decision set, not detached now, and
naming a definition the resolution read (`definition_resolver.needed_definitions`; both
sides of such an output equivalence are in one group read). Non-joining and `pending`
ones join nothing and are not counted. Hashed in the same canonical form; empty for an
exact read, which forms no group.
"""

import hashlib
import json
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime

import asyncpg
from qiita_common.models import (
    JOINING_VERDICTS,
    DecisionProvenance,
    DefinitionKind,
    OutputEquivalenceVerdict,
)

__all__ = [
    "DecisionSet",
    "DecisionSetUnknown",
    "decision_set_token",
    "load_decision_set",
]


class DecisionSetUnknown(Exception):
    """No state of the output-equivalence history hashes to the token."""

    def __init__(self, token: str) -> None:
        super().__init__(
            f"as_of_event {token} names no decision set of the output-equivalence history: it"
            " was not recorded by this server, or names a state not reached"
        )
        self.token = token


@dataclass(frozen=True)
class _Equivalence:
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
        if verdict != OutputEquivalenceVerdict.PENDING.value
    )
    body = json.dumps(entries, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(body.encode()).hexdigest()


@dataclass(frozen=True)
class DecisionSet:
    """The verdicts a resolution is computed under. `verdicts` maps every equivalence the
    history names to its verdict at that point; an equivalence opened later is absent and
    reads as `pending`."""

    token: str
    newest_event_at: datetime | None
    as_of: bool
    verdicts: Mapping[int, OutputEquivalenceVerdict]
    _equivalences: Mapping[int, _Equivalence]

    def verdict_of(self, equivalence_idx: int) -> OutputEquivalenceVerdict:
        return self.verdicts.get(equivalence_idx, OutputEquivalenceVerdict.PENDING)

    def provenance(
        self, read: Mapping[DefinitionKind, Collection[int]] | None
    ) -> DecisionProvenance:
        """The wire record. `read` is the definitions the resolution read by kind, or
        None for an exact read; the equivalences used are as the module docstring says."""
        used = [
            (equivalence.kind.value, equivalence.low, equivalence.high, self.verdict_of(idx).value)
            for idx, equivalence in self._equivalences.items()
            if read is not None
            and self.verdict_of(idx) in JOINING_VERDICTS
            and not equivalence.detached
            and (
                equivalence.low in read.get(equivalence.kind, ())
                or equivalence.high in read.get(equivalence.kind, ())
            )
        ]
        return DecisionProvenance(
            decision_set=self.token,
            newest_event_at=self.newest_event_at,
            as_of=self.as_of,
            equivalences_used=len(used),
            equivalences_used_set=decision_set_token(used),
        )


async def load_decision_set(conn: asyncpg.Connection, as_of_event: str | None) -> DecisionSet:
    """The current decision set, or the one `as_of_event` names, replayed from the equivalence
    history in the caller's transaction. Raises DecisionSetUnknown for a token no
    state hashes to."""
    rows = await conn.fetch(
        "SELECT e.output_equivalence_idx, e.verdict, e.occurred_at,"
        "       o.definition_kind, o.low_definition_ref, o.high_definition_ref,"
        "       o.detached_at IS NOT NULL AS detached"
        "  FROM qiita.output_equivalence_event e"
        "  JOIN qiita.output_equivalence o USING (output_equivalence_idx)"
        " ORDER BY e.output_equivalence_event_idx"
    )
    equivalences: dict[int, _Equivalence] = {}
    for r in rows:
        equivalences.setdefault(
            r["output_equivalence_idx"],
            _Equivalence(
                DefinitionKind(r["definition_kind"]),
                r["low_definition_ref"],
                r["high_definition_ref"],
                r["detached"],
            ),
        )

    def replay(through: int) -> dict[int, OutputEquivalenceVerdict]:
        """The verdicts after the first `through` events."""
        return {
            r["output_equivalence_idx"]: OutputEquivalenceVerdict(r["verdict"])
            for r in rows[:through]
        }

    def token_of(verdicts: Mapping[int, OutputEquivalenceVerdict]) -> str:
        return decision_set_token(
            (equivalences[i].kind.value, equivalences[i].low, equivalences[i].high, v.value)
            for i, v in verdicts.items()
        )

    def at(through: int) -> datetime | None:
        return rows[through - 1]["occurred_at"] if through else None

    if as_of_event is None:
        verdicts = replay(len(rows))
        return DecisionSet(token_of(verdicts), at(len(rows)), False, verdicts, equivalences)
    verdicts: dict[int, OutputEquivalenceVerdict] = {}
    tokens = [token_of(verdicts)]
    for r in rows:
        verdicts[r["output_equivalence_idx"]] = OutputEquivalenceVerdict(r["verdict"])
        tokens.append(token_of(verdicts))
    for through in range(len(rows), -1, -1):
        if tokens[through] == as_of_event:
            return DecisionSet(as_of_event, at(through), True, replay(through), equivalences)
    raise DecisionSetUnknown(as_of_event)
