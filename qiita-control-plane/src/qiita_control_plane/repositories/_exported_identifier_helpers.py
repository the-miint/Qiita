"""Shared pieces of the exported-handle mints."""

from collections.abc import Mapping, Sequence
from typing import Any


class IncompleteMintError(RuntimeError):
    """Fewer entities came back from a mint than were asked for.

    Carries the missing identifiers and the kind they belong to.
    """

    def __init__(self, missing: list[int], *, kind: str) -> None:
        self.missing = missing
        self.kind = kind
        super().__init__(
            f"{len(missing)} {kind}(s) have no exported handle after minting: {missing}"
        )


def missing_from(
    rows: Sequence[Mapping[str, Any]], requested: Sequence[int], *, key: str
) -> list[int]:
    """Requested identifiers that no row carries under `key`, ascending.

    `key` names the field holding the identifier. A row whose `key` is NULL matches
    nothing, so a result set spanning several kinds is checked one kind at a time.
    """
    returned = {row[key] for row in rows}
    gap = sorted(idx for idx in set(requested) if idx not in returned)
    return gap
