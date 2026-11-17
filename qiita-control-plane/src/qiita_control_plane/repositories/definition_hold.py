"""Minting a definition and holding the minted row for the caller's transaction.

`runner._record` states when a ticket's definition is held from its mint through the
write that records it, and why; `align_planner` holds its alignment the same way."""

from collections.abc import Awaitable, Callable

import asyncpg

from . import require_transaction

# A settings edit that commits between a mint and its hold re-keys the row the mint
# resolved; the next mint then resolves (or inserts) the row for these params. One
# retry covers one such edit; a second in the same window raises MintHoldExhausted.
MINT_HOLD_ATTEMPTS = 2


class MintHoldExhausted(Exception):
    """The minted row's settings were edited between the mint and its hold on every
    attempt. A runner fails the ticket as bad submission input (`runner._record`); the
    align planner refuses the plan with a 409 (`AlignPlanDefinitionEdited`)."""

    def __init__(self, what: str):
        self.what = what
        super().__init__(
            f"{what}: the minted row's settings were edited between the mint and its hold"
            f" on each of {MINT_HOLD_ATTEMPTS} attempts"
        )


async def mint_and_hold(
    conn: asyncpg.Connection,
    *,
    mint: Callable[[], Awaitable[asyncpg.Record]],
    held_params_hash: Callable[[asyncpg.Record], Awaitable[bytes]],
    params_hash: bytes,
    what: str,
) -> asyncpg.Record:
    """`mint()`, then `held_params_hash(row)`, which holds the minted row `FOR SHARE`
    for the caller's transaction and returns its `params_hash`; re-mint when that is
    no longer `params_hash`: a settings edit committed between the mint and the hold.
    Requires an open transaction: outside one the `FOR SHARE` ends with its
    statement. `what` names the definition in `MintHoldExhausted`."""
    require_transaction(conn)
    for _ in range(MINT_HOLD_ATTEMPTS):
        row = await mint()
        if await held_params_hash(row) == params_hash:
            return row
    raise MintHoldExhausted(what)
