"""Minting a definition and holding the minted row for the caller's transaction.

The rule, stated here once: a mask, an alignment or a processing minted for a ticket
is held `FOR SHARE` from the mint through the write that records it on the ticket
(`work_ticket.mask_idx`, `alignment_idx`, `processing_idx`), re-minted when a
settings edit moved it before the hold (`mint_and_hold`). A settings edit holds the
row `FOR UPDATE` while it checks for unfinished tickets (`definition_settings`), so
the edit and the mint-and-record cannot both commit unseen by the other, and a
ticket is never recorded under settings it did not hash. The per-sample read-mask,
de novo alignment and processing mints record on the ticket in the hold's
transaction (`runner._record.record_minted`); the align-block plan mints and inserts
its tickets in one transaction. The block-mask plan is the exception: it mints
before its plan transaction and, under the hold, refuses the plan when a minted
mask's settings moved (`block_planner.BlockPlanMaskEdited`) rather than re-minting,
since a re-mint would re-partition the plan mid-transaction.

When the row is edited before every hold (`MintHoldExhausted`), a runner fails the
ticket as bad submission input, as its other pre-loop refusals do, and the align
planner refuses the plan with a 409; a redrive or a new plan mints again."""

from collections.abc import Awaitable, Callable

import asyncpg

from . import require_transaction

# A settings edit that commits between a mint and its hold re-keys the row the mint
# resolved; the next mint then resolves (or inserts) the row for these params. One
# retry covers one such edit; a second in the same window raises MintHoldExhausted.
MINT_HOLD_ATTEMPTS = 2


class MintHoldExhausted(Exception):
    """The minted row's settings were edited between the mint and its hold on every
    attempt (the module docstring says what each caller does with it)."""

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
