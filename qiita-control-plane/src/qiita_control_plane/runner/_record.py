"""Minting a definition and recording it on its ticket, in one transaction.

A runner that mints a mask, an alignment or a processing for a ticket holds the
minted row `FOR SHARE` from the mint through the write of the ticket's column
(`work_ticket.mask_idx`, `alignment_idx`, `processing_idx`). A settings edit holds
the row `FOR UPDATE` while it checks for unfinished tickets (`definition_settings`),
so the edit and the mint-and-record cannot both commit unseen by the other, and a
ticket is never recorded under settings it did not hash.

A redrive keeps the ticket's step rows and gate row, keyed on the definition it
recorded on its first attempt. When the mint now resolves to another row, the
ticket fails as bad submission input rather than being re-recorded, whether or not
it kept step rows, so no ticket names two definitions of one kind. Two causes reach
it: the recorded definition's settings were edited (an edit re-keys the row in
place, so the ticket's params hash to a row it is not), or the ticket's params now
build differently (a re-synced action default filling an omitted knob). A new
ticket is recorded under the definition minted for its own params; the edited row
keeps the edited settings. The recorded value is read without a row lock: the
dispatch state guard keeps one ticket from running twice at once (`dispatch.py`).
"""

from collections.abc import Awaitable, Callable

import asyncpg

from ..repositories import require_transaction
from ._upload import _submission_bad_input

# A settings edit that commits between a mint and its hold re-keys the row the mint
# resolved; the next mint then resolves (or inserts) the row for these params. One
# retry covers one such edit; a second in the same window raises RuntimeError, which
# fails the ticket.
MINT_HOLD_ATTEMPTS = 2

# The ticket columns a runner records a minted definition in, with the noun a
# refusal names it by. Interpolated into SQL, so the set is closed.
_RECORDED_COLUMNS = {
    "mask_idx": "mask",
    "alignment_idx": "alignment definition",
    "processing_idx": "processing",
}


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
    statement. `what` names the definition in the error."""
    require_transaction(conn)
    for _ in range(MINT_HOLD_ATTEMPTS):
        row = await mint()
        if await held_params_hash(row) == params_hash:
            return row
    raise RuntimeError(
        f"{what}: the minted row's settings were edited between the mint and its hold"
        f" on each of {MINT_HOLD_ATTEMPTS} attempts"
    )


async def record_minted(
    conn: asyncpg.Connection, *, work_ticket_idx: int, column: str, definition_idx: int
) -> None:
    """Write `definition_idx` into the ticket's `column`, refusing (bad submission
    input) a ticket that already records another one (the module docstring says
    when). Run in the transaction that holds the minted row."""
    noun = _RECORDED_COLUMNS.get(column)
    if noun is None:
        raise ValueError(f"not a recorded definition column: {column!r}")
    require_transaction(conn)
    recorded = await conn.fetchval(
        f"SELECT {column} FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if recorded is not None and recorded != definition_idx:
        kept = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM qiita.work_ticket_step WHERE work_ticket_idx = $1)",
            work_ticket_idx,
        )
        held = (
            "Its kept step rows are keyed on the first"
            if kept
            else "It records the first and is not re-recorded under another"
        )
        raise _submission_bad_input(
            f"work ticket {work_ticket_idx} first ran under {noun} {recorded}; its params"
            f" now resolve to {noun} {definition_idx} (the first's settings were edited,"
            f" or the params now build differently). {held}, so it cannot resume; submit"
            f" a new ticket, which is recorded under the {noun} minted for its own params"
        )
    await conn.execute(
        f"UPDATE qiita.work_ticket SET {column} = $1 WHERE work_ticket_idx = $2",
        definition_idx,
        work_ticket_idx,
    )
