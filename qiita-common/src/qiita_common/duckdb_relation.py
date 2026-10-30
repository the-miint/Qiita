"""An Arrow object registered on a DuckDB connection for the length of a block.

DuckDB 2.0 refuses `register`, `unregister` of a registered name, and running a
relation — fetching from one `sql`, `from_arrow` or `table` built, opening an Arrow
reader on one (`to_arrow_reader`), or a `sql` call that is not a SELECT and so runs at
once — on a connection whose last `execute`d SELECT is part-read, or that an open Arrow
reader of its own is still streaming, with `ResourceInUseException`. Building such a
relation is accepted. The two cases differ on a plain `execute`: it discards a
part-read result, but is refused while an Arrow reader is open. `fetchone()` leaves
even a one-row result part-read until a fetch comes back empty; `fetchall()` does not.
Measured on 2.0.0, a part-read `DESCRIBE` or `SHOW` is refused like a SELECT, and
nothing is refused after `SET`, `CREATE`, `INSERT` (with or without `RETURNING`),
`COPY`, `CALL` or `PRAGMA`, read or not.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


@contextmanager
def registered(con: duckdb.DuckDBPyConnection, relation: str, obj: object) -> Iterator[str]:
    """Register `obj` — an Arrow table or a stream reader — on `con` as `relation` for
    the block's duration, and release it however the block ends.

    The release cancels (`con.abort()`) a SELECT the block left part-read before it
    unregisters, because the unregister would otherwise be refused (see the module
    docstring): a block that succeeded would fail, and one that raised would have its
    exception replaced. Nothing started before the block can be cancelled — the
    `register` would have been refused — but a result the block starts is, so read it
    inside the block. `con.abort()` does not end an Arrow reader, so a reader the block
    opens on `con` must be drained or closed inside it: one left open makes the
    unregister raise `ResourceInUseException` in place of the block's own outcome.
    """
    con.register(relation, obj)
    try:
        yield relation
    finally:
        con.abort()
        con.unregister(relation)
