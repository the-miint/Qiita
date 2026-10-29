"""An Arrow object registered on a DuckDB connection for the length of a block.

DuckDB 2.0 refuses a relational call — `register`, `unregister`, `sql`, `from_arrow` —
on a connection whose last `execute`d SELECT is part-read, with
`ResourceInUseException`. `fetchone()` leaves even a one-row result part-read until a
fetch comes back empty; `fetchall()` does not. A plain `execute` is not refused (it
discards the part-read result), and neither is a statement that returns no rows to
read (`SET`, `CREATE`, `INSERT`, `COPY`). DuckDB 1.5.5 refused none of this.
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
    inside the block.
    """
    con.register(relation, obj)
    try:
        yield relation
    finally:
        con.abort()
        con.unregister(relation)
