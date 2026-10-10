"""`insert_new_entities_metadata_batch`: which database errors fall back to the
per-value path and which re-raise. The write itself is covered end to end by
the bulk-import route tests in tests/routes/test_biosample.py."""

import contextlib

import asyncpg
import pytest
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories._sample_helpers import (
    ResolvedField,
    insert_new_entities_metadata_batch,
)
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC


class _Conn:
    """Just enough of an asyncpg connection: in a transaction, savepoints that
    succeed, and an executemany that raises the given error."""

    def __init__(self, error: Exception):
        self.error = error
        self.savepoints = 0

    def is_in_transaction(self) -> bool:
        return True

    @contextlib.asynccontextmanager
    async def transaction(self):
        self.savepoints += 1
        yield

    async def executemany(self, *args, **kwargs):
        raise self.error


def _local_entry() -> list:
    field = ResolvedField(
        caller_key="depth",
        global_field_idx=None,
        study_field_idx=11,
        canonical_display="depth",
        data_type=FieldDataType.TEXT,
        internal_name=None,
        terminology_idx=None,
    )
    return [(1, [(field, "5 m")])]


async def _write(conn) -> bool:
    return await insert_new_entities_metadata_batch(
        conn, spec=BIOSAMPLE_METADATA_SPEC, study_idx=3, caller_idx=4, entries=_local_entry()
    )


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.UniqueViolationError("dup"),
        asyncpg.CheckViolationError("check"),
        asyncpg.ForeignKeyViolationError("fk"),
        asyncpg.RaiseError("trigger refused"),
    ],
)
async def test_a_rejected_batch_returns_false_for_the_per_value_fallback(error):
    conn = _Conn(error)
    assert await _write(conn) is False
    assert conn.savepoints == 1


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.DeadlockDetectedError("deadlock"),
        asyncpg.SerializationError("serialization"),
        asyncpg.QueryCanceledError("cancelled"),
    ],
)
async def test_a_transient_error_reraises_rather_than_replaying_slowly(error):
    with pytest.raises(type(error)):
        await _write(_Conn(error))


async def test_refuses_to_run_outside_a_transaction():
    conn = _Conn(asyncpg.UniqueViolationError("unused"))
    conn.is_in_transaction = lambda: False
    with pytest.raises(RuntimeError, match="outside a transaction"):
        await _write(conn)
