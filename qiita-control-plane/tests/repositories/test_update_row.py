"""Tests for the shared UPDATE composer in the repositories package init."""

from typing import get_args

import pytest

from qiita_control_plane.repositories import UpdatableTable, update_row


async def test_update_row_rejects_table_outside_the_allowlist():
    """Tests the case where a table name outside UpdatableTable reaches the
    composer: it is refused before any SQL is built, since the table name is
    interpolated and a Literal annotation does not bind at runtime.
    """
    with pytest.raises(ValueError) as excinfo:
        await update_row(
            None,
            table="qiita.principal; DROP TABLE qiita.study",
            row_idx=1,
            fields={"title": "x"},
            allowlist=frozenset({"title"}),
            returning_cols="idx",
            repo_name="test_update_row",
        )

    assert "non-updatable table" in str(excinfo.value)


async def test_update_row_rejects_the_unqualified_spelling():
    """Tests the case where a caller passes a table without its schema: the
    allowlist holds qualified names only, so the bare spelling is refused
    rather than silently composing SQL against a search-path-dependent table.
    """
    with pytest.raises(ValueError):
        await update_row(
            None,
            table="study",
            row_idx=1,
            fields={"title": "x"},
            allowlist=frozenset({"title"}),
            returning_cols="idx",
            repo_name="test_update_row",
        )


def test_update_row_allowlist_is_schema_qualified():
    """Tests the case where a member is added to UpdatableTable without its
    schema: every entry must carry one, because the composer interpolates the
    value verbatim into the UPDATE.
    """
    assert all(table.startswith("qiita.") for table in get_args(UpdatableTable))
