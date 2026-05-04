"""Tests for control plane database pool."""

import pytest


def test_get_pool_importable():
    """get_pool must be importable from qiita_control_plane.db."""
    from qiita_control_plane.db import get_pool

    assert callable(get_pool)


def test_close_pool_importable():
    """close_pool must be importable from qiita_control_plane.db."""
    from qiita_control_plane.db import close_pool

    assert callable(close_pool)


async def test_get_pool_rejects_invalid_url():
    """get_pool must raise on an unreachable database URL, not hang."""
    from qiita_control_plane.db import get_pool

    with pytest.raises(Exception):
        await get_pool("postgresql://nobody:bad@localhost:1/nonexistent", connect_timeout=1)


@pytest.mark.db
async def test_select_one(postgres_pool):
    """Pool can execute SELECT 1 against the test Postgres."""
    result = await postgres_pool.fetchval("SELECT 1")
    assert result == 1


@pytest.mark.db
async def test_schema_exists_after_migration(postgres_pool):
    """The qiita schema must exist after migrations run."""
    result = await postgres_pool.fetchval(
        "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'qiita')"
    )
    assert result is True
