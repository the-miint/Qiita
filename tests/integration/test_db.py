"""RED: Integration test for database connectivity — should fail until db.py is implemented."""

import pytest


@pytest.mark.asyncio
async def test_select_one(postgres_pool):
    """Pool can execute SELECT 1 against the test Postgres."""
    result = await postgres_pool.fetchval("SELECT 1")
    assert result == 1


@pytest.mark.asyncio
async def test_schema_exists_after_migration(postgres_pool):
    """The qiita schema must exist after migrations run."""
    result = await postgres_pool.fetchval(
        "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'qiita')"
    )
    assert result is True
