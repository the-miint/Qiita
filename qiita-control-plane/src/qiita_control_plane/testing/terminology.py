"""Pytest fixtures for tests that create terminology rows."""

import pytest_asyncio

from qiita_control_plane.testing.db_seeds import delete_terminology_cascade


@pytest_asyncio.fixture
async def created_terminologies(postgres_pool):
    """Yields a list the test appends terminology idxs to; teardown removes
    each one along with its term and closure rows."""
    created: list[int] = []
    yield created
    for terminology_idx in created:
        await delete_terminology_cascade(postgres_pool, terminology_idx)
