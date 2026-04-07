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
