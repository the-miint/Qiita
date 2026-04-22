import asyncpg
import qiita_common


def test_health_endpoint_importable():
    """The app module and health route are importable."""
    from qiita_control_plane.main import app, health

    assert app is not None
    assert callable(health)


def test_dependencies_importable():
    assert asyncpg is not None
    assert qiita_common is not None
