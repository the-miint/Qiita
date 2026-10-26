import duckdb
import pytest

from qiita_control_plane.ena_import import miint_resolver


def _http_error(status: int) -> duckdb.HTTPException:
    error = duckdb.HTTPException(f"HTTP {status} from ENA")
    error.status_code = status
    return error


@pytest.fixture
def no_backoff(monkeypatch):
    monkeypatch.setattr(miint_resolver, "_RESOLVE_BACKOFF_BASE_S", 0)
