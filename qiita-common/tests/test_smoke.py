import pytest
from pydantic import BaseModel

import qiita_common
import qiita_common.client
import qiita_common.config
import qiita_common.models
from qiita_common.config import require_env


def test_import():
    assert qiita_common is not None


def test_submodules_importable():
    assert qiita_common.models is not None
    assert qiita_common.config is not None
    assert qiita_common.client is not None


def test_pydantic_available():
    class _M(BaseModel):
        x: int

    assert _M(x=1).x == 1


def test_require_env_returns_value(monkeypatch):
    monkeypatch.setenv("TEST_VAR", "hello")
    assert require_env("TEST_VAR") == "hello"


def test_require_env_raises_on_missing(monkeypatch):
    monkeypatch.delenv("TEST_VAR", raising=False)
    with pytest.raises(RuntimeError, match="not set"):
        require_env("TEST_VAR")


def test_require_env_raises_on_empty(monkeypatch):
    monkeypatch.setenv("TEST_VAR", "")
    with pytest.raises(RuntimeError, match="set but empty"):
        require_env("TEST_VAR")
