import qiita_common
import qiita_common.client
import qiita_common.config
import qiita_common.models
from pydantic import BaseModel


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
