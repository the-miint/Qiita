"""`DecisionSetToken`'s pattern: the form `qiita_control_plane.decision_set` writes."""

import pytest
from pydantic import TypeAdapter, ValidationError

from qiita_common.models import DecisionSetToken

_TOKEN = TypeAdapter(DecisionSetToken)


def test_a_sha256_token_of_64_lowercase_hex_digits_is_accepted():
    token = "sha256:" + "0123456789abcdef" * 4
    assert _TOKEN.validate_python(token) == token


@pytest.mark.parametrize(
    "token",
    [
        "md5:" + "0" * 64,
        "sha256" + "0" * 64,
        "sha256:" + "0" * 63,
        "sha256:" + "0" * 65,
        "sha256:" + "A" * 64,
        "sha256:" + "g" * 64,
        " sha256:" + "0" * 64,
    ],
)
def test_any_other_form_is_refused(token):
    with pytest.raises(ValidationError):
        _TOKEN.validate_python(token)
