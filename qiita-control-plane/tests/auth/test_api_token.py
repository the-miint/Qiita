"""Pure unit tests for opaque API token format / hashing / scope validation.

DB-required tests live in qiita-control-plane/tests/auth/test_api_token_db.py.
"""

import hashlib

import pytest
from qiita_common.auth_constants import Scope


def test_token_constants_match_spec():
    from qiita_control_plane.auth import (
        TOKEN_BODY_BYTES,
        TOKEN_BODY_LEN,
        TOKEN_HASH_BYTES,
        TOKEN_PREFIX,
        TOKEN_TOTAL_LEN,
    )

    assert TOKEN_PREFIX == "qk_"
    assert TOKEN_BODY_BYTES == 32
    assert TOKEN_BODY_LEN == 43
    assert TOKEN_TOTAL_LEN == 46
    assert TOKEN_HASH_BYTES == 32


def test_generate_token_starts_with_qk_prefix():
    from qiita_control_plane.auth.token import _generate_token

    plaintext, _ = _generate_token()
    assert plaintext.startswith("qk_")


def test_generate_token_total_length():
    from qiita_control_plane.auth.token import _generate_token

    plaintext, _ = _generate_token()
    assert len(plaintext) == 46


def test_generate_token_returns_plaintext_and_hash_pair():
    from qiita_control_plane.auth.token import _generate_token

    plaintext, digest = _generate_token()
    assert isinstance(plaintext, str)
    assert isinstance(digest, bytes)
    assert len(digest) == 32


def test_generate_token_hash_is_sha256_of_plaintext():
    from qiita_control_plane.auth.token import _generate_token

    plaintext, digest = _generate_token()
    assert digest == hashlib.sha256(plaintext.encode("ascii")).digest()


def test_generate_token_is_random():
    from qiita_control_plane.auth.token import _generate_token

    a, _ = _generate_token()
    b, _ = _generate_token()
    assert a != b


def test_valid_scopes_is_frozen():
    from qiita_control_plane.auth.scopes import VALID_SCOPES

    assert isinstance(VALID_SCOPES, frozenset)
    # Spot-check a few — full coverage lives in the role/scope tests.
    assert Scope.REFERENCE_READ in VALID_SCOPES
    assert Scope.ADMIN_USER in VALID_SCOPES
    assert Scope.SELF_PROFILE in VALID_SCOPES


def test_verified_token_dataclass_is_frozen():
    from qiita_control_plane.auth.token import VerifiedToken

    vt = VerifiedToken(principal_idx=42, token_idx=7, scopes=frozenset({"a"}))
    with pytest.raises((AttributeError, Exception)):
        vt.principal_idx = 99  # type: ignore[misc]


def test_module_exports():
    """Public surface must be importable from auth.tokens."""
    from qiita_control_plane.auth.token import (
        VerifiedToken,
        mint_api_token,
        record_token_use,
        verify_api_token,
    )

    assert callable(mint_api_token)
    assert callable(verify_api_token)
    assert callable(record_token_use)
    assert VerifiedToken is not None
