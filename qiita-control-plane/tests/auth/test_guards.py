"""Unit tests for guards. Guards are FastAPI deps; we exercise them by
synthesising the dep input directly (no FastAPI request needed)."""

import pytest
from fastapi import HTTPException
from qiita_common.auth_constants import Scope, SystemRole


def _human(*, role=SystemRole.USER, scopes=frozenset(), profile_complete=True):
    from qiita_control_plane.auth.principal import HumanUser

    return HumanUser(
        principal_idx=2,
        email="alice@example.com",
        system_role=role,
        scopes=scopes,
        profile_complete=profile_complete,
        disabled=False,
        retired=False,
    )


def _service(scopes=frozenset()):
    from qiita_control_plane.auth.principal import ServiceAccount

    return ServiceAccount(
        principal_idx=3,
        name="orchestrator",
        scopes=scopes,
        disabled=False,
        retired=False,
    )


def _anon():
    from qiita_control_plane.auth.principal import Anonymous

    return Anonymous()


# ---------------------------------------------------------------------------
# require_human / require_service
# ---------------------------------------------------------------------------


def test_require_human_returns_human():
    from qiita_control_plane.auth.guards import require_human

    h = _human()
    assert require_human(h) is h


def test_require_human_401_on_anonymous():
    from qiita_control_plane.auth.guards import require_human

    with pytest.raises(HTTPException) as exc:
        require_human(_anon())
    assert exc.value.status_code == 401


def test_require_human_403_on_service_account():
    from qiita_control_plane.auth.guards import require_human

    with pytest.raises(HTTPException) as exc:
        require_human(_service())
    assert exc.value.status_code == 403


def test_require_service_returns_service():
    from qiita_control_plane.auth.guards import require_service

    s = _service()
    assert require_service(s) is s


def test_require_service_401_on_anonymous():
    from qiita_control_plane.auth.guards import require_service

    with pytest.raises(HTTPException) as exc:
        require_service(_anon())
    assert exc.value.status_code == 401


def test_require_service_403_on_human():
    from qiita_control_plane.auth.guards import require_service

    with pytest.raises(HTTPException) as exc:
        require_service(_human())
    assert exc.value.status_code == 403


# ---------------------------------------------------------------------------
# require_role_at_least
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "user_role,required,should_pass",
    [
        (SystemRole.USER, SystemRole.USER, True),
        (SystemRole.USER, SystemRole.WET_LAB_ADMIN, False),
        (SystemRole.USER, SystemRole.SYSTEM_ADMIN, False),
        (SystemRole.WET_LAB_ADMIN, SystemRole.USER, True),
        (SystemRole.WET_LAB_ADMIN, SystemRole.WET_LAB_ADMIN, True),
        (SystemRole.WET_LAB_ADMIN, SystemRole.SYSTEM_ADMIN, False),
        (SystemRole.SYSTEM_ADMIN, SystemRole.USER, True),
        (SystemRole.SYSTEM_ADMIN, SystemRole.WET_LAB_ADMIN, True),
        (SystemRole.SYSTEM_ADMIN, SystemRole.SYSTEM_ADMIN, True),
    ],
)
def test_require_role_at_least_matrix(user_role, required, should_pass):
    from qiita_control_plane.auth.guards import require_role_at_least

    dep = require_role_at_least(required)
    if should_pass:
        assert dep(_human(role=user_role))
    else:
        with pytest.raises(HTTPException) as exc:
            dep(_human(role=user_role))
        assert exc.value.status_code == 403


def test_require_role_at_least_401_on_anonymous():
    from qiita_control_plane.auth.guards import require_role_at_least

    with pytest.raises(HTTPException) as exc:
        require_role_at_least(SystemRole.USER)(_anon())
    assert exc.value.status_code == 401


def test_require_role_at_least_returns_403_for_service_account():
    """Service accounts don't fit the human hierarchy — every role check
    against them is a 403 (not a 401)."""
    from qiita_control_plane.auth.guards import require_role_at_least

    with pytest.raises(HTTPException) as exc:
        require_role_at_least(SystemRole.USER)(_service())
    assert exc.value.status_code == 403


# ---------------------------------------------------------------------------
# require_scope
# ---------------------------------------------------------------------------


def test_require_scope_pass_when_scope_present():
    from qiita_control_plane.auth.guards import require_scope

    dep = require_scope(Scope.REFERENCE_READ)
    h = _human(scopes=frozenset({Scope.SELF_PROFILE, Scope.REFERENCE_READ}))
    assert dep(h) is h


def test_require_scope_403_when_scope_missing():
    from qiita_control_plane.auth.guards import require_scope

    dep = require_scope(Scope.ADMIN_USER)
    with pytest.raises(HTTPException) as exc:
        dep(_human(scopes=frozenset({Scope.SELF_PROFILE})))
    assert exc.value.status_code == 403


def test_require_scope_401_on_anonymous():
    from qiita_control_plane.auth.guards import require_scope

    with pytest.raises(HTTPException) as exc:
        require_scope(Scope.SELF_PROFILE)(_anon())
    assert exc.value.status_code == 401


def test_require_scope_works_for_service_account_with_matching_scope():
    """Service accounts pass scope checks identically to humans — the only
    role constraint is from require_role_at_least, not require_scope."""
    from qiita_control_plane.auth.guards import require_scope

    s = _service(scopes=frozenset({Scope.FEATURE_MINT}))
    dep = require_scope(Scope.FEATURE_MINT)
    assert dep(s) is s


# ---------------------------------------------------------------------------
# require_human_with_role — composes require_human + role check
# ---------------------------------------------------------------------------
# These tests exercise the role-check body of the helper directly.
# Anonymous/Service rejection is delegated to require_human (tested above)
# via the FastAPI dep chain; calling the body with a non-human input is
# not a real production code path.


def test_require_human_with_role_returns_human_user_with_sufficient_role():
    from qiita_control_plane.auth.guards import require_human_with_role

    dep = require_human_with_role(SystemRole.USER)
    h = _human(role=SystemRole.SYSTEM_ADMIN)
    assert dep(h) is h


def test_require_human_with_role_403_on_insufficient_role():
    from qiita_control_plane.auth.guards import require_human_with_role

    dep = require_human_with_role(SystemRole.SYSTEM_ADMIN)
    with pytest.raises(HTTPException) as exc:
        dep(_human(role=SystemRole.WET_LAB_ADMIN))
    assert exc.value.status_code == 403


# ---------------------------------------------------------------------------
# require_complete_profile
# ---------------------------------------------------------------------------


def test_require_complete_profile_passes_when_complete():
    from qiita_control_plane.auth.guards import require_complete_profile

    h = _human(profile_complete=True)
    assert require_complete_profile(h) is h


def test_require_complete_profile_422_when_incomplete():
    from qiita_control_plane.auth.guards import require_complete_profile

    h = _human(profile_complete=False)
    with pytest.raises(HTTPException) as exc:
        require_complete_profile(h)
    assert exc.value.status_code == 422
    assert exc.value.detail["reason"] == "profile_incomplete"
    # The guard does NOT carry per-field missing list — the route layer
    # (POST /auth/pat) owns that, since it can pull the actual field
    # values from the DB.
    assert "missing_fields" not in exc.value.detail
