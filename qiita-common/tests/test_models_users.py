"""Tests for the user-management Pydantic models (Phase B)."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError


def test_user_create_minimal():
    from qiita_common.models import UserCreate

    u = UserCreate(display_name="Alice", email="alice@example.com")
    assert u.display_name == "Alice"
    assert u.email == "alice@example.com"
    assert u.affiliation == ""
    assert u.address == ""
    assert u.phone == ""
    assert u.orcid is None
    assert u.receive_processing_emails is True


def test_user_create_full_profile():
    from qiita_common.models import UserCreate

    u = UserCreate(
        display_name="Bob",
        email="bob@lab.org",
        affiliation="UCSD",
        address="9500 Gilman Dr, La Jolla, CA",
        phone="555-1234",
        orcid="0000-0002-1825-0097",
        receive_processing_emails=False,
    )
    assert u.affiliation == "UCSD"
    assert u.orcid == "0000-0002-1825-0097"
    assert u.receive_processing_emails is False


def test_user_create_rejects_empty_display_name():
    from qiita_common.models import UserCreate

    with pytest.raises(ValidationError):
        UserCreate(display_name="", email="a@b.com")


def test_user_create_rejects_invalid_email():
    from qiita_common.models import UserCreate

    with pytest.raises(ValidationError):
        UserCreate(display_name="Alice", email="not-an-email")


@pytest.mark.parametrize("orcid", ["bad", "0000-0002-1825-009", "abcd-efgh-ijkl-mnop"])
def test_user_create_rejects_invalid_orcid(orcid):
    from qiita_common.models import UserCreate

    with pytest.raises(ValidationError):
        UserCreate(display_name="Alice", email="a@b.com", orcid=orcid)


@pytest.mark.parametrize("orcid", ["0000-0002-1825-0097", "0000-0002-1694-233X"])
def test_user_create_accepts_valid_orcid(orcid):
    from qiita_common.models import UserCreate

    u = UserCreate(display_name="Alice", email="a@b.com", orcid=orcid)
    assert u.orcid == orcid


def test_user_update_all_optional():
    """PATCH /users/me should accept an empty body (no-op)."""
    from qiita_common.models import UserUpdate

    u = UserUpdate()
    assert u.affiliation is None
    assert u.address is None
    assert u.phone is None
    assert u.orcid is None
    assert u.receive_processing_emails is None


def test_user_update_rejects_email_field():
    """email is intentionally absent from UserUpdate — extra='ignore' by
    default in Pydantic v2 means unknown fields are silently dropped, but
    the model's contract is clear from its definition: it has no email
    attribute. We verify by attribute access that the field truly doesn't
    exist (a regression that added it would surface here)."""
    from qiita_common.models import UserUpdate

    u = UserUpdate(affiliation="UCSD")
    assert not hasattr(u, "email")


def test_user_update_partial_orcid_validation():
    from qiita_common.models import UserUpdate

    with pytest.raises(ValidationError):
        UserUpdate(orcid="bad-format")


def test_user_response_round_trip():
    """A UserResponse must round-trip from a DB-row-shaped dict."""
    from qiita_common.models import UserResponse

    db_row = {
        "principal_idx": 42,
        "display_name": "Charlie",
        "email": "charlie@example.com",
        "affiliation": "UCSD",
        "address": "addr",
        "phone": "555-0000",
        "orcid": None,
        "receive_processing_emails": True,
        "profile_complete": True,
        "created_at": datetime(2026, 4, 26, 10, 0, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 4, 26, 10, 5, 0, tzinfo=UTC),
    }
    r = UserResponse.model_validate(db_row)
    assert r.principal_idx == 42
    assert r.profile_complete is True
    assert r.created_at < r.updated_at


def test_user_response_rejects_zero_principal_idx():
    from qiita_common.models import UserResponse

    with pytest.raises(ValidationError):
        UserResponse(
            principal_idx=0,
            display_name="x",
            email="x@x.com",
            affiliation="",
            address="",
            phone="",
            orcid=None,
            receive_processing_emails=True,
            profile_complete=False,
            created_at=datetime(2026, 4, 26, tzinfo=UTC),
            updated_at=datetime(2026, 4, 26, tzinfo=UTC),
        )


def test_user_response_profile_complete_reflects_input():
    """The model carries profile_complete as-given (the DB computes it via
    a GENERATED column; the model just relays)."""
    from qiita_common.models import UserResponse

    args = dict(
        principal_idx=1,
        display_name="x",
        email="x@x.com",
        orcid=None,
        receive_processing_emails=True,
        created_at=datetime(2026, 4, 26, tzinfo=UTC),
        updated_at=datetime(2026, 4, 26, tzinfo=UTC),
    )
    r1 = UserResponse(
        **args,
        affiliation="A",
        address="B",
        phone="C",
        profile_complete=True,
    )
    assert r1.profile_complete is True

    r2 = UserResponse(
        **args,
        affiliation="",
        address="",
        phone="",
        profile_complete=False,
    )
    assert r2.profile_complete is False
