"""Auth: user-management models."""

from typing import Annotated

from pydantic import AwareDatetime, BaseModel, EmailStr, Field

from qiita_common.auth_constants import MAX_NAME_LENGTH

# ORCID iD format: four groups of four digits separated by hyphens, with the
# final character optionally being 'X' (the ISO 7064 mod-11-2 checksum).
# See https://orcid.org/.
ORCID_PATTERN = r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$"


class UserCreate(BaseModel):
    """Body for POST /api/v1/user — admin creates a user."""

    display_name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    email: EmailStr
    affiliation: str = ""
    address: str = ""
    phone: str = ""
    orcid: Annotated[str | None, Field(pattern=ORCID_PATTERN)] = None
    receive_processing_emails: bool = True


class UserUpdate(BaseModel):
    """Body for PATCH /api/v1/user/me. Excludes email and status — those are
    immutable through the self-service path. Email-change requires a separate
    flow (re-verify via OIDC); status changes are admin-only."""

    affiliation: str | None = None
    address: str | None = None
    phone: str | None = None
    orcid: Annotated[str | None, Field(pattern=ORCID_PATTERN)] = None
    receive_processing_emails: bool | None = None


class UserResponse(BaseModel):
    """Returned by user-management endpoints."""

    principal_idx: Annotated[int, Field(gt=0)]
    display_name: str
    email: EmailStr
    affiliation: str
    address: str
    phone: str
    orcid: str | None
    receive_processing_emails: bool
    profile_complete: bool
    created_at: AwareDatetime
    updated_at: AwareDatetime
