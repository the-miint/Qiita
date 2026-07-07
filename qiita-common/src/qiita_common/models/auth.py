"""Auth surface models: API-token mint / list, the CLI login exchange, the
admin service-account and principal-management bodies, the /auth/whoami
discriminated union, and the audit-event / revoke responses."""

from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from qiita_common.auth_constants import (
    MAX_NAME_LENGTH,
    PAT_MAX_TTL_DAYS,
    SERVICE_TOKEN_MAX_TTL_DAYS,
    SystemRole,
)

# ============================================================================
# Auth: API token mint / list models
# ============================================================================


class ApiTokenMintRequest(BaseModel):
    """Body for POST /api/v1/auth/pat (humans) and POST /api/v1/admin/service-account (workers).

    `scopes=None` means "default to the principal's full role ceiling" (humans
    only — service accounts must always specify scopes explicitly).
    `ttl_days=None` means "default to QIITA_TOKEN_DEFAULT_TTL_DAYS"; max 365.
    """

    label: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    scopes: list[str] | None = None
    ttl_days: Annotated[int, Field(gt=0, le=PAT_MAX_TTL_DAYS)] | None = None


class ApiTokenMintResponse(BaseModel):
    """Returned exactly once at mint time. The `token` field is the plaintext;
    capture it now and never log it. Subsequent requests retrieve only metadata
    via ApiTokenSummary."""

    token: str  # plaintext qk_... — shown once, never persisted past this response
    token_idx: Annotated[int, Field(gt=0)]
    label: str
    scopes: list[str]
    expires_at: AwareDatetime | None
    created_at: AwareDatetime


class ApiTokenSummary(BaseModel):
    """Returned by GET /api/v1/auth/token — metadata only, no plaintext or hash."""

    token_idx: Annotated[int, Field(gt=0)]
    label: str
    scopes: list[str]
    expires_at: AwareDatetime | None
    revoked_at: AwareDatetime | None
    last_used_at: AwareDatetime | None
    created_at: AwareDatetime


class CliLoginExchangeRequest(BaseModel):
    """Body for POST /api/v1/auth/cli-exchange.

    The CLI redeems a one-time `ot_code` it captured from the AuthRocket
    handoff redirect. Server consumes the row atomically and returns the
    PAT plaintext exactly once via ApiTokenMintResponse.
    """

    ot_code: str = Field(min_length=16, max_length=128)


# ============================================================================
# Auth: admin-surface models
# ============================================================================


class ServiceAccountCreate(BaseModel):
    """Body for POST /api/v1/admin/service-account.

    Scopes are required (no implicit ceiling for service kind) — admins
    must explicitly state what the worker is allowed to do. ttl_days=None
    means no expiry; service tokens are typically long-lived and rotated
    by an out-of-band runbook.
    """

    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    description: str | None = None
    scopes: list[str] = Field(min_length=1)
    ttl_days: Annotated[int, Field(gt=0, le=SERVICE_TOKEN_MAX_TTL_DAYS)] | None = None
    label: str = Field(min_length=1, max_length=MAX_NAME_LENGTH, default="initial")


class ServiceAccountCreateResponse(BaseModel):
    """Returned exactly once at service-account creation. Captures both the
    new principal/service identity and the freshly-minted token."""

    principal_idx: Annotated[int, Field(gt=0)]
    name: str
    description: str | None
    token: str  # plaintext qk_... — shown once
    token_idx: Annotated[int, Field(gt=0)]
    scopes: list[str]
    expires_at: AwareDatetime | None
    created_at: AwareDatetime


# ---------------------------------------------------------------------------
# /auth/whoami — discriminated union over principal kind
# ---------------------------------------------------------------------------


class WhoAmIHumanResponse(BaseModel):
    """`/auth/whoami` response when a HumanUser is authenticated."""

    kind: Literal["human"]
    principal_idx: Annotated[int, Field(gt=0)]
    email: str
    system_role: str
    scopes: list[str]
    profile_complete: bool


class WhoAmIServiceResponse(BaseModel):
    """`/auth/whoami` response when a ServiceAccount is authenticated."""

    kind: Literal["service"]
    principal_idx: Annotated[int, Field(gt=0)]
    name: str
    scopes: list[str]


class WhoAmIAnonymousResponse(BaseModel):
    """`/auth/whoami` response for an unauthenticated caller."""

    kind: Literal["anonymous"]


# Discriminated union — Pydantic / OpenAPI dispatch on the `kind` field.
WhoAmIResponse = Annotated[
    WhoAmIHumanResponse | WhoAmIServiceResponse | WhoAmIAnonymousResponse,
    Field(discriminator="kind"),
]


class PrincipalDisabledUpdate(BaseModel):
    """Body for PATCH /api/v1/admin/principal/{idx}/disabled.

    `disabled=true` requires `reason` (audit-trail). `disabled=false` is
    the round-trip back to active and leaves the audit columns NULL via
    the principal_disabled_consistent CHECK.
    """

    disabled: bool
    reason: str | None = None


class PrincipalRetiredUpdate(BaseModel):
    """Body for PATCH /api/v1/admin/principal/{idx}/retired.

    Retirement is terminal (CHECK forbids active → retired → active);
    `reason` is required for the audit trail.
    """

    reason: str = Field(min_length=1)


class PrepSampleRetiredUpdate(BaseModel):
    """Body for PATCH /api/v1/prep-sample/{idx}/retired.

    Reversible operator disposition (unlike the terminal principal retire): set
    `retired=true` to drop an empty / failed-yield well out of a pool's active
    set, or `retired=false` to un-retire a misclassified one. `reason` is the
    optional retire_reason (only meaningful when retiring; the DB CHECK requires
    retired_by_idx/retired_at when retired=true and forbids them otherwise, both
    populated/cleared by the route).
    """

    retired: bool
    reason: str | None = None


class PrincipalSystemRoleUpdate(BaseModel):
    """Body for PATCH /api/v1/admin/principal/{idx}/system-role.

    `use_enum_values=True` so `model_dump()` returns the lowercase string
    (e.g. `"user"`) rather than the `SystemRole` member — preserves the
    JSON-serialised contract that pre-dated the StrEnum migration.
    """

    model_config = ConfigDict(use_enum_values=True)

    system_role: SystemRole
    reason: str | None = None


class AuthEventResponse(BaseModel):
    """One row from GET /api/v1/admin/audit."""

    event_idx: Annotated[int, Field(gt=0)]
    event_type: str
    principal_idx: int | None
    actor_principal_idx: int | None
    detail: dict
    occurred_at: AwareDatetime


class RevokeAllTokensResponse(BaseModel):
    """Returned by POST /api/v1/admin/principal/{idx}/revoke-all-tokens."""

    revoked_token_idxs: list[int]
    already_revoked_count: int
