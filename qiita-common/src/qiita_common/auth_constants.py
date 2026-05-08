"""Shared auth constants: roles, scopes, event types, and policy limits.

Imported by both control-plane (route handlers, scope ceilings, audit emit
sites) and qiita-common (Pydantic model field types). Modeled on the existing
constants pattern in `qiita_control_plane.auth.__init__` — module-level
declarations with inline justification — and the `StrEnum` template at
`qiita_common.models.ReferenceStatus`.

StrEnum members compare equal to their string values, so existing code that
compares against bare strings (e.g. `system_role == "user"`) keeps working,
and JSON / DB serialization round-trips through the same lowercase values.
"""

from enum import StrEnum


class SystemRole(StrEnum):
    """Enum mirror of the Postgres `qiita.system_role` enum.

    Hierarchy: SYSTEM_ADMIN > WET_LAB_ADMIN > USER. The hierarchy is
    encoded in `auth.principal._ROLE_ORDER` and the per-role ceilings in
    `auth.scopes.ROLE_IMPLIED_SCOPES`; this enum is the closed value set.
    """

    USER = "user"
    WET_LAB_ADMIN = "wet_lab_admin"
    SYSTEM_ADMIN = "system_admin"


class Scope(StrEnum):
    """Closed set of scope strings the mint path validates against.

    Format is `<resource>:<verb>`. Reference-data scopes target sequence
    references / features / Flight tickets; admin scopes gate admin-surface
    routes; self-* scopes are humans-only.
    """

    # Reference data
    REFERENCE_READ = "reference:read"
    REFERENCE_WRITE = "reference:write"
    REFERENCE_REGISTER_FILES = "reference:register_files"
    FEATURE_MINT = "feature:mint"
    TICKET_DOGET = "ticket:doget"

    # Biosample data
    BIOSAMPLE_READ = "biosample:read"
    BIOSAMPLE_WRITE = "biosample:write"

    # Study data
    STUDY_READ = "study:read"
    STUDY_WRITE = "study:write"

    # Admin operations
    ADMIN_USER = "admin:user"
    ADMIN_SERVICE_ACCOUNT = "admin:service_account"
    ADMIN_AUDIT_READ = "admin:audit_read"

    # Self-service (humans only)
    SELF_PROFILE = "self:profile"
    SELF_TOKEN = "self:token"


class AuthEventType(StrEnum):
    """Closed set of event_type values written to qiita.auth_event.

    Mirrors the column comment on `qiita.auth_event.event_type` but lists
    only the values currently emitted from Python. The DB column is TEXT, so
    adding members here is a no-op at the schema layer; future event types
    named in the column comment (e.g. `oidc_login`, `token_use`,
    `token_verify_failure`) can be added when their emit sites are introduced.
    """

    OIDC_CREATE_PRINCIPAL = "oidc_create_principal"
    OIDC_CREATE_PRINCIPAL_EMAIL_CONFLICT = "oidc_create_principal_email_conflict"
    # Recorded when a returning OIDC user's JWT email differs from the
    # email stored on qiita.user — i.e., they changed it at the IdP. Detail
    # carries `outcome=updated` (we synced the new value) or
    # `outcome=collision` (another user already has that email; we logged
    # sha256 of the attempted value and left the existing email in place).
    EMAIL_DRIFT = "email_drift"
    TOKEN_MINT = "token_mint"
    TOKEN_REVOKE = "token_revoke"
    PRINCIPAL_DISABLED = "principal_disabled"
    PRINCIPAL_ENABLED = "principal_enabled"
    PRINCIPAL_RETIRED = "principal_retired"
    SYSTEM_ROLE_CHANGE = "system_role_change"


# The system principal occupies idx=1 in `qiita.principal`, seeded with
# OVERRIDING SYSTEM VALUE. It is the `created_by_idx` for any principal
# minted by an OIDC first-login (where there's no human actor yet) and is
# forbidden from being disabled, retired, or having its role changed (CHECK
# and route guards).
SYSTEM_PRINCIPAL_IDX = 1


# REST API path prefix. The control plane mounts every route under this prefix
# via `APIRouter(prefix=API_PREFIX)`; the in-tree client and CLI build URLs
# from the same constant so renames stay in lockstep.
API_PREFIX = "/api/v1"


# HTTP Authorization header bearer-scheme prefix (note the trailing space).
# Splitting on this is what `auth.principal.get_current_principal` does.
BEARER_PREFIX = "Bearer "


# Pydantic Field max_length policy values. 255 is the historical "name-ish"
# default that lines up with VARCHAR(255) in DB columns; 100 covers reference
# version strings; 64 caps DuckLake / DB table names per Postgres identifier
# limits.
MAX_NAME_LENGTH = 255
MAX_VERSION_LENGTH = 100
MAX_TABLE_NAME_LENGTH = 64


# TTL maxima enforced at the API boundary. Human PATs cap at 1 year so a
# departed user's token can't outlive a typical access-review cycle. Service
# tokens cap at 10 years — workers are rotated by an out-of-band runbook and
# a long ceiling avoids forced rotation cliffs.
PAT_MAX_TTL_DAYS = 365
SERVICE_TOKEN_MAX_TTL_DAYS = 3650


# Pagination policy for GET /admin/audit. Default is small to keep responses
# cheap; the cap prevents accidental full-table scans through the API.
AUDIT_QUERY_DEFAULT_LIMIT = 100
AUDIT_QUERY_MAX_LIMIT = 1000


# Postgres interval string used to coalesce repeated last_used_at writes on
# api_tokens — within this window the UPDATE is skipped to avoid hot-row
# contention on a frequently-validated token. Embedded in a SQL fragment, so
# the value must be a valid `interval` literal.
LAST_USED_AT_COALESCE_INTERVAL = "1 minute"


# Shared HTTPException detail strings. Kept here so the resolver, route
# handlers, and admin paths emit byte-identical messages — a drift between
# them would force tests / clients to special-case wording per call site.
MSG_PRINCIPAL_DISABLED_OR_RETIRED = "principal disabled or retired"
MSG_PRINCIPAL_NOT_FOUND = "principal not found"
