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
    # Full purge of a reference (Postgres rows + DuckLake data + on-disk
    # indexes). Deliberately distinct from REFERENCE_WRITE: deletion is
    # destructive and admin-only, granted solely to system_admin in
    # ROLE_IMPLIED_SCOPES — never to wet_lab_admin or service accounts.
    REFERENCE_DELETE = "reference:delete"
    # Curate the global reference exclusion blocklist (block/unblock a bad
    # genome_idx / feature_idx so it stops contributing to downstream products).
    # Destructive-adjacent and admin-only: granted solely to system_admin in
    # ROLE_IMPLIED_SCOPES — never to wet_lab_admin or service accounts. Mirrors
    # REFERENCE_DELETE (the read-only GET /reference/{idx}/exclusion rides
    # REFERENCE_READ instead).
    REFERENCE_EXCLUSION_WRITE = "reference:exclusion:write"
    REFERENCE_REGISTER_FILES = "reference:register_files"
    FEATURE_MINT = "feature:mint"
    # Sign DoGet tickets for the data plane's reference-data surfaces, the
    # per-sample `alignment` slice (host-depleted derived data, the feature-table
    # OGU job's input — see routes/alignment.py), and one assembly run's contig
    # sequences (routes/assembly.py, which carries what that reuse grants).
    # Deliberately NOT split out the way READ_MASKED_DOGET is below: neither
    # alignment nor an assembled contig is raw human/host reads, so both ride the
    # generic scope rather than a privacy-sensitive one.
    TICKET_DOGET = "ticket:doget"
    # Mint an alignment DoGet ticket as a HUMAN, naming the cohort directly.
    #
    # Split from TICKET_DOGET because the two carry different trust models, not
    # different data. TICKET_DOGET signs a cohort the control plane read out of a
    # work ticket's action_context, which the runner resolver already validated
    # at submit; there is no such upstream for a client-driven request, so this
    # scope's route resolves the cohort and authorizes it per-study (Tier.VIEWER
    # on every study each sample links to) before signing. Reusing TICKET_DOGET
    # would have put a human PAT on the worker path — and it is deliberately
    # absent from every role ceiling precisely so that cannot happen.
    #
    # On every role ceiling, unlike its neighbours: for a plain user the
    # per-study tier check is the real boundary, so the role does not need to be.
    # That argument is honest only for `user`. `wet_lab_admin` and `system_admin`
    # are at or above the cohort gate's `bypass_role`
    # (`filter_prep_samples_caller_can_read`), so for them the whole cohort comes
    # back readable with no per-study lookup at all and the 403 cannot fire — the
    # ROLE is the boundary, which is the inverse of the reason above. Both are
    # deliberate; they are just not the same reason, and a reader who assumes the
    # per-study check protects every caller is wrong about the two roles that
    # matter most. Correspondingly NOT on SERVICE_ACCOUNT_SCOPE_CEILING — a
    # worker holding both would be two ways into one surface with two different
    # validation paths.
    ALIGNMENT_DOGET = "alignment:doget"
    # Mint an assembly-run DoGet ticket as a HUMAN, naming the run directly.
    #
    # Split from TICKET_DOGET on exactly the argument ALIGNMENT_DOGET above makes,
    # and reached by the same route shape: the work-ticket mint signs a run the
    # control plane read out of a validated action_context, this one resolves and
    # authorizes the run the caller named (Tier.VIEWER on every study its
    # prep_sample links to). The two ceilings follow from that and not from the
    # data — this is on every human role ceiling and NOT on
    # SERVICE_ACCOUNT_SCOPE_CEILING, so a worker keeps the other route and one
    # surface never has two validation paths.
    #
    # What it opens to a human PAT is `assembled_sequence` /
    # `assembled_sequence_chunks`: contigs assembled from the `read_masked`
    # pass-set, which TICKET_DOGET already classes as not raw human/host reads —
    # so this widens WHO may ask for an assembly, never WHAT an assembly ticket
    # returns. The privacy-sensitive neighbours (READ_MASKED_DOGET, READ_DOGET)
    # are for the read surfaces and are untouched by it.
    ASSEMBLY_DOGET = "assembly:doget"
    # DoGet against the data plane's masked-read surface (`read_masked`).
    # Deliberately distinct from the generic TICKET_DOGET above: masked reads are
    # privacy-sensitive (the lake retains human/host reads, excluded only by the
    # read_masked macro), so the capability to pull them is granted separately —
    # to service accounts that drive the masked-read consumer path, never
    # piggybacking on the surfaces TICKET_DOGET already opens.
    READ_MASKED_DOGET = "read_masked:doget"
    # DoGet against the data plane's BLOCK-read selectors (`read_block` /
    # `read_masked_block`) — one block's `(prep_sample_idx, sequence_idx
    # sub-range)` members, streamed to a block-scoped compute job.
    #
    # Distinct from BOTH neighbours, and strictly the most privileged of the
    # three. `read_block` streams RAW `read` rows: host/human sequence that the
    # `read_masked` macro exists to exclude, so it is a strict superset of what
    # READ_MASKED_DOGET covers. Riding TICKET_DOGET would let any service account
    # minting reference tickets pull raw reads — an inversion of the model the
    # two scopes above establish. Granted only to the service account that drives
    # block compute.
    READ_DOGET = "read:doget"
    # Full purge of a mask (the mask_definition row + its DuckLake read_mask
    # rows). Deliberately distinct from the mask-minting capability: deletion
    # is destructive and admin-only, granted solely to system_admin in
    # ROLE_IMPLIED_SCOPES — never to wet_lab_admin or service accounts.
    # Mirrors REFERENCE_DELETE.
    MASK_DEFINITION_DELETE = "mask_definition:delete"
    # Mask LIFECYCLE: deprecate a config so it can no longer be minted against, and
    # withdraw individual runs of a sound config. Separate from
    # MASK_DEFINITION_DELETE because the two are opposites — deletion destroys the
    # record of what filtered published data, deprecation preserves it and adds the
    # judgement. Separate from the minting capability (READ_MASKED_DOGET) because a
    # service account that mints masks must not be able to void them. system_admin
    # only, in ROLE_IMPLIED_SCOPES.
    MASK_DEFINITION_LIFECYCLE = "mask_definition:lifecycle"
    # Processing-run LIFECYCLE: deprecate a qiita.processing config, and withdraw
    # individual (run, sample) pairs of a sound one. Separate from
    # MASK_DEFINITION_LIFECYCLE because the two name different identities — a mask
    # can be sound while an assembly built from it is not, and vice versa. There is
    # no matching :delete scope, because there is no delete path to gate: the
    # `assembly_sample.processing_idx` FK is ON DELETE RESTRICT. system_admin only,
    # in ROLE_IMPLIED_SCOPES.
    PROCESSING_LIFECYCLE = "processing:lifecycle"
    # Full purge of an alignment (the alignment_definition row + its DuckLake
    # alignment rows; the alignment_sample gate cascade-deletes). Deliberately
    # distinct from the align-submitting capability (PREP_SAMPLE_WRITE): deletion
    # is destructive and admin-only, granted solely to system_admin in
    # ROLE_IMPLIED_SCOPES — never to wet_lab_admin or service accounts. Mirrors
    # MASK_DEFINITION_DELETE; it is the disallow-without-delete escape hatch.
    ALIGNMENT_DEFINITION_DELETE = "alignment_definition:delete"
    # Generic upload domain. Gates the slot-minting + DoPut path; not
    # reference-specific. On every role ceiling and on service accounts: it
    # buys a numbered staging slot the caller owns and nothing else, so what
    # an upload may then FEED is gated separately (reference-add needs
    # `reference:write`; a work_ticket naming another principal's upload_idx
    # is refused at dispatch).
    TICKET_DOPUT = "ticket:doput"

    # Biosample data
    BIOSAMPLE_READ = "biosample:read"
    BIOSAMPLE_WRITE = "biosample:write"

    # Prep-sample data
    PREP_SAMPLE_READ = "prep_sample:read"
    PREP_SAMPLE_WRITE = "prep_sample:write"

    # Sequence-range allocation (workers-only)
    SEQUENCE_RANGE_MINT = "sequence_range:mint"

    # Sequenced-pool preflight blob read (workers-only). Gates the SA-only
    # GET /sequencing-run/{R}/sequenced-pool/{P}/preflight route the
    # bcl-convert prep step calls to materialize the sample sheet.
    SEQUENCED_POOL_PREFLIGHT_READ = "sequenced_pool:preflight:read"
    # Full hard-delete of a sequenced_pool (the pool row plus every
    # sequenced_sample / prep_sample under it, their metadata, study links,
    # and pool-/sample-scoped work tickets). Destructive and admin-only —
    # granted solely to system_admin in ROLE_IMPLIED_SCOPES, never to
    # wet_lab_admin or service accounts. Mirrors REFERENCE_DELETE.
    SEQUENCED_POOL_DELETE = "sequenced_pool:delete"

    # Study data
    STUDY_READ = "study:read"
    STUDY_WRITE = "study:write"

    # Admin operations
    ADMIN_USER = "admin:user"
    ADMIN_SERVICE_ACCOUNT = "admin:service_account"
    ADMIN_AUDIT_READ = "admin:audit_read"
    # Re-identification read: dump the owner-submitted original sample names
    # (biosample_metadata where is_owner_biosample_id=true) keyed by minted
    # idx + public accession. That value is the owner's own name for their
    # sample, restricted to study members rather than shown on the normal
    # biosample:read path (submitters sometimes put PII in sample names), so
    # exporting it gets its own system_admin-only scope rather than
    # overloading biosample:read. Granted solely to
    # system_admin in ROLE_IMPLIED_SCOPES; never wet_lab_admin or service
    # accounts.
    ADMIN_BIOSAMPLE_OWNER_ID_READ = "admin:biosample_owner_id_read"
    # Admin per-pool masked-read export: list a sequenced_pool's samples and mint
    # per-sample DoGet tickets on the data plane's read_masked macro, so an admin
    # can download masked sequence data locally. This is the first *human*
    # masked-read pull — distinct from the service-account READ_MASKED_DOGET path
    # (which is untouched). Admin-gated until there's a model for auto-selecting
    # the correct mask; granted solely to system_admin in ROLE_IMPLIED_SCOPES,
    # never wet_lab_admin or service accounts.
    ADMIN_MASKED_READ_EXPORT = "admin:masked_read_export"

    # Operator-cancel of in-flight compute: flip a work_ticket terminal
    # (cancelled) so the CP stops driving it AND scancel its SLURM job(s). Privileged
    # — it stops running work and crosses into the compute account's reap on the
    # operator's behalf — so it is granted solely to system_admin in
    # ROLE_IMPLIED_SCOPES, never to wet_lab_admin or service accounts. Mirrors the
    # destructive-delete scopes (REFERENCE_DELETE / SEQUENCED_POOL_DELETE).
    WORK_TICKET_CANCEL = "work_ticket:cancel"

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


# Machine-readable marker the scope guards set on a *stale-scope* 403 — one
# whose real cause is a PAT minted before the missing scope entered the
# caller's live role ceiling (or a PAT deliberately narrowed below it). The
# server is the authoritative place to decide this (it holds the live
# `role_ceiling`), so it flags the condition here as a response header rather
# than in the detail prose. The CLI's single HTTP-error chokepoint keys off
# this header to surface a clean "run `qiita login`" prompt, without the CLI
# needing its own (drift-prone) copy of the role ceiling. Presence is the
# signal; the value is unspecified beyond being truthy.
STALE_TOKEN_SCOPE_HEADER = "X-Qiita-Stale-Token-Scope"


# Pydantic Field max_length policy values. 255 is the historical "name-ish"
# default that lines up with VARCHAR(255) in DB columns; 100 covers reference
# version strings; 64 caps DuckLake / DB table names per Postgres identifier
# limits; 50 tracks the VARCHAR(50) accession columns, so an over-long
# accession fails at the wire rather than in the database.
MAX_NAME_LENGTH = 255
MAX_VERSION_LENGTH = 100
MAX_TABLE_NAME_LENGTH = 64
MAX_ACCESSION_LENGTH = 50


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
