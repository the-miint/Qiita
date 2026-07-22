"""Submit helper for the `download-ena-study` work ticket.

Thin, admin-facing composer: builds the `WorkTicketCreateRequest` body for
ONE `sequenced_pool`-scoped `download-ena-study` ticket, `action_context`
carrying `{ena_study_accession, download_method}`. Mirrors the shape
`submit-bcl-convert` (`qiita_control_plane.cli.user.pool`) composes for its
`sequenced_pool` scope target, but skips all of that flow's sample-sheet /
pool-provisioning ceremony -- by the time this helper is called, the pool and
its `sequenced_sample` rows already exist (created by
`ena_import.registration.register_ena_study`).

Deliberately a PURE body-builder -- no DB access, no HTTP call. The caller
(the batch driver, which fans a whole study's runs out into one ticket
per `(study, platform)` pool `register_ena_study` created, or an operator via
the existing generic `qiita user ticket submit`) is responsible for knowing
`sequenced_pool_idx` / `sequencing_run_idx` (e.g. from
`register_ena_study`'s own `EnaStudyRegistrationResult`, or a
`fetch_sequenced_pool` lookup) and for POSTing the built request through the
existing `/api/v1/work-ticket` route. Staying pure keeps this helper directly
unit-testable and means the audience / scope / disallow-without-delete checks
that route already enforces are never duplicated here.
"""

from __future__ import annotations

from qiita_common.models import ScopeTargetKind, WorkTicketCreateRequest

# action_id + version for the download-ena-study workflow. Pinned here so a
# caller does not drift from the workflow YAML the operator's deploy syncs
# into qiita.action -- mirrors `_BCL_CONVERT_ACTION_ID` in
# `qiita_control_plane.cli.user.pool`.
DOWNLOAD_ENA_STUDY_ACTION_ID = "download-ena-study"
DOWNLOAD_ENA_STUDY_ACTION_VERSION = "1.0.0"

# The only transport this compute environment supports (no Aspera
# key-staging) -- see ARCHITECTURE.md's ENA Study Import download-ticket-
# granularity decision (2026-07-21). Matches the workflow YAML's context_schema, which
# pins `download_method` to a single-value enum.
DEFAULT_DOWNLOAD_METHOD = "http"


def build_download_ena_study_ticket(
    *,
    sequenced_pool_idx: int,
    sequencing_run_idx: int,
    ena_study_accession: str,
    download_method: str = DEFAULT_DOWNLOAD_METHOD,
) -> WorkTicketCreateRequest:
    """Compose ONE `sequenced_pool`-scoped `download-ena-study`
    `WorkTicketCreateRequest`.

    `sequenced_pool_idx` / `sequencing_run_idx` identify the pool
    `ena_import.registration.register_ena_study` already created for this
    `(study, platform)` pair -- this helper does no DB I/O to resolve them,
    so it stays a pure, directly-testable unit; the caller supplies them.

    Returns the request model; the caller POSTs it to
    `/api/v1/work-ticket` (e.g. via `qiita_control_plane.cli._common.call`
    or an `httpx` client)."""
    return WorkTicketCreateRequest(
        action_id=DOWNLOAD_ENA_STUDY_ACTION_ID,
        action_version=DOWNLOAD_ENA_STUDY_ACTION_VERSION,
        scope_target={
            "kind": ScopeTargetKind.SEQUENCED_POOL.value,
            "sequenced_pool_idx": sequenced_pool_idx,
            "sequencing_run_idx": sequencing_run_idx,
        },
        action_context={
            "ena_study_accession": ena_study_accession,
            "download_method": download_method,
        },
    )
