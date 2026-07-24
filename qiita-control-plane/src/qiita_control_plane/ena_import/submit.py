"""Submit helper for the `download-ena-study` work ticket.

Builds the `WorkTicketCreateRequest` body for ONE `sequenced_pool`-scoped
`download-ena-study` ticket, `action_context` carrying `{ena_study_accession,
download_method}`. By the time this is called, the pool and its
`sequenced_sample` rows already exist (from `register_ena_study`).

Deliberately a PURE body-builder -- no DB access, no HTTP. The caller supplies
`sequenced_pool_idx` / `sequencing_run_idx` and POSTs the request through the
existing `/api/v1/work-ticket` route, so that route's audience / scope /
disallow-without-delete checks are never duplicated here.
"""

from __future__ import annotations

from qiita_common.models import ScopeTargetKind, WorkTicketCreateRequest

# action_id + version for the download-ena-study workflow. Pinned here so a
# caller can't drift from the workflow YAML the deploy syncs into qiita.action.
DOWNLOAD_ENA_STUDY_ACTION_ID = "download-ena-study"
DOWNLOAD_ENA_STUDY_ACTION_VERSION = "1.0.0"

# The only transport this compute environment supports (no Aspera key-staging);
# matches the workflow YAML's single-value download_method enum.
DEFAULT_DOWNLOAD_METHOD = "http"


def build_download_ena_study_ticket(
    *,
    sequenced_pool_idx: int,
    sequencing_run_idx: int,
    ena_study_accession: str,
    download_method: str = DEFAULT_DOWNLOAD_METHOD,
) -> WorkTicketCreateRequest:
    """Compose ONE `sequenced_pool`-scoped `download-ena-study`
    `WorkTicketCreateRequest`. `sequenced_pool_idx` / `sequencing_run_idx`
    identify the pool `register_ena_study` already created; the caller supplies
    them and POSTs the returned model to `/api/v1/work-ticket`."""
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
