"""Unit tests for the download-ena-study submit helper (TASK-04).

Pure-function coverage (no DB, no HTTP) for
`ena_import.submit.build_download_ena_study_ticket` — the thin composer
TASK-06's batch driver (and an operator via the generic `qiita user ticket
submit`) uses to open one sequenced_pool-scoped download-ena-study ticket.
"""

from __future__ import annotations

from qiita_common.models import ScopeTargetKind, WorkTicketCreateRequest

from qiita_control_plane.ena_import import (
    DEFAULT_DOWNLOAD_METHOD,
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    build_download_ena_study_ticket,
)


def test_builds_sequenced_pool_scoped_ticket_with_default_transport():
    """Default call: action_id/version pinned, scope_target carries both pool
    scalars, action_context carries the accession + the default ('http')
    transport."""
    req = build_download_ena_study_ticket(
        sequenced_pool_idx=7,
        sequencing_run_idx=3,
        ena_study_accession="PRJEB1234",
    )
    assert isinstance(req, WorkTicketCreateRequest)
    assert req.action_id == DOWNLOAD_ENA_STUDY_ACTION_ID
    assert req.action_version == DOWNLOAD_ENA_STUDY_ACTION_VERSION
    assert req.scope_target.kind == ScopeTargetKind.SEQUENCED_POOL
    assert req.scope_target.sequenced_pool_idx == 7
    assert req.scope_target.sequencing_run_idx == 3
    assert req.action_context == {
        "ena_study_accession": "PRJEB1234",
        "download_method": DEFAULT_DOWNLOAD_METHOD,
    }


def test_default_download_method_is_http():
    """Locks the default transport to 'http' — the only value this compute
    environment supports (no Aspera key-staging); a change here must be a
    deliberate decision, not a silent drift."""
    assert DEFAULT_DOWNLOAD_METHOD == "http"


def test_explicit_download_method_overrides_default():
    req = build_download_ena_study_ticket(
        sequenced_pool_idx=1,
        sequencing_run_idx=1,
        ena_study_accession="PRJEB1",
        download_method="http",
    )
    assert req.action_context["download_method"] == "http"


def test_action_id_version_pinned_against_the_synced_workflow():
    """The pinned constants must match the on-disk workflow YAML the
    operator's deploy syncs into qiita.action — a drift here would submit
    tickets against a non-existent (or wrong-shaped) action."""
    from pathlib import Path

    from qiita_control_plane.actions import load_actions

    repo_root = Path(__file__).resolve().parents[3]
    by_id = {a.action_id: a for a in load_actions(repo_root / "workflows")}
    assert "download-ena-study" in by_id, "workflows/download-ena-study/1.0.0.yaml must load"
    action = by_id["download-ena-study"]
    assert DOWNLOAD_ENA_STUDY_ACTION_ID == action.action_id
    assert DOWNLOAD_ENA_STUDY_ACTION_VERSION == action.version
