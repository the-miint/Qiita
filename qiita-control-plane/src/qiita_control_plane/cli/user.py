"""qiita — end-user CLI for the Qiita control plane.

Scope: credentialed HTTP calls against a running deployment.

Placement rule (qiita vs qiita-admin) — the deciding test is how a
command reaches the system and whether the auth model can gate it:

  qiita        — credentialed API calls over HTTP+PAT. The server's
                 role/scope guards decide what's allowed, so the binary
                 is NOT the security boundary; the server is. A command
                 only a system_admin can use still belongs here if it's a
                 normal authenticated API call (the server 403s everyone
                 else).
  qiita-admin  — operator-on-the-host actions that run *outside* the
                 API/auth model: direct Postgres writes (gated by
                 DATABASE_URL) or host/cluster operations. They exist for
                 moments the auth system can't help — no admin exists yet,
                 the API is down, or you're recovering state.

This module owns the user-facing argparse surface and its subcommand
handlers. PAT file I/O, the LoginRocket loopback flow, the
authenticated HTTP call helper, and the generic token-read + invoke +
JSON-print runner live in `cli._common`.

Authentication: HTTP subcommands read the PAT from QIITA_TOKEN env or
from ~/.qiita/token (mode 0600).
"""

import argparse
import asyncio
import base64
import json
import sqlite3
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

from pydantic import BaseModel, ValidationError
from qiita_common.api_paths import (
    PATH_BIOSAMPLE_BY_IDX,
    PATH_BIOSAMPLE_LIST_BY_STUDY,
    PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION,
    PATH_BIOSAMPLE_PREFIX,
    PATH_PREP_SAMPLE_PREFIX,
    PATH_PREP_SAMPLE_STUDY_LIST,
    PATH_REFERENCE_BY_IDX,
    PATH_REFERENCE_INDEX,
    PATH_REFERENCE_PREFIX,
    PATH_SEQUENCED_POOL_BY_IDX,
    PATH_SEQUENCED_SAMPLE_BY_IDX,
    PATH_SEQUENCED_SAMPLE_FROM_RUN,
    PATH_SEQUENCED_SAMPLE_LIST_BY_POOL,
    PATH_SEQUENCED_SAMPLE_LIST_BY_RUN_FULL,
    PATH_SEQUENCED_SAMPLE_PREFIX,
    PATH_SEQUENCING_RUN_BY_IDX,
    PATH_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID,
    PATH_SEQUENCING_RUN_PREFIX,
    PATH_SEQUENCING_RUN_SEQUENCED_POOL,
    PATH_STUDY_BY_IDX,
    PATH_STUDY_LOOKUP_BY_ACCESSION,
    PATH_STUDY_PREFIX,
    PATH_WORK_TICKET_PREFIX,
)
from qiita_common.illumina import read_instrument_run_info
from qiita_common.models import (
    HOST_FILTER_INDEX_TYPE_MINIMAP2,
    HOST_FILTER_INDEX_TYPE_RYPE,
    BiosampleImportRequest,
    BiosampleLookupByAccessionRequest,
    BiosamplePatchRequest,
    Platform,
    ReferenceStatus,
    ScopeTargetKind,
    SequencedPoolCreateRequest,
    SequencedSampleCreateRequest,
    SequencedSamplePatchRequest,
    SequencingRunCreateRequest,
    StudyCreate,
    StudyLookupByAccessionRequest,
    StudyPatchRequest,
    Tier,
    UserUpdate,
    WorkTicketCreateRequest,
    WorkTicketState,
)

from . import _common

# action_id + version for the bundled bcl-convert submission flow. Pinned
# here so the CLI does not drift from the workflow YAML the operator's
# deploy syncs into qiita.action; bumping the workflow major version is a
# coordinated change.
_BCL_CONVERT_ACTION_ID = "bcl-convert"
_BCL_CONVERT_ACTION_VERSION = "1.0.0"

# action_id + version for the submit-host-filter-pool fan-out. 1.2.0 adds the
# always-on QC step + the two-reference host filter (1.0.0 has no host_filter,
# 1.1.0 is the legacy single-reference host filter); pinned here for the same
# drift reason as the bcl-convert constants above.
_FASTQ_TO_PARQUET_ACTION_ID = "fastq-to-parquet"
_FASTQ_TO_PARQUET_ACTION_VERSION = "1.2.0"


class _PreflightRow(NamedTuple):
    """One illumina_sample row pulled from the kl-run-preflight SQLite.

    The first four fields mirror `run_preflight.get_illumina_sample_info`'s
    4-tuple. `secondary_project_accessions` is empty for non-control samples;
    controls carry one entry per non-primary plate project, sorted by accession
    value. `human_filtering` is the sample's effective project's
    `human_filtering` flag — True -> deplete against the operator's host
    reference(s), False -> no host filtering — mapped onto the sequenced_sample's
    host reference columns at creation. It is sourced separately from the info
    4-tuple: the sample's effective project comes from
    `run_preflight.db.get_illumina_sample_rows` (the project_name column) joined
    to a `project.human_filtering` read, since the library exposes no per-sample
    human_filtering accessor.
    """

    illumina_sample_idx: int
    biosample_accession: str
    primary_project_accession: str
    secondary_project_accessions: list[str]
    human_filtering: bool


# ---------------------------------------------------------------------------
# HTTP helpers (call sites for individual endpoints)
# ---------------------------------------------------------------------------


def _patch_user_me(base_url: str, token: str, updates: dict) -> dict:
    """PATCH /api/v1/user/me with the (already-pruned) updates dict.

    Only the fields the caller actually set are sent; unset ones stay
    absent so the server's `exclude_unset` SET-clause builder never
    UPDATEs a field the user didn't ask about. With an empty dict, the
    route round-trips the current profile — handy as a side-effect
    "show" but argparse requires at least one --flag here so empty
    bodies don't slip through silently.
    """
    return _common.call("PATCH", base_url, token, "/user/me", json=updates)


def _post_study(base_url: str, token: str, body: dict) -> dict:
    """POST /api/v1/study with the (already-pruned) body. Owner defaults to
    the caller server-side; the CLI does not surface --owner-idx because
    naming a different owner requires wet_lab_admin+ (lab-tech-on-behalf),
    out of scope for the regular-user CLI."""
    return _common.call("POST", base_url, token, "/study", json=body)


def _post_biosample(base_url: str, token: str, study_idx: int, body: dict) -> dict:
    """POST /api/v1/study/{study_idx}/biosample with the (already-pruned) body.

    The route currently requires owner_idx in the body explicitly; the CLI
    handler resolves it via whoami when --owner-idx is omitted so the
    caller can run `qiita biosample create` for themselves without first
    chasing their own principal_idx.
    """
    return _common.call("POST", base_url, token, f"/study/{study_idx}/biosample", json=body)


def _post_sequencing_run(base_url: str, token: str, body: dict) -> dict:
    """POST /api/v1/sequencing-run with the (already-pruned) body.

    Instrument-level resource: not study-scoped, has no owner field. The
    creator is recorded server-side as created_by_idx.
    """
    return _common.call("POST", base_url, token, "/sequencing-run", json=body)


def _post_sequenced_pool(base_url: str, token: str, run_idx: int, body: dict) -> dict:
    """POST /api/v1/sequencing-run/{run_idx}/sequenced-pool with the
    (already-pruned) body. The run-preflight pair (blob + filename) is
    co-populated; the route's Pydantic validator returns 422 on a
    half-populated pair, but the CLI guards earlier so the user sees an
    argparse-style error instead of a server round-trip.
    """
    return _common.call(
        "POST", base_url, token, f"/sequencing-run/{run_idx}/sequenced-pool", json=body
    )


def _post_sequenced_sample(
    base_url: str, token: str, run_idx: int, pool_idx: int, body: dict
) -> dict:
    """POST /api/v1/sequencing-run/{R}/sequenced-pool/{P}/sequenced-sample
    composer. Atomically mints the prep_sample + sequenced_sample subtype +
    prep_sample_to_study links (primary + each secondary) + metadata rows.
    """
    return _common.call(
        "POST",
        base_url,
        token,
        f"/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample",
        json=body,
    )


def _post_work_ticket(base_url: str, token: str, body: dict) -> dict:
    """POST /api/v1/work-ticket. originator_principal_idx is set server-side
    from the authenticated caller; the body carries action_id, action_version,
    scope_target (discriminated union), and action_context (free-form per the
    action's declared context_schema)."""
    return _common.call("POST", base_url, token, PATH_WORK_TICKET_PREFIX, json=body)


def _get_work_ticket(base_url: str, token: str, work_ticket_idx: int) -> dict:
    """GET /api/v1/work-ticket/{idx}. Returns the full WorkTicket record —
    state, action info, scope_target, action_context, retry accounting,
    failure surface, timestamps. Auth: originator or wet_lab_admin+."""
    return _common.call("GET", base_url, token, f"{PATH_WORK_TICKET_PREFIX}/{work_ticket_idx}")


def _run_work_ticket(base_url: str, token: str, work_ticket_idx: int) -> dict:
    """POST /api/v1/work-ticket/{idx}/run. Operator override and the only retry
    mechanism (there is no auto-retry worker): resets a FAILED ticket to PENDING
    (clears failure_*, retry_count → 0) and re-dispatches it, or re-dispatches a
    PENDING ticket whose create-time dispatch was lost. The runner fast-forwards
    every step already marked COMPLETED and resumes at the first incomplete one,
    so a costly finished step (e.g. stage_local_fasta) is not recomputed. No
    request body. Auth: originator or wet_lab_admin+. Returns the new {idx,
    state}."""
    return _common.call("POST", base_url, token, f"{PATH_WORK_TICKET_PREFIX}/{work_ticket_idx}/run")


def _get_work_ticket_step_logs(
    base_url: str,
    token: str,
    work_ticket_idx: int,
    *,
    step_index: int,
    attempt: int | None,
    tail_lines: int | None,
) -> dict:
    """GET /api/v1/work-ticket/{idx}/step/{step_index}/logs. Returns the step
    attempt's stdout/stderr tail (and per-stream truncation flags). Query
    params are sent only when set so the server's defaults (latest attempt,
    200 lines) apply otherwise. Auth: originator or wet_lab_admin+."""
    params: dict[str, str] = {}
    if attempt is not None:
        params["attempt"] = str(attempt)
    if tail_lines is not None:
        params["tail_lines"] = str(tail_lines)
    return _common.call(
        "GET",
        base_url,
        token,
        f"{PATH_WORK_TICKET_PREFIX}/{work_ticket_idx}/step/{step_index}/logs",
        params=params,
    )


def _list_work_tickets(
    base_url: str,
    token: str,
    *,
    state: str | None,
    active: bool,
    all_tickets: bool,
    limit: int | None,
) -> list:
    """GET /api/v1/work-ticket. Returns a list of WorkTicketSummary records —
    each ticket plus its current step's compute_target / slurm_job_id /
    step_state. Scope: the caller's own tickets, or all originators' with
    `all_tickets` (wet_lab_admin+). Query params are sent only when set so
    the server's defaults (own, all states, limit 50) apply otherwise."""
    params: dict[str, str] = {}
    if state is not None:
        params["state"] = state
    if active:
        params["active"] = "true"
    if all_tickets:
        params["all"] = "true"
    if limit is not None:
        params["limit"] = str(limit)
    return _common.call("GET", base_url, token, PATH_WORK_TICKET_PREFIX, params=params)


def _lookup_accessions(
    base_url: str,
    token: str,
    path: str,
    accessions: list[str],
    model_cls: type[BaseModel],
) -> tuple[dict[str, int], list[str]]:
    """POST a bulk lookup-by-accession route and return (resolved, missing).

    `model_cls` is the route's request Pydantic model (e.g.
    `BiosampleLookupByAccessionRequest`); it is constructed from the
    accession list and json-dumped so the route's wire validation is
    exercised. The biosample and study lookup routes share this shape.
    """
    body = model_cls(accessions=accessions).model_dump(mode="json")
    resp = _common.call("POST", base_url, token, path, json=body)
    return resp["resolved"], resp["missing"]


# ---------------------------------------------------------------------------
# argparse entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qiita", description="Qiita end-user CLI")
    _common.add_base_url_arg(parser)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_login = sub.add_parser(
        "login",
        help="AuthRocket LoginRocket Web flow with localhost loopback",
    )
    _common.add_token_file_arg(p_login)
    p_login.set_defaults(handler=_handle_login)

    p_whoami = sub.add_parser("whoami", help="Print the authenticated principal")
    p_whoami.set_defaults(handler=_handle_whoami)

    p_profile = sub.add_parser("profile", help="User profile operations")
    p_profile_sub = p_profile.add_subparsers(dest="profile_cmd", required=True)
    p_profile_set = p_profile_sub.add_parser(
        "set",
        help="Update affiliation / address / phone / orcid / mail prefs (PATCH /user/me)",
    )
    # All optional; argparse default None lets main() prune unset fields out
    # of the JSON body, matching the server's exclude_unset semantics.
    p_profile_set.add_argument("--affiliation")
    p_profile_set.add_argument("--address")
    p_profile_set.add_argument("--phone")
    p_profile_set.add_argument(
        "--orcid",
        help="ORCID iD (format NNNN-NNNN-NNNN-NNNX); server-side regex enforces shape",
    )
    p_profile_set.add_argument(
        "--receive-processing-emails",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Opt in (--receive-processing-emails) or out"
            " (--no-receive-processing-emails); omit to leave the current"
            " value unchanged"
        ),
    )
    p_profile_set.set_defaults(handler=_handle_profile_set)

    p_study = sub.add_parser("study", help="Study operations")
    p_study_sub = p_study.add_subparsers(dest="study_cmd", required=True)
    p_study_create = p_study_sub.add_parser(
        "create",
        help="Create a study owned by the calling principal (POST /study)",
    )
    p_study_create.add_argument("--title", required=True)
    p_study_create.add_argument("--alias")
    p_study_create.add_argument("--description")
    p_study_create.add_argument("--abstract")
    p_study_create.add_argument("--funding")
    p_study_create.add_argument("--ena-study-accession")
    p_study_create.add_argument("--bioproject-accession")
    p_study_create.add_argument("--notes")
    p_study_create.add_argument(
        "--principal-investigator-idx",
        type=int,
        help="principal_idx of the PI; must already exist as a user-kind principal",
    )
    p_study_create.add_argument(
        "--default-tier",
        choices=tuple(t.value for t in Tier),
        help="Default study_access tier; server defaults to 'member' when unset",
    )
    p_study_create.add_argument(
        "--extra-metadata",
        help="Free-form JSON object stored as JSONB",
    )
    p_study_create.set_defaults(handler=_handle_study_create)

    p_study_get = p_study_sub.add_parser(
        "get",
        help="Fetch a study by idx (GET /study/{study_idx})",
    )
    p_study_get.add_argument("--study-idx", type=int, required=True)
    p_study_get.set_defaults(
        handler=_handle_read,
        read_path=f"{PATH_STUDY_PREFIX}{PATH_STUDY_BY_IDX}",
        read_idx_arg="study_idx",
    )

    p_study_patch = p_study_sub.add_parser(
        "patch",
        help="Update editable study fields (PATCH /study/{study_idx})",
    )
    p_study_patch.add_argument("--study-idx", type=int, required=True)
    p_study_patch.add_argument("--title")
    p_study_patch.add_argument("--principal-investigator-idx", type=int)
    p_study_patch.add_argument("--alias")
    p_study_patch.add_argument("--description")
    p_study_patch.add_argument("--abstract")
    p_study_patch.add_argument("--funding")
    p_study_patch.add_argument("--ena-study-accession")
    p_study_patch.add_argument("--bioproject-accession")
    p_study_patch.add_argument("--notes")
    p_study_patch.add_argument("--extra-metadata", help="Free-form JSON object stored as JSONB")
    p_study_patch.set_defaults(
        handler=_handle_patch,
        patch_model=StudyPatchRequest,
        patch_path=f"{PATH_STUDY_PREFIX}{PATH_STUDY_BY_IDX}",
        patch_idx_arg="study_idx",
        patch_json_fields=("extra_metadata",),
    )

    p_biosample = sub.add_parser("biosample", help="Biosample operations")
    p_biosample_sub = p_biosample.add_subparsers(dest="biosample_cmd", required=True)
    p_biosample_create = p_biosample_sub.add_parser(
        "create",
        help="Create a biosample on a study (POST /study/{S}/biosample)",
    )
    p_biosample_create.add_argument("--study-idx", type=int, required=True)
    p_biosample_create.add_argument(
        "--owner-idx",
        type=int,
        help="principal_idx of the biosample's owner; defaults to the caller (resolved via whoami)",
    )
    p_biosample_create.add_argument(
        "--owner-biosample-id-field-name",
        required=True,
        help="display_name of the study's local field that carries the owner-biosample-id",
    )
    p_biosample_create.add_argument(
        "--owner-biosample-id-value",
        required=True,
        help="the owner-biosample-id value to record on this biosample",
    )
    p_biosample_create.add_argument(
        "--metadata",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help=(
            "Metadata entry; repeat for multiple. KEY is a biosample_global_field"
            " display_name; the route parses VALUE into the field's data type."
        ),
    )
    p_biosample_create.add_argument(
        "--metadata-checklist-name",
        help="Checklist name the biosample claims conformance to (e.g. ERC000015)",
    )
    p_biosample_create.add_argument(
        "--biosample-accession",
        help="External biosample accession (e.g. NCBI), if the biosample already has one",
    )
    p_biosample_create.add_argument(
        "--ena-sample-accession",
        help="ENA sample accession (ERS…), if the biosample already has one",
    )
    p_biosample_create.add_argument(
        "--matrix-tube-id",
        help="Matrix-tube identifier (digits only); validated server-side",
    )
    p_biosample_create.set_defaults(handler=_handle_biosample_create)

    p_biosample_get = p_biosample_sub.add_parser(
        "get",
        help="Fetch a biosample by idx (GET /biosample/{biosample_idx})",
    )
    p_biosample_get.add_argument("--biosample-idx", type=int, required=True)
    p_biosample_get.set_defaults(
        handler=_handle_read,
        read_path=f"{PATH_BIOSAMPLE_PREFIX}{PATH_BIOSAMPLE_BY_IDX}",
        read_idx_arg="biosample_idx",
    )

    p_biosample_list = p_biosample_sub.add_parser(
        "list-idxs",
        help="List biosample idxs in a study (GET /study/{S}/biosample/list-idxs)",
    )
    p_biosample_list.add_argument("--study-idx", type=int, required=True)
    p_biosample_list.set_defaults(
        handler=_handle_read,
        read_path=f"{PATH_STUDY_PREFIX}{PATH_BIOSAMPLE_LIST_BY_STUDY}",
        read_idx_arg="study_idx",
    )

    p_biosample_patch = p_biosample_sub.add_parser(
        "patch",
        help="Update editable biosample fields (PATCH /biosample/{biosample_idx})",
    )
    p_biosample_patch.add_argument("--biosample-idx", type=int, required=True)
    p_biosample_patch.add_argument(
        "--metadata-checklist-name",
        help="Checklist name the biosample claims conformance to (e.g. ERC000015)",
    )
    p_biosample_patch.add_argument("--owner-idx", type=int)
    p_biosample_patch.add_argument("--biosample-accession")
    p_biosample_patch.add_argument("--ena-sample-accession")
    p_biosample_patch.add_argument("--matrix-tube-id")
    p_biosample_patch.set_defaults(
        handler=_handle_patch,
        patch_model=BiosamplePatchRequest,
        patch_path=f"{PATH_BIOSAMPLE_PREFIX}{PATH_BIOSAMPLE_BY_IDX}",
        patch_idx_arg="biosample_idx",
        patch_json_fields=(),
    )

    p_seqrun = sub.add_parser("sequencing-run", help="Sequencing-run operations")
    p_seqrun_sub = p_seqrun.add_subparsers(dest="sequencing_run_cmd", required=True)
    p_seqrun_create = p_seqrun_sub.add_parser(
        "create",
        help="Create a sequencing-run row (POST /sequencing-run)",
    )
    p_seqrun_create.add_argument(
        "--instrument-run-id",
        required=True,
        help="Instrument-assigned run identifier; UNIQUE in the system",
    )
    p_seqrun_create.add_argument(
        "--platform",
        required=True,
        choices=tuple(p.value for p in Platform),
        help="Sequencing platform; values mirror ENA SRA platform names",
    )
    p_seqrun_create.add_argument("--instrument-model")
    p_seqrun_create.add_argument("--instrument-serial")
    p_seqrun_create.add_argument(
        "--run-performed-at",
        help="ISO-8601 timestamp with timezone, e.g. 2026-05-19T15:30:00Z",
    )
    p_seqrun_create.add_argument(
        "--extra-metadata",
        help="Free-form JSON object stored as JSONB",
    )
    p_seqrun_create.set_defaults(handler=_handle_sequencing_run_create)

    p_seqrun_get = p_seqrun_sub.add_parser(
        "get",
        help="Fetch a sequencing-run by idx (GET /sequencing-run/{idx})",
    )
    p_seqrun_get.add_argument("--sequencing-run-idx", type=int, required=True)
    p_seqrun_get.set_defaults(
        handler=_handle_read,
        read_path=f"{PATH_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCING_RUN_BY_IDX}",
        read_idx_arg="sequencing_run_idx",
    )

    p_seqrun_lookup = p_seqrun_sub.add_parser(
        "lookup",
        help=(
            "Resolve instrument_run_id(s) to sequencing_run idx"
            " (POST /sequencing-run/lookup-by-instrument-run-id)"
        ),
    )
    p_seqrun_lookup.add_argument(
        "--instrument-run-id",
        dest="instrument_run_ids",
        action="append",
        required=True,
        metavar="INSTRUMENT_RUN_ID",
        help="Instrument-assigned run id; repeat for a bulk lookup",
    )
    p_seqrun_lookup.set_defaults(handler=_handle_sequencing_run_lookup)

    p_seqpool = sub.add_parser("sequenced-pool", help="Sequenced-pool operations")
    p_seqpool_sub = p_seqpool.add_subparsers(dest="sequenced_pool_cmd", required=True)
    p_seqpool_create = p_seqpool_sub.add_parser(
        "create",
        help="Create a sequenced-pool on a run (POST /sequencing-run/{R}/sequenced-pool)",
    )
    p_seqpool_create.add_argument("--run-idx", type=int, required=True)
    p_seqpool_create.add_argument(
        "--run-preflight-blob",
        type=Path,
        dest="run_preflight_blob",
        help=(
            "Path to the local run-preflight file (typically a SQLite blob)."
            " The CLI reads it, base64-encodes the bytes, and sends them in"
            " the JSON body. Co-populated with --run-preflight-filename."
        ),
    )
    p_seqpool_create.add_argument(
        "--run-preflight-filename",
        dest="run_preflight_filename",
        help=(
            "Originating file name on disk (just the basename, e.g."
            " 'RunPreflight.db'); defaults to the basename of"
            " --run-preflight-blob when that flag was supplied"
        ),
    )
    p_seqpool_create.add_argument(
        "--extra-metadata",
        help="Free-form JSON object stored as JSONB",
    )
    p_seqpool_create.set_defaults(handler=_handle_sequenced_pool_create)

    p_seqsample = sub.add_parser("sequenced-sample", help="Sequenced-sample operations")
    p_seqsample_sub = p_seqsample.add_subparsers(dest="sequenced_sample_cmd", required=True)
    p_seqsample_create = p_seqsample_sub.add_parser(
        "create",
        help=(
            "Create a sequenced-sample under a pool"
            " (POST /sequencing-run/{R}/sequenced-pool/{P}/sequenced-sample)"
        ),
    )
    p_seqsample_create.add_argument("--run-idx", type=int, required=True)
    p_seqsample_create.add_argument("--pool-idx", type=int, required=True)
    p_seqsample_create.add_argument("--biosample-idx", type=int, required=True)
    p_seqsample_create.add_argument("--prep-protocol-idx", type=int, required=True)
    p_seqsample_create.add_argument(
        "--owner-idx",
        type=int,
        help=(
            "principal_idx of the prep_sample's owner; defaults to the caller (resolved via whoami)"
        ),
    )
    p_seqsample_create.add_argument(
        "--pool-item-id",
        dest="sequenced_pool_item_id",
        required=True,
        help=(
            "Per-pool unique item identifier (a well position or library"
            " barcode). MUST also be the filename prefix of every fastq this"
            " sample's fastq-to-parquet ticket processes: the control plane"
            " rejects a submission whose fastq basename does not start with"
            " this value."
        ),
    )
    p_seqsample_create.add_argument("--primary-study-idx", type=int, required=True)
    p_seqsample_create.add_argument(
        "--secondary-study-idx",
        dest="secondary_study_idxs",
        type=int,
        action="append",
        default=None,
        metavar="STUDY_IDX",
        help=(
            "Additional study this sequenced_sample is linked to."
            " Repeat for multiple; the primary owns metadata rows,"
            " secondaries read through the global field slot."
        ),
    )
    p_seqsample_create.add_argument(
        "--metadata",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help=(
            "Metadata entry; repeat for multiple. KEY is a prep_sample_global_field"
            " display_name; the composer parses VALUE into the field's data type."
        ),
    )
    p_seqsample_create.add_argument(
        "--metadata-checklist-name",
        help="Checklist name the sample claims conformance to (e.g. ERC000015)",
    )
    p_seqsample_create.add_argument(
        "--ena-experiment-accession",
        help="ENA experiment accession (ERX…), if this sample already has one",
    )
    p_seqsample_create.add_argument(
        "--ena-run-accession",
        help="ENA run accession (ERR…), if this sample already has one",
    )
    p_seqsample_create.set_defaults(handler=_handle_sequenced_sample_create)

    p_seqsample_patch = p_seqsample_sub.add_parser(
        "patch",
        help="Set a sequenced-sample's ENA accessions (PATCH /sequenced-sample/{idx})",
    )
    p_seqsample_patch.add_argument("--sequenced-sample-idx", type=int, required=True)
    p_seqsample_patch.add_argument("--ena-experiment-accession")
    p_seqsample_patch.add_argument("--ena-run-accession")
    p_seqsample_patch.set_defaults(
        handler=_handle_patch,
        patch_model=SequencedSamplePatchRequest,
        patch_path=f"{PATH_SEQUENCED_SAMPLE_PREFIX}{PATH_SEQUENCED_SAMPLE_BY_IDX}",
        patch_idx_arg="sequenced_sample_idx",
        patch_json_fields=(),
    )

    p_seqsample_list = p_seqsample_sub.add_parser(
        "list",
        help=(
            "List a run's sequenced-samples with their biosample linkage and"
            " ENA/biosample accessions (GET /sequencing-run/{R}/sequenced-sample/list)"
        ),
    )
    p_seqsample_list.add_argument("--sequencing-run-idx", type=int, required=True)
    p_seqsample_list.set_defaults(
        handler=_handle_read,
        read_path=f"{PATH_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_SAMPLE_LIST_BY_RUN_FULL}",
        read_idx_arg="sequencing_run_idx",
    )

    p_prepsample = sub.add_parser("prep-sample", help="Prep-sample operations")
    p_prepsample_sub = p_prepsample.add_subparsers(dest="prep_sample_cmd", required=True)
    p_prepsample_list_studies = p_prepsample_sub.add_parser(
        "list-studies",
        help=(
            "List the studies a prep-sample is linked to, with their accessions"
            " (GET /prep-sample/{idx}/study/list)"
        ),
    )
    p_prepsample_list_studies.add_argument("--prep-sample-idx", type=int, required=True)
    p_prepsample_list_studies.set_defaults(
        handler=_handle_read,
        read_path=f"{PATH_PREP_SAMPLE_PREFIX}{PATH_PREP_SAMPLE_STUDY_LIST}",
        read_idx_arg="prep_sample_idx",
    )

    p_ticket = sub.add_parser("ticket", help="Work-ticket operations")
    p_ticket_sub = p_ticket.add_subparsers(dest="ticket_cmd", required=True)
    p_ticket_submit = p_ticket_sub.add_parser(
        "submit",
        help="Submit a work-ticket for an action (POST /work-ticket)",
    )
    p_ticket_submit.add_argument("--action-id", required=True)
    p_ticket_submit.add_argument("--action-version", required=True)
    # Scope-target shape is a discriminated union; the smoke path is
    # prep_sample-scoped (fastq-to-parquet). --prep-sample-idx is the
    # convenience flag for that common case; --scope-target-json is the
    # escape hatch for non-prep_sample scope kinds. Exactly one is required.
    target_group = p_ticket_submit.add_mutually_exclusive_group(required=True)
    target_group.add_argument(
        "--prep-sample-idx",
        type=int,
        help=(
            "Submit a prep_sample-scoped ticket against this prep_sample_idx."
            " Constructs scope_target={kind:prep_sample, prep_sample_idx:N}."
        ),
    )
    target_group.add_argument(
        "--scope-target-json",
        help=(
            "Verbatim scope_target as a JSON object — escape hatch for"
            " non-prep_sample scope kinds (study_prep, reference)"
        ),
    )
    p_ticket_submit.add_argument(
        "--context-json",
        help=(
            "Action context as a JSON object (validated server-side against"
            " the action's context_schema). For fastq-to-parquet, paired-end:"
            ' \'{"fastq_path": "/abs/filename_prefix_R1.fastq",'
            ' "reverse_fastq_path": "/abs/filename_prefix_R2.fastq"}\' — each'
            " fastq basename must start with the sequenced-sample's"
            " --pool-item-id."
        ),
    )
    p_ticket_submit.add_argument(
        "--mem-gb",
        type=int,
        help=(
            "Per-run memory floor (GB) for this ticket's SLURM steps: raises any"
            " step whose baseline is below it, bounded by the action's mem"
            " ceiling. Requires wet_lab_admin / system_admin; omit to use each"
            " step's workflow default."
        ),
    )
    p_ticket_submit.set_defaults(handler=_handle_ticket_submit)

    p_ticket_status = p_ticket_sub.add_parser(
        "status",
        help="Read a work-ticket's status (GET /work-ticket/{idx})",
    )
    p_ticket_status.add_argument(
        "work_ticket_idx",
        type=int,
        help="Work-ticket idx returned by `qiita ticket submit`.",
    )
    p_ticket_status.set_defaults(handler=_handle_ticket_status)

    p_ticket_run = p_ticket_sub.add_parser(
        "run",
        help="Resume/retry a work-ticket — reset a FAILED ticket and re-dispatch, "
        "skipping already-completed steps (POST /work-ticket/{idx}/run)",
    )
    p_ticket_run.add_argument(
        "work_ticket_idx",
        type=int,
        help="Work-ticket idx to resume (from `qiita ticket submit` / `status`).",
    )
    p_ticket_run.set_defaults(handler=_handle_ticket_run)

    p_ticket_list = p_ticket_sub.add_parser(
        "list",
        help="List work-tickets with their current compute placement (GET /work-ticket)",
    )
    p_ticket_list.add_argument(
        "--state",
        choices=[s.value for s in WorkTicketState],
        help="Filter to a single lifecycle state.",
    )
    p_ticket_list.add_argument(
        "--active",
        action="store_true",
        help="Only non-terminal tickets (pending / queued / processing).",
    )
    p_ticket_list.add_argument(
        "--all",
        dest="all_tickets",
        action="store_true",
        help="All originators' tickets (requires wet_lab_admin+); default is your own.",
    )
    p_ticket_list.add_argument(
        "--limit",
        type=int,
        help="Max tickets to return (server default 50, max 500).",
    )
    p_ticket_list.set_defaults(handler=_handle_ticket_list)

    p_ticket_logs = p_ticket_sub.add_parser(
        "logs",
        help="Read a step attempt's stdout/stderr tail "
        "(GET /work-ticket/{idx}/step/{step_index}/logs)",
    )
    p_ticket_logs.add_argument(
        "work_ticket_idx",
        type=int,
        help="Work-ticket idx returned by `qiita ticket submit`.",
    )
    p_ticket_logs.add_argument(
        "--step-index",
        type=int,
        required=True,
        help="0-based index of the step in the action's steps: list.",
    )
    p_ticket_logs.add_argument(
        "--attempt",
        type=int,
        help="Step attempt to read (default: the latest recorded attempt).",
    )
    p_ticket_logs.add_argument(
        "--tail-lines",
        type=int,
        help="Max lines of each stream to return (server default 200, max 5000).",
    )
    p_ticket_logs.set_defaults(handler=_handle_ticket_logs)

    p_reference = sub.add_parser("reference", help="Reference-data lifecycle operations")
    p_reference_sub = p_reference.add_subparsers(dest="reference_cmd", required=True)
    p_reference_load = p_reference_sub.add_parser(
        "load",
        help=("Upload FASTA + optional inputs and run the reference-add workflow end-to-end"),
    )
    # Reference selection — XOR enforced inside the handler so the help
    # output reads cleanly; argparse's mutually_exclusive_group can't
    # express "either A+B together, or C alone."
    p_reference_load.add_argument("--name", help="New reference name (paired with --version)")
    p_reference_load.add_argument("--version", help="New reference version (paired with --name)")
    p_reference_load.add_argument(
        "--kind",
        default="sequence_reference",
        choices=("sequence_reference", "taxonomy_authority", "artifact_sequence_set"),
        help="Reference kind for newly-created references (default: sequence_reference)",
    )
    p_reference_load.add_argument(
        "--reference-idx",
        type=int,
        help="Bind to an existing reference instead of creating one",
    )
    p_reference_load.add_argument(
        "--host",
        action="store_true",
        help=(
            "Mark the reference as a host (is_host=true) and run host-reference-add,"
            " which builds the rype + minimap2 host-filter indexes. Requires --taxonomy."
        ),
    )
    # FASTA source: --fasta (remote DoPut upload) XOR --fasta-manifest (--local
    # by-path). Neither is argparse-required because exactly which one applies
    # depends on --local; the entry point enforces the XOR and the
    # per-mode requirement with clear messages.
    p_reference_load.add_argument(
        "--fasta",
        type=Path,
        help="Single FASTA to stream over DoPut (remote ingest; omit under --local)",
    )
    p_reference_load.add_argument(
        "--local",
        action="store_true",
        help=(
            "Ingest FASTA by path instead of DoPut: stage the files listed in"
            " --fasta-manifest (and pass companions as raw paths) to the"
            " local-(host-)reference-add workflow. No --data-plane-url needed."
        ),
    )
    p_reference_load.add_argument(
        "--fasta-manifest",
        type=Path,
        dest="fasta_manifest",
        help=(
            "Under --local: absolute path to a manifest listing one absolute"
            " FASTA path per line (blank lines and `#` comments ignored)."
        ),
    )
    p_reference_load.add_argument("--taxonomy", type=Path)
    p_reference_load.add_argument("--tree", type=Path)
    p_reference_load.add_argument("--jplace", type=Path)
    p_reference_load.add_argument("--genome-map", type=Path, dest="genome_map")
    # Host index selection + build params (apply only with --host). Default
    # builds both indexes; the opt-out flags skip one (not both — the entry
    # point rejects building neither). --rype-w / --minimap2-preset tune the
    # builders; omitted, they use the builders' defaults (w=20, preset=sr).
    p_reference_load.add_argument(
        "--no-rype-index",
        action="store_true",
        help=(
            "With --host: skip the rype index (build minimap2 only). Cannot be"
            " combined with --no-minimap2-index."
        ),
    )
    p_reference_load.add_argument(
        "--no-minimap2-index",
        action="store_true",
        help=(
            "With --host: skip the minimap2 index (build rype only). Cannot be"
            " combined with --no-rype-index."
        ),
    )
    p_reference_load.add_argument(
        "--rype-w",
        type=int,
        help="With --host: rype minimizer window `w` for the rype index build (default 20).",
    )
    p_reference_load.add_argument(
        "--minimap2-preset",
        choices=("sr", "map-ont", "map-pb", "map-hifi", "asm5", "asm10", "asm20"),
        help="With --host: minimap2 preset baked into the .mmi index (default sr).",
    )
    p_reference_load.add_argument(
        "--data-plane-url",
        help=(
            "gRPC URL of the data plane (e.g. grpc://qiita-data.example.com:50051)."
            " Required for remote ingest; ignored (and optional) under --local."
        ),
    )
    p_reference_load.add_argument(
        "--no-watch",
        action="store_true",
        help="Submit the work_ticket and exit without polling. Default polls until terminal.",
    )
    p_reference_load.add_argument(
        "--poll-interval",
        type=float,
        default=2.0,
        help="Seconds between work_ticket polls under --watch (default: 2.0)",
    )
    p_reference_load.add_argument(
        "--timeout",
        type=float,
        default=24 * 3600,
        help="Max seconds to wait for the work_ticket under --watch (default: 86400)",
    )
    p_reference_load.add_argument(
        "--mem-gb",
        type=int,
        help=(
            "Per-run memory floor (GB) for the workflow's SLURM steps: raises any"
            " step whose baseline is below it, bounded by the action's mem"
            " ceiling. Use for a genome-scale reference (e.g. a human host"
            " genome) that OOMs the conservative default. Requires"
            " wet_lab_admin / system_admin."
        ),
    )
    p_reference_load.set_defaults(handler=_handle_reference_load)

    p_submit_bcl = sub.add_parser(
        "submit-bcl-convert",
        help=(
            "Bundled operator gesture for the bcl-convert workflow: mint"
            " (or reuse) a sequencing-run row, attach a sequenced-pool"
            " with the preflight blob, and submit the work-ticket against"
            " the pool."
        ),
        description=(
            "Submit a bcl-convert work-ticket end-to-end. The instrument run"
            " ID and model are read from the --bcl-input-dir's RunInfo.xml and"
            " the serial number resolved against the vendored Illumina prefix"
            " table (qiita_common.illumina); serial numbers from unsupported"
            " families"
            " (HiSeq1500, HiSeq3000, NextSeq, NovaSeqXPlus) and from PacBio"
            " fail-fast before any server round-trip. All three server-side"
            " calls are find-or-create on their natural keys, so a re-run"
            " after a partial failure converges on the existing rows without"
            " operator cleanup."
        ),
    )
    p_submit_bcl.add_argument(
        "--bcl-input-dir",
        type=Path,
        required=True,
        help=(
            "Absolute path to the Illumina BCL run folder; it must contain a"
            " top-level RunInfo.xml so the reader can derive the"
            " instrument_run_id + instrument_model. This same path is passed"
            " through as action_context.bcl_input_dir on the resulting"
            " work-ticket; the orchestrator binds its parent directory into"
            " the bcl-convert container at submit time."
        ),
    )
    p_submit_bcl.add_argument(
        "--preflight-blob",
        type=Path,
        required=True,
        help=(
            "Path to the local kl-run-preflight SQLite file. The CLI reads"
            " it (refuses empty), base64-encodes the bytes, and attaches the"
            " blob to the sequenced-pool row; the file basename becomes"
            " run_preflight_filename, which serves as the find-or-create key"
            " for the pool POST."
        ),
    )
    p_submit_bcl.add_argument(
        "--prep-protocol-idx",
        type=int,
        required=True,
        help=(
            "Qiita prep_protocol_idx to FK every per-sample row to. Today"
            " applied uniformly across the whole pool because the preflight"
            " does not carry a Qiita prep_protocol identifier; a future"
            " preflight column may let this flag come out of the file like"
            " the per-row study_idx already does (project.qiita_id)."
        ),
    )
    p_submit_bcl.add_argument(
        "--host-rype-reference-idx",
        type=int,
        default=None,
        help=(
            "ACTIVE host reference_idx whose rype (.ryxdi) index every"
            " human_filtering sample is depleted against. REQUIRED when any"
            " preflight sample has human_filtering set; recorded per sample so the"
            " later pool fan-out host-filters exactly those samples (preflight"
            " human_filtering=0 samples get no host reference and are not"
            " filtered). Checked ACTIVE + carrying a rype index up front."
        ),
    )
    p_submit_bcl.add_argument(
        "--host-minimap2-reference-idx",
        type=int,
        default=None,
        help=(
            "Optional ACTIVE host reference_idx whose minimap2 (.mmi) index drives"
            " the second host-filter pass for human_filtering samples. Requires"
            " --host-rype-reference-idx. Omit for a rype-only host filter."
        ),
    )
    p_submit_bcl.set_defaults(handler=_handle_submit_bcl_convert)

    p_delete_pool = sub.add_parser(
        "delete-sequenced-pool",
        help=(
            "Hard-delete a full sequenced-pool (one bcl-convert sample"
            " sheet's worth of samples) and everything under it. Admin only."
        ),
        description=(
            "Fully purge a sequenced_pool: the pool row plus every"
            " sequenced-sample / prep-sample under it, their metadata, study"
            " links, and pool-/sample-scoped work tickets. The parent"
            " sequencing-run and the underlying biosamples are retained."
            " Because each prep-sample is exclusive to this pool, deleting it"
            " removes those samples from EVERY study they link to, not only"
            " one. Requires system_admin (sequenced_pool:delete). In-flight"
            " work tickets block the delete unconditionally; completed/failed"
            " tickets, published prep-samples, and ENA-submitted samples block"
            " it unless --force is passed."
        ),
    )
    p_delete_pool.add_argument(
        "--sequencing-run-idx",
        type=int,
        required=True,
        help="The parent sequencing_run_idx the pool belongs to.",
    )
    p_delete_pool.add_argument(
        "--sequenced-pool-idx",
        type=int,
        required=True,
        help="The sequenced_pool_idx to purge.",
    )
    p_delete_pool.add_argument(
        "--force",
        action="store_true",
        help=(
            "Override the soft blocks: delete even when completed/failed work"
            " tickets reference the pool, prep-samples are published into a"
            " study, or samples carry an ENA accession. Does NOT override"
            " in-flight work tickets."
        ),
    )
    p_delete_pool.set_defaults(handler=_handle_delete_sequenced_pool)

    p_submit_hf = sub.add_parser(
        "submit-host-filter-pool",
        help=(
            "Bundled operator gesture: after a bcl-convert pool finishes, fan"
            " out one host-filtered fastq-to-parquet ticket per sample in the"
            " pool."
        ),
        description=(
            "For every active sequenced_sample in --sequenced-pool-idx, locate"
            " its per-sample FASTQ(s) under --convert-dir (matched on the"
            " sequenced_pool_item_id == bcl-convert Sample_ID prefix) and submit"
            " a fastq-to-parquet/1.2.0 work-ticket: always-on QC (fastp-equivalent"
            " adapter/polyG/length trimming) followed by host filtering against"
            " --host-rype-reference-idx (the rype index, required) and, when"
            " given, --host-minimap2-reference-idx (the minimap2 index). Each host"
            " reference is checked for ACTIVE status + its required index up front,"
            " and every sample's FASTQs are resolved before any ticket is"
            " submitted, so a misconfiguration aborts with zero side effects. The"
            " run's instrument_model is read once (GET /sequencing-run) and"
            " forwarded per sample so QC's polyG step is gated correctly."
            " Re-running after a partial failure is safe: disallow-without-delete"
            " is keyed per prep_sample, so only samples without an"
            " in-flight/terminal ticket are re-submitted. ASSUMPTIONS:"
            " --convert-dir must be visible from this host (the per-sample FASTQs"
            " are read off the shared compute filesystem), and the run is"
            " single-lane (a sample with >1 R1 file, e.g. lane-split _L001/_L002,"
            " is rejected — fastq-to-parquet takes a single fastq_path)."
        ),
    )
    p_submit_hf.add_argument(
        "--sequencing-run-idx",
        type=int,
        required=True,
        help="sequencing_run_idx the pool belongs to (the route checks pool↔run).",
    )
    p_submit_hf.add_argument(
        "--sequenced-pool-idx",
        type=int,
        required=True,
        help="sequenced_pool_idx whose samples to fan out over.",
    )
    p_submit_hf.add_argument(
        "--host-rype-reference-idx",
        type=int,
        required=True,
        help=(
            "ACTIVE host reference_idx whose rype (.ryxdi) index drives the first"
            " host-filter pass. Required."
        ),
    )
    p_submit_hf.add_argument(
        "--host-minimap2-reference-idx",
        type=int,
        default=None,
        help=(
            "Optional ACTIVE host reference_idx whose minimap2 (.mmi) index drives"
            " the second host-filter pass (on the rype survivors). Omit for a"
            " rype-only host filter."
        ),
    )
    p_submit_hf.add_argument(
        "--convert-dir",
        type=Path,
        required=True,
        help=(
            "Absolute path to the bcl-convert ConvertJob output directory,"
            " visible from this host. Searched recursively for each sample's"
            " <pool_item_id>_*_R1_*.fastq.gz (and _R2_ when paired-end), since"
            " bcl-convert nests per-sample FASTQs under a Sample_Project subdir."
        ),
    )
    p_submit_hf.set_defaults(handler=_handle_submit_host_filter_pool)

    return parser


# ---------------------------------------------------------------------------
# Subcommand handlers (registered via parser.set_defaults(handler=...))
# ---------------------------------------------------------------------------


def _handle_login(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.do_login(
        base_url=args.base_url,
        token_file=args.token_file,
        cli_command="qiita login",
    )


def _handle_whoami(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(lambda t: _common.whoami(args.base_url, t))


def _handle_profile_set(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    updates = _build_body(UserUpdate, args, parser)
    if not updates:
        parser.error(
            "qiita profile set requires at least one of"
            " --affiliation / --address / --phone / --orcid /"
            " --receive-processing-emails / --no-receive-processing-emails"
        )
    return _common.run_http_subcommand(lambda t: _patch_user_me(args.base_url, t, updates))


def _handle_read(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Fetch a resource by idx (GET) and print its JSON body.

    The per-command `set_defaults` supplies `read_path` (a subpath
    template) and `read_idx_arg` (the namespace attr whose value fills
    the template), so the path formats from exactly one identifier.
    """
    idx_arg = args.read_idx_arg
    path = args.read_path.format(**{idx_arg: getattr(args, idx_arg)})
    return _common.run_http_subcommand(lambda t: _common.call("GET", args.base_url, t, path))


def _handle_patch(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Apply a partial update to a resource under optimistic concurrency.

    The per-command `set_defaults` supplies `patch_model` (the
    PatchRequestModel subclass the flags map to), `patch_path` (a subpath
    template), `patch_idx_arg` (the namespace attr that fills it), and
    `patch_json_fields` (flags parsed from JSON before validation). An
    empty update (no field flags) fails the model's at-least-one-field
    rule and exits 2.
    """
    for field in args.patch_json_fields:
        setattr(
            args,
            field,
            _common.parse_json_arg(
                getattr(args, field), parser, flag=f"--{field.replace('_', '-')}"
            ),
        )
    body = _build_body(args.patch_model, args, parser)
    idx_arg = args.patch_idx_arg
    path = args.patch_path.format(**{idx_arg: getattr(args, idx_arg)})
    return _common.run_http_subcommand(
        lambda t: _common.patch_with_if_match(args.base_url, t, path, body)
    )


def _handle_study_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a study owned by the caller. --extra-metadata is parsed from
    JSON before Pydantic validation so a malformed paste surfaces as a
    clean argparse exit 2."""
    args.extra_metadata = _common.parse_json_arg(
        args.extra_metadata, parser, flag="--extra-metadata"
    )
    body = _build_body(StudyCreate, args, parser)
    return _common.run_http_subcommand(lambda t: _post_study(args.base_url, t, body))


def _handle_biosample_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a biosample on a study, auto-defaulting owner_idx to the caller.

    The whoami lookup that resolves a missing --owner-idx shares the
    token-read path with the POST itself, so both calls happen inside the
    same `run_http_subcommand` invocation. Body construction runs after
    that resolution so BiosampleImportRequest sees a populated owner_idx.
    """
    args.metadata = _common.parse_kv_pairs(args.metadata, parser, flag="--metadata")

    def _run(token: str) -> dict:
        _common.resolve_owner_idx(args, args.base_url, token)
        body = _build_body(BiosampleImportRequest, args, parser)
        return _post_biosample(args.base_url, token, args.study_idx, body)

    return _common.run_http_subcommand(_run)


def _handle_sequencing_run_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a sequencing-run row. --extra-metadata is parsed from JSON
    before Pydantic validation so a malformed paste surfaces as a clean
    argparse exit 2."""
    args.extra_metadata = _common.parse_json_arg(
        args.extra_metadata, parser, flag="--extra-metadata"
    )
    body = _build_body(SequencingRunCreateRequest, args, parser)
    return _common.run_http_subcommand(lambda t: _post_sequencing_run(args.base_url, t, body))


def _handle_sequencing_run_lookup(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Resolve instrument_run_id(s) to sequencing_run idx via the bulk lookup.

    Prints the {resolved, missing} mapping.
    """
    body = {"instrument_run_ids": args.instrument_run_ids}
    path = f"{PATH_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID}"
    return _common.run_http_subcommand(
        lambda t: _common.call("POST", args.base_url, t, path, json=body)
    )


def _handle_sequenced_pool_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a sequenced-pool under a sequencing-run.

    The run-preflight is an optional co-populated pair. When
    --run-preflight-blob is supplied: read the file, refuse-on-empty,
    and default --run-preflight-filename to the file's basename when
    the user didn't pass one. When --run-preflight-filename is supplied
    without --run-preflight-blob: refuse before the server round-trip.
    Otherwise both flags are absent and the pool is created with no
    preflight, which is the optional-pair case the schema permits.
    """
    if args.run_preflight_blob is not None:
        blob_path: Path = args.run_preflight_blob
        if not blob_path.is_file():
            parser.error(f"--run-preflight-blob {blob_path} is not a regular file")
        blob_bytes = blob_path.read_bytes()
        if not blob_bytes:
            parser.error(f"--run-preflight-blob {blob_path} is empty")
        # Pydantic's Base64Bytes interprets input as an *already* base64-
        # encoded string and decodes it. We base64-encode here so the model
        # round-trips back to the same raw bytes; mode="json" then re-encodes
        # to the canonical base64 string for the wire.
        args.run_preflight_blob = base64.b64encode(blob_bytes).decode("ascii")
        if args.run_preflight_filename is None:
            args.run_preflight_filename = blob_path.name
    elif args.run_preflight_filename is not None:
        parser.error(
            "--run-preflight-filename supplied without --run-preflight-blob;"
            " the preflight pair must be both or neither"
        )

    args.extra_metadata = _common.parse_json_arg(
        args.extra_metadata, parser, flag="--extra-metadata"
    )
    body = _build_body(SequencedPoolCreateRequest, args, parser)
    return _common.run_http_subcommand(
        lambda t: _post_sequenced_pool(args.base_url, t, args.run_idx, body)
    )


def _handle_sequenced_sample_create(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """Mint a sequenced_sample composer atomically with its study links and
    metadata. --owner-idx auto-defaults to the caller via whoami when omitted;
    --metadata KEY=VALUE entries collect via the shared parse_kv_pairs helper.
    """
    args.metadata = _common.parse_kv_pairs(args.metadata, parser, flag="--metadata")

    def _run(token: str) -> dict:
        _common.resolve_owner_idx(args, args.base_url, token)
        body = _build_body(SequencedSampleCreateRequest, args, parser)
        return _post_sequenced_sample(args.base_url, token, args.run_idx, args.pool_idx, body)

    return _common.run_http_subcommand(_run)


def _handle_ticket_submit(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Submit a work-ticket. Construct the scope_target from the convenience
    --prep-sample-idx flag when supplied; otherwise parse --scope-target-json
    verbatim. action_context comes from --context-json (or stays {} when
    omitted). All JSON parsing flows through parse_json_arg so a malformed
    paste lands as a clean exit 2.
    """
    if args.prep_sample_idx is not None:
        args.scope_target = {
            "kind": ScopeTargetKind.PREP_SAMPLE.value,
            "prep_sample_idx": args.prep_sample_idx,
        }
    else:
        args.scope_target = _common.parse_json_arg(
            args.scope_target_json, parser, flag="--scope-target-json"
        )
    parsed_context = _common.parse_json_arg(args.context_json, parser, flag="--context-json")
    # action_context defaults to {} server-side via the model; only set it
    # when the user supplied --context-json so unset stays "not set".
    if parsed_context is not None:
        args.action_context = parsed_context

    # Map the convenience --mem-gb flag onto the resource_override model field
    # _build_body picks up. Only set it when supplied so unset stays "not set".
    if args.mem_gb is not None:
        args.resource_override = {"mem_gb": args.mem_gb}

    body = _build_body(WorkTicketCreateRequest, args, parser)
    return _common.run_http_subcommand(lambda t: _post_work_ticket(args.base_url, t, body))


def _handle_ticket_status(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """GET a single work-ticket's full record. The CLI prints the full
    server response so a USER polling for state can render every field
    (state, retry_count, failure_*, timestamps) without a second call."""
    return _common.run_http_subcommand(
        lambda t: _get_work_ticket(args.base_url, t, args.work_ticket_idx)
    )


def _handle_ticket_run(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Resume/retry a work-ticket: reset a FAILED ticket and re-dispatch (the
    only retry path — no auto-retry worker). The runner fast-forwards every
    already-COMPLETED step, so an expensive finished step is not re-run; the
    ticket resumes at the first incomplete step. Prints the new {idx, state}."""
    return _common.run_http_subcommand(
        lambda t: _run_work_ticket(args.base_url, t, args.work_ticket_idx)
    )


def _handle_ticket_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List work-tickets (your own by default, or every originator's with
    --all for wet_lab_admin+). Prints the full summary list so a poller sees
    each ticket's state plus its current step's compute_target / slurm_job_id
    / step_state in one call."""
    return _common.run_http_subcommand(
        lambda t: _list_work_tickets(
            args.base_url,
            t,
            state=args.state,
            active=args.active,
            all_tickets=args.all_tickets,
            limit=args.limit,
        )
    )


def _render_ticket_logs(body: dict) -> None:
    """Print a step's logs for human reading: a one-line header, then each
    stream raw (so embedded newlines render as newlines, not the JSON-escaped
    `\\n` literals a plain dump would show — the whole point of a logs verb)."""
    print(
        f"# work_ticket {body['work_ticket_idx']} step {body['step_index']}"
        f" attempt {body['attempt']} ({body['step_name']})"
    )
    for stream in ("stdout", "stderr"):
        suffix = " (truncated)" if body.get(f"{stream}_truncated") else ""
        print(f"\n===== {stream}{suffix} =====")
        text = body.get(stream, "")
        if text:
            print(text)


def _handle_ticket_logs(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """GET a step attempt's stdout/stderr tail so an operator can diagnose a
    failure (OOM, bad input, contract violation) without a host shell. Renders
    each stream raw (not JSON-escaped); defaults to the latest recorded attempt
    when --attempt is omitted."""
    return _common.run_http_subcommand(
        lambda t: _get_work_ticket_step_logs(
            args.base_url,
            t,
            args.work_ticket_idx,
            step_index=args.step_index,
            attempt=args.attempt,
            tail_lines=args.tail_lines,
        ),
        render=_render_ticket_logs,
    )


async def _run_reference_load(
    *,
    base_url: str,
    token: str,
    data_plane_url: str | None,
    args: argparse.Namespace,
) -> dict:
    """Construct real httpx + (for remote ingest) pyarrow.flight clients and
    drive `do_reference_load`. Lives next to the CLI handler so the handler
    stays a thin argparse → entry-point shim; the entry point itself
    (in cli.reference_load) takes injected clients so tests bypass this
    function entirely.

    Under `--local` no bytes cross the wire, so no Flight client is built and
    `--data-plane-url` is not needed; the by-path manifest + companions ride in
    action_context. The remote path requires `--data-plane-url`."""
    import httpx as _httpx

    from .reference_load import do_reference_load

    # Shared keyword args for both ingest modes.
    common_kwargs: dict[str, Any] = dict(
        token=token,
        local=args.local,
        fasta_path=args.fasta,
        fasta_manifest_path=args.fasta_manifest,
        name=args.name,
        version=args.version,
        kind=args.kind,
        host=args.host,
        reference_idx=args.reference_idx,
        taxonomy_path=args.taxonomy,
        tree_path=args.tree,
        jplace_path=args.jplace,
        genome_map_path=args.genome_map,
        build_rype=not args.no_rype_index,
        build_minimap2=not args.no_minimap2_index,
        rype_w=args.rype_w,
        minimap2_preset=args.minimap2_preset,
        watch=not args.no_watch,
        poll_interval_seconds=args.poll_interval,
        timeout_seconds=args.timeout,
        mem_gb=args.mem_gb,
    )

    if args.local:
        async with _httpx.AsyncClient(
            base_url=base_url, timeout=_common.CLI_HTTP_TIMEOUT_SECONDS
        ) as http:
            return await do_reference_load(http=http, flight_client=None, **common_kwargs)

    if not data_plane_url:
        raise ValueError("--data-plane-url is required for remote ingest (or use --local)")

    import pyarrow.flight as flight

    flight_client = flight.FlightClient(data_plane_url)
    try:
        async with _httpx.AsyncClient(
            base_url=base_url, timeout=_common.CLI_HTTP_TIMEOUT_SECONDS
        ) as http:
            return await do_reference_load(http=http, flight_client=flight_client, **common_kwargs)
    finally:
        flight_client.close()


def _handle_reference_load(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Entry point for `qiita reference load`. Reads the PAT, builds
    a real httpx + flight client, and calls `do_reference_load`. Maps
    every known failure shape to exit 1 with a one-line stderr message —
    no silent retry, no buried traceback. Terminal work_ticket=failed
    also exits 1 so callers wrapping this in a Makefile / CI step get
    the build break."""
    import httpx as _httpx
    import pyarrow.flight as _flight

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    try:
        result = asyncio.run(
            _run_reference_load(
                base_url=args.base_url,
                token=token,
                data_plane_url=args.data_plane_url,
                args=args,
            )
        )
    except _httpx.HTTPStatusError as exc:
        print(
            f"http error {exc.response.status_code}: {exc.response.text}",
            file=sys.stderr,
        )
        return 1
    except _flight.FlightError as exc:
        # Catch the gRPC-level error explicitly so the operator sees a
        # formatted error line instead of a raw traceback. FlightError is
        # NOT a RuntimeError subclass, so the catch-all below would miss
        # it. Common shapes: network refused, expired ticket, DP
        # rejected the stream mid-write.
        print(f"flight error: {exc}", file=sys.stderr)
        return 1
    except (RuntimeError, ValueError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    # Terminal work_ticket=failed under --watch surfaces as exit 1, not
    # exit 0 — a CI step wrapping this CLI must distinguish a successful
    # reference build from a failed one. The JSON body still goes to
    # stdout so the caller can see the failure_reason.
    work_ticket = result.get("work_ticket") or {}
    final_state = work_ticket.get("state")
    print(json.dumps(_serializable(result), indent=2))
    if final_state == "failed":
        return 1
    return 0


def _serializable(obj):
    """Recursively replace Pydantic / Path values with their JSON form so
    `json.dumps` succeeds on the result dict (which carries upload-idx
    metadata + the final work_ticket body)."""
    from pathlib import Path as _Path

    if isinstance(obj, dict):
        return {k: _serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serializable(v) for v in obj]
    if isinstance(obj, _Path):
        return str(obj)
    return obj


def _read_preflight_rows(
    preflight_blob: Path, parser: argparse.ArgumentParser
) -> list[_PreflightRow]:
    """Open the preflight SQLite and return one `_PreflightRow` per illumina_sample row.

    Errors that the operator can fix (file not a SQLite, library raises
    on a malformed row, a row missing biosample_accession or
    primary_project_accession) raise via parser.error so the CLI
    surfaces a single stderr line and exits 2 before any network call.
    """
    # get_illumina_sample_info / open_db_file are top-level run_preflight exports;
    # get_illumina_sample_rows lives in the run_preflight.db submodule (NOT
    # re-exported at the top level), so it is reached through `db`.
    from run_preflight import db as run_preflight_db  # noqa: PLC0415
    from run_preflight import get_illumina_sample_info, open_db_file  # noqa: PLC0415

    try:
        conn = open_db_file(preflight_blob)
    except sqlite3.DatabaseError as exc:
        parser.error(f"--preflight-blob {preflight_blob}: not a readable SQLite file: {exc}")
    try:
        illumina_samples = get_illumina_sample_info(conn)
        # The library exposes no per-sample human_filtering accessor, so map each
        # illumina_sample to its effective project (get_illumina_sample_rows,
        # do_not_use-excluded like get_illumina_sample_info) and read that
        # project's human_filtering flag from the preflight directly.
        project_by_idx = {row[0]: row[4] for row in run_preflight_db.get_illumina_sample_rows(conn)}
        filtering_by_project = {
            name: bool(flag)
            for name, flag in conn.execute(
                "SELECT project_name, human_filtering FROM project"
            ).fetchall()
        }
    except (sqlite3.DatabaseError, ValueError) as exc:
        parser.error(
            f"--preflight-blob {preflight_blob}: preflight query failed ({exc});"
            " verify the file is a kl-run-preflight SQLite"
        )
    finally:
        conn.close()

    if not illumina_samples:
        parser.error(
            f"--preflight-blob {preflight_blob} contains no illumina_sample rows;"
            " a bcl-convert submission needs at least one sample to demultiplex"
        )

    parsed: list[_PreflightRow] = []
    for illumina_sample_idx, biosample_accession, primary, secondary in illumina_samples:
        if not biosample_accession:
            parser.error(
                f"--preflight-blob {preflight_blob}: illumina_sample_idx"
                f" {illumina_sample_idx} carries no biosample_accession; populate"
                " upstream before re-submitting"
            )
        if not primary:
            parser.error(
                f"--preflight-blob {preflight_blob}: illumina_sample_idx"
                f" {illumina_sample_idx} carries no primary_project_accession;"
                " populate upstream before re-submitting"
            )
        project_name = project_by_idx.get(illumina_sample_idx)
        if project_name not in filtering_by_project:
            parser.error(
                f"--preflight-blob {preflight_blob}: illumina_sample_idx"
                f" {illumina_sample_idx} maps to no project with a human_filtering"
                " flag; verify the file is a kl-run-preflight SQLite"
            )
        parsed.append(
            _PreflightRow(
                illumina_sample_idx=int(illumina_sample_idx),
                biosample_accession=biosample_accession,
                primary_project_accession=primary,
                secondary_project_accessions=list(secondary),
                human_filtering=filtering_by_project[project_name],
            )
        )
    return parsed


def _build_missing_section(
    *,
    label: str,
    missing: list[str],
    preflight_rows: list[_PreflightRow],
    row_accessions: Callable[[_PreflightRow], list[str]],
) -> str | None:
    """Build one labeled section naming every preflight row that carries
    a missing accession in this class. Returns None if `missing` is empty.

    `row_accessions` extracts the row's accessions in the relevant class
    (one for biosamples, primary + secondaries for studies). The header
    counts distinct missing accessions and the rows affected, so the
    per-row bullet count is no longer ambiguous against the dedup count.
    """
    if not missing:
        return None
    missing_set = set(missing)
    bullets: list[str] = []
    for row in preflight_rows:
        row_misses = [a for a in row_accessions(row) if a in missing_set]
        if row_misses:
            bullets.append(
                f"  - {', '.join(row_misses)} (illumina_sample_idx={row.illumina_sample_idx})"
            )
    acc_plural = "s" if len(missing) != 1 else ""
    rows_plural = "s" if len(bullets) != 1 else ""
    return (
        f"{len(missing)} distinct preflight {label} accession{acc_plural}"
        f" not found in qiita, affecting {len(bullets)} illumina_sample row{rows_plural}:\n"
        + "\n".join(bullets)
    )


def _print_missing_accession_error(
    preflight_rows: list[_PreflightRow],
    missing_biosamples: list[str],
    missing_studies: list[str],
) -> None:
    """Emit one combined stderr block naming every offending preflight row.

    Each present class (biosample, study) gets its own header + bullet
    list, built by `_build_missing_section`.
    """
    sections = [
        s
        for s in (
            _build_missing_section(
                label="biosample",
                missing=missing_biosamples,
                preflight_rows=preflight_rows,
                row_accessions=lambda row: [row.biosample_accession],
            ),
            _build_missing_section(
                label="study",
                missing=missing_studies,
                preflight_rows=preflight_rows,
                row_accessions=lambda row: [
                    row.primary_project_accession,
                    *row.secondary_project_accessions,
                ],
            ),
        )
        if s is not None
    ]
    print(
        "error: " + "\n".join(sections) + "\nimport the missing record(s) and re-run.",
        file=sys.stderr,
    )


def _handle_submit_bcl_convert(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Bundle the bcl-convert submission flow into one operator gesture.

    1. POST /sequencing-run — instrument_run_id and instrument_model read
       from `--bcl-input-dir`'s RunInfo.xml via the shared
       qiita_common.illumina reader. Fails fast on a missing/malformed RunInfo.xml
       or on a PacBio-prefixed serial number (the parser filters PacBio out
       at load time so the lookup surfaces ``unknown instrument serial prefix``).
    2. POST /sequencing-run/{run_idx}/sequenced-pool — attaches the
       blob read from `--preflight-blob` (refuses empty), with the file
       basename as ``run_preflight_filename``.
    3. For each preflight ``illumina_sample`` row: POST the
       sequenced-sample composer with the resolved biosample_idx (from
       step 2.5 below), the resolved study_idx (from step 2.5 below),
       the operator-supplied prep_protocol_idx,
       and ``sequenced_pool_item_id = str(illumina_sample_idx)`` so the
       eventual fastq-to-parquet step keys on the bcl-convert fastq
       basename prefix.
    4. POST /work-ticket — target_kind sequenced_pool, the two idxs
       from steps 1 and 2, action_id+version pinned at the top of this
       module, action_context carrying the absolute bcl_input_dir.

    Step 2.5 (before any 1/2 side effects): POST
    /biosample/lookup-by-accession with the deduped preflight biosample
    accessions, then POST /study/lookup-by-accession with the deduped
    union of every row's primary + secondary project accessions. Both
    lookups always run, and if either carries a non-empty `missing`,
    the CLI emits a single combined stderr block (labeled sub-sections
    per class) and exits 1 with no side effects — the operator imports
    the missing biosamples / studies and re-runs. Find-or-create on
    steps 1 and 2 means a partial-failure retry converges on the same
    rows.

    All calls share one PAT (one ``run_http_subcommand`` invocation,
    one ``read_token``) so retries use the same credential.
    """
    if not args.bcl_input_dir.is_absolute():
        parser.error(f"--bcl-input-dir must be absolute, got {args.bcl_input_dir}")
    if not args.bcl_input_dir.is_dir():
        parser.error(
            f"--bcl-input-dir {args.bcl_input_dir} is not a directory; the workflow"
            " requires the on-disk Illumina BCL run folder"
        )
    if not args.preflight_blob.is_file():
        parser.error(f"--preflight-blob {args.preflight_blob} is not a regular file")
    blob_bytes = args.preflight_blob.read_bytes()
    if not blob_bytes:
        parser.error(f"--preflight-blob {args.preflight_blob} is empty")

    try:
        instrument_run_id, instrument_model = read_instrument_run_info(args.bcl_input_dir)
    except ValueError as exc:
        parser.error(str(exc))

    # Open the preflight SQLite locally and pull the per-sample rows
    # before any network call. Errors here are operator-actionable and
    # land as parser.error / exit 2.
    preflight_rows = _read_preflight_rows(args.preflight_blob, parser)

    # Host-filter argument coherence, validated before any network call:
    #   - minimap2 is the optional second stage; it never runs without rype.
    #   - if any sample requests human_filtering, --host-rype-reference-idx must
    #     name the host reference to record on those samples; without it those
    #     samples would silently never be host-filtered.
    if args.host_minimap2_reference_idx is not None and args.host_rype_reference_idx is None:
        parser.error("--host-minimap2-reference-idx requires --host-rype-reference-idx")
    any_human_filtering = any(row.human_filtering for row in preflight_rows)
    if any_human_filtering and args.host_rype_reference_idx is None:
        parser.error(
            "the preflight has human_filtering samples but no"
            " --host-rype-reference-idx was given; pass the host reference to"
            " deplete them against (preflight human_filtering=0 samples are left"
            " unfiltered)"
        )
    if args.host_rype_reference_idx is not None and not any_human_filtering:
        # Not fatal, but the operator likely expected the reference to take
        # effect — every sample's per-row guard leaves it unrecorded.
        sys.stderr.write(
            "warning: --host-rype-reference-idx was given but no preflight sample"
            " has human_filtering set; no sample will record a host reference\n"
        )

    # One-pass order-preserving dedup over preflight_rows so the lookup
    # route's `missing` echo is deterministic; the study side pools each
    # row's primary + secondaries so controls land their full set.
    unique_biosample_accessions: list[str] = []
    unique_study_accessions: list[str] = []
    seen_biosample: set[str] = set()
    seen_study: set[str] = set()
    for row in preflight_rows:
        if row.biosample_accession not in seen_biosample:
            seen_biosample.add(row.biosample_accession)
            unique_biosample_accessions.append(row.biosample_accession)
        for study_accession in (row.primary_project_accession, *row.secondary_project_accessions):
            if study_accession not in seen_study:
                seen_study.add(study_accession)
                unique_study_accessions.append(study_accession)

    run_body = SequencingRunCreateRequest(
        instrument_run_id=instrument_run_id,
        platform=Platform.ILLUMINA,
        instrument_model=instrument_model,
    ).model_dump(exclude_unset=True, mode="json")
    pool_body = SequencedPoolCreateRequest(
        run_preflight_blob=base64.b64encode(blob_bytes).decode("ascii"),
        run_preflight_filename=args.preflight_blob.name,
    ).model_dump(exclude_unset=True, mode="json")

    def _run(token: str) -> dict:
        # Resolve the caller's principal_idx via whoami once for the
        # per-sample owner_idx — composer requires it, route does not
        # auto-fill it server-side.
        owner_idx = _common.whoami(args.base_url, token)["principal_idx"]

        # Step 2.5: resolve every accession before any side effect. Both
        # lookups always run so the operator sees biosample + study
        # misses in a single round trip; a non-empty miss on either side
        # is the fail-fast path — print the combined block and exit 1
        # with no sequencing_run / sequenced_pool created.
        resolved_biosamples, missing_biosamples = _lookup_accessions(
            args.base_url,
            token,
            f"{PATH_BIOSAMPLE_PREFIX}{PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION}",
            unique_biosample_accessions,
            BiosampleLookupByAccessionRequest,
        )
        resolved_studies, missing_studies = _lookup_accessions(
            args.base_url,
            token,
            f"{PATH_STUDY_PREFIX}{PATH_STUDY_LOOKUP_BY_ACCESSION}",
            unique_study_accessions,
            StudyLookupByAccessionRequest,
        )
        if missing_biosamples or missing_studies:
            _print_missing_accession_error(preflight_rows, missing_biosamples, missing_studies)
            raise SystemExit(1)

        # Host-reference readiness — only when at least one sample is host-
        # filtered. Fail the whole gesture before any run/pool/sample side effect
        # if a designated host reference can't filter (not ACTIVE / missing its
        # index), so the operator sees one actionable error instead of samples
        # recorded against an unusable reference.
        if any_human_filtering:
            _assert_host_reference_ready(
                args.base_url,
                token,
                args.host_rype_reference_idx,
                HOST_FILTER_INDEX_TYPE_RYPE,
                "--host-rype-reference-idx",
            )
            if args.host_minimap2_reference_idx is not None:
                _assert_host_reference_ready(
                    args.base_url,
                    token,
                    args.host_minimap2_reference_idx,
                    HOST_FILTER_INDEX_TYPE_MINIMAP2,
                    "--host-minimap2-reference-idx",
                )

        run_resp, run_status = _common.call_with_status(
            "POST",
            args.base_url,
            token,
            PATH_SEQUENCING_RUN_PREFIX,
            json=run_body,
        )
        sequencing_run_idx = run_resp["sequencing_run_idx"]

        pool_resp, pool_status = _common.call_with_status(
            "POST",
            args.base_url,
            token,
            f"{PATH_SEQUENCING_RUN_PREFIX}"
            f"{PATH_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=sequencing_run_idx)}",
            json=pool_body,
        )
        sequenced_pool_idx = pool_resp["sequenced_pool_idx"]

        # Step 3: one sequenced-sample POST per preflight row. The
        # composer route runs each POST inside its own transaction, so a
        # mid-loop failure leaves a partial pool — the operator re-runs
        # the CLI and find-or-create on the run + pool plus
        # ON CONFLICT on the (pool_idx, pool_item_id) uniqueness lands
        # the rest. The composer route's own uniqueness check makes
        # repeat POSTs of the same (pool_idx, sequenced_pool_item_id) a
        # 409 — the CLI does NOT swallow this, so any divergence
        # (e.g. someone re-ran with a different prep_protocol_idx)
        # surfaces to the operator.
        per_sample_results: list[dict] = []
        for row in preflight_rows:
            secondary_study_idxs = [resolved_studies[a] for a in row.secondary_project_accessions]
            # human_filtering samples record the operator's host reference(s) so
            # the pool fan-out depletes them; human_filtering=0 samples carry no
            # host reference and are left unfiltered. Only set the keys when
            # filtering applies so an unfiltered sample's body stays minimal
            # (the model defaults both to None == no host filtering).
            host_rype = args.host_rype_reference_idx if row.human_filtering else None
            host_minimap2 = args.host_minimap2_reference_idx if row.human_filtering else None
            host_kwargs: dict[str, int] = {}
            if host_rype is not None:
                host_kwargs["host_rype_reference_idx"] = host_rype
                if host_minimap2 is not None:
                    host_kwargs["host_minimap2_reference_idx"] = host_minimap2
            sample_body = SequencedSampleCreateRequest(
                biosample_idx=resolved_biosamples[row.biosample_accession],
                owner_idx=owner_idx,
                prep_protocol_idx=args.prep_protocol_idx,
                sequenced_pool_item_id=str(row.illumina_sample_idx),
                primary_study_idx=resolved_studies[row.primary_project_accession],
                secondary_study_idxs=secondary_study_idxs,
                **host_kwargs,
            ).model_dump(exclude_unset=True, mode="json")
            sample_path = PATH_SEQUENCED_SAMPLE_FROM_RUN.format(
                sequencing_run_idx=sequencing_run_idx,
                sequenced_pool_idx=sequenced_pool_idx,
            )
            sample_resp = _common.call(
                "POST",
                args.base_url,
                token,
                f"{PATH_SEQUENCING_RUN_PREFIX}{sample_path}",
                json=sample_body,
            )
            per_sample_results.append(
                {
                    "illumina_sample_idx": row.illumina_sample_idx,
                    "biosample_accession": row.biosample_accession,
                    "biosample_idx": resolved_biosamples[row.biosample_accession],
                    "primary_study_idx": resolved_studies[row.primary_project_accession],
                    "secondary_study_idxs": secondary_study_idxs,
                    "human_filtering": row.human_filtering,
                    "host_rype_reference_idx": host_rype,
                    "host_minimap2_reference_idx": host_minimap2,
                    "sequenced_sample_idx": sample_resp["sequenced_sample_idx"],
                }
            )

        # Step 4: submit the bcl-convert work_ticket against the pool.
        ticket_body = WorkTicketCreateRequest(
            action_id=_BCL_CONVERT_ACTION_ID,
            action_version=_BCL_CONVERT_ACTION_VERSION,
            scope_target={
                "kind": ScopeTargetKind.SEQUENCED_POOL.value,
                "sequenced_pool_idx": sequenced_pool_idx,
                "sequencing_run_idx": sequencing_run_idx,
            },
            action_context={"bcl_input_dir": str(args.bcl_input_dir)},
        ).model_dump(exclude_unset=True, mode="json")
        ticket_resp, _ticket_status = _common.call_with_status(
            "POST",
            args.base_url,
            token,
            PATH_WORK_TICKET_PREFIX,
            json=ticket_body,
        )

        return {
            "sequencing_run": {
                "sequencing_run_idx": sequencing_run_idx,
                "status": "created" if run_status == 201 else "reused",
            },
            "sequenced_pool": {
                "sequenced_pool_idx": sequenced_pool_idx,
                "status": "created" if pool_status == 201 else "reused",
            },
            "sequenced_samples": per_sample_results,
            "work_ticket": ticket_resp,
            # Echo the args the orchestrator side will see so the
            # operator can sanity-check before the workflow runs.
            "instrument_run_id": instrument_run_id,
            "instrument_model": instrument_model,
            "prep_protocol_idx": args.prep_protocol_idx,
        }

    return _common.run_http_subcommand(_run)


def _handle_delete_sequenced_pool(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """DELETE /sequencing-run/{run}/sequenced-pool/{pool} — full pool purge.

    system_admin only. Passes ``force=true`` as a query param when --force is
    set; the server gates in-flight work tickets unconditionally regardless.
    The response echoes the per-table delete counts.
    """
    path = f"{PATH_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_POOL_BY_IDX}".format(
        sequencing_run_idx=args.sequencing_run_idx,
        sequenced_pool_idx=args.sequenced_pool_idx,
    )
    params = {"force": "true"} if args.force else None

    return _common.run_http_subcommand(
        lambda t: _common.call("DELETE", args.base_url, t, path, params=params)
    )


class _FastqMatchError(NamedTuple):
    """Sentinel: a sample's FASTQ could not be uniquely resolved under
    --convert-dir. Carries the operator-facing reason."""

    reason: str


def _match_pool_item_fastq(
    convert_dir: Path,
    pool_item_id: str,
    read_tag: str,
    *,
    required: bool,
) -> Path | _FastqMatchError | None:
    """Resolve the single `<pool_item_id>_*_<read_tag>_*.fastq.gz` under
    convert_dir, searching recursively.

    bcl-convert runs with --bcl-sampleproject-subdirectories, so per-sample
    FASTQs sit one level below convert_dir; rglob descends into them. The
    trailing underscore in the `<pool_item_id>_` prefix anchors the match so
    `12` never matches `120_...`. This is intentionally narrower than the
    server's submit-time prefix check (which also accepts a bare `<id>.fastq`):
    bcl-convert ConvertJob output is always the `<id>_..._R1_..._.fastq.gz`
    form, and a file this matcher accepts the server accepts too.

    Returns the Path on a unique match; None when none match and the read is
    optional (single-end R2); a _FastqMatchError when a required read is
    missing or when >1 match (multi-lane is out of scope — the workflow takes
    a single fastq_path).
    """
    matches = sorted(convert_dir.rglob(f"{pool_item_id}_*_{read_tag}_*.fastq.gz"))
    if len(matches) == 1:
        return matches[0]
    if not matches:
        if required:
            return _FastqMatchError(
                f"no {read_tag} FASTQ matched {pool_item_id}_*_{read_tag}_*.fastq.gz"
            )
        return None
    return _FastqMatchError(
        f"{len(matches)} {read_tag} FASTQs matched {pool_item_id} (lane-split runs are"
        f" not supported): {', '.join(m.name for m in matches)}"
    )


def _assert_host_reference_ready(
    base_url: str, token: str, reference_idx: int, index_type: str, flag: str
) -> None:
    """Pre-flight one host reference: it must be ACTIVE and carry `index_type`.

    Fails the whole gesture (SystemExit(1)) with an actionable message rather than
    letting the runner FAIL every per-sample ticket at its submission stage
    (_resolve_host_filter_indexes) — one error instead of N FAILED tickets. `flag`
    names the CLI flag in the message so the operator knows which reference is bad.
    """
    reference = _common.call(
        "GET",
        base_url,
        token,
        f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_BY_IDX.format(reference_idx=reference_idx)}",
    )
    if reference.get("status") != ReferenceStatus.ACTIVE.value:
        sys.stderr.write(
            f"{flag} host reference {reference_idx} is not active"
            f" (status={reference.get('status')!r}); load it to completion"
            " before host-filtering\n"
        )
        raise SystemExit(1)
    indexes = _common.call(
        "GET",
        base_url,
        token,
        f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_INDEX.format(reference_idx=reference_idx)}",
    )
    index_types = {row["index_type"] for row in indexes}
    if index_type not in index_types:
        sys.stderr.write(
            f"{flag} host reference {reference_idx} has no {index_type!r} index"
            f" (has {sorted(index_types)}); build it with host-reference-add\n"
        )
        raise SystemExit(1)


def _handle_submit_host_filter_pool(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """Fan out one QC'd, host-filtered fastq-to-parquet/1.2.0 ticket per active
    sequenced_sample in a completed bcl-convert pool.

    Flow:
      1. Validate --convert-dir (absolute, is_dir) before any network call.
      2. Pre-flight each host reference: the rype reference (required) must be
         ACTIVE + carry a rype index; the minimap2 reference (optional) must be
         ACTIVE + carry a minimap2 index. A bad reference here would otherwise
         fail every ticket at the runner's submission stage
         (_resolve_host_filter_indexes) — N FAILED tickets instead of one
         actionable error.
      3. Read the run's instrument_model once (GET /sequencing-run) to forward
         per sample so QC's polyG step is gated correctly (nullable).
      4. List the pool's active samples (run-scoped pool route) and resolve
         each one's R1 (required) + R2 (optional) FASTQ under --convert-dir.
         Resolve ALL samples before any POST so a missing/ambiguous file
         aborts with zero side effects.
      5. POST one fastq-to-parquet/1.2.0 ticket per sample (always-on QC +
         host_filter_enabled with the two-reference layout), scoped to the
         sample's prep_sample_idx.

    The pool_item_id == bcl-convert FASTQ basename prefix coupling holds via
    submit-bcl-convert (sequenced_pool_item_id = str(illumina_sample_idx)) and
    the pinned run_preflight (Sample_ID emitted as illumina_sample_idx); a
    future preflight that changes Sample_ID would silently break this match.

    Per-sample 409 (in-flight) / 422 (filename-prefix) are NOT swallowed — they
    surface so the operator can act, the same as submit-bcl-convert's composer.
    """
    if not args.convert_dir.is_absolute():
        parser.error(f"--convert-dir must be absolute, got {args.convert_dir}")
    if not args.convert_dir.is_dir():
        parser.error(
            f"--convert-dir {args.convert_dir} is not a directory; pass the"
            " bcl-convert ConvertJob output directory, visible from this host"
        )

    def _run(token: str) -> dict:
        # Step 1: host-reference readiness — fail the whole gesture before any
        # ticket if a designated reference can't host-filter. rype is required;
        # minimap2 is optional (omitted -> rype-only host filter).
        _assert_host_reference_ready(
            args.base_url,
            token,
            args.host_rype_reference_idx,
            HOST_FILTER_INDEX_TYPE_RYPE,
            "--host-rype-reference-idx",
        )
        if args.host_minimap2_reference_idx is not None:
            _assert_host_reference_ready(
                args.base_url,
                token,
                args.host_minimap2_reference_idx,
                HOST_FILTER_INDEX_TYPE_MINIMAP2,
                "--host-minimap2-reference-idx",
            )

        # Step 2: the run's instrument_model gates QC's polyG; read it once and
        # forward per sample. Nullable (a non-bcl run may not record it).
        run = _common.call(
            "GET",
            args.base_url,
            token,
            f"{PATH_SEQUENCING_RUN_PREFIX}"
            f"{PATH_SEQUENCING_RUN_BY_IDX.format(sequencing_run_idx=args.sequencing_run_idx)}",
        )
        instrument_model = run.get("instrument_model")

        # Step 3: enumerate the pool's active samples (single round trip).
        pool_list_path = PATH_SEQUENCED_SAMPLE_LIST_BY_POOL.format(
            sequencing_run_idx=args.sequencing_run_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
        )
        roster = _common.call(
            "GET",
            args.base_url,
            token,
            f"{PATH_SEQUENCING_RUN_PREFIX}{pool_list_path}",
        )
        samples = roster["samples"]
        if not samples:
            sys.stderr.write(
                f"sequenced_pool {args.sequenced_pool_idx} has no active"
                " sequenced_samples to host-filter\n"
            )
            raise SystemExit(1)

        # Step 4: resolve every sample's FASTQ(s) up front. Collect all errors
        # so the operator sees the full set, and submit nothing if any fail.
        resolved: list[tuple[dict, Path, Path | None]] = []
        errors: list[str] = []
        for sample in samples:
            item_id = sample["sequenced_pool_item_id"]
            label = f"sample {item_id} (prep_sample {sample['prep_sample_idx']})"
            r1 = _match_pool_item_fastq(args.convert_dir, item_id, "R1", required=True)
            if isinstance(r1, _FastqMatchError):
                errors.append(f"{label}: {r1.reason}")
                continue
            r2 = _match_pool_item_fastq(args.convert_dir, item_id, "R2", required=False)
            if isinstance(r2, _FastqMatchError):
                errors.append(f"{label}: {r2.reason}")
                continue
            resolved.append((sample, r1, r2))
        if errors:
            sys.stderr.write(
                "could not resolve FASTQs for every sample; no tickets"
                " submitted:\n  " + "\n  ".join(errors) + "\n"
            )
            raise SystemExit(1)

        # Step 5: one fastq-to-parquet/1.2.0 ticket per sample — always-on QC +
        # host filtering (two-reference layout). instrument_model is forwarded
        # only when the run records it (QC defaults polyG OFF when it's absent).
        per_sample_results: list[dict] = []
        for sample, r1_path, r2_path in resolved:
            action_context: dict[str, Any] = {
                "fastq_path": str(r1_path),
                "host_filter_enabled": True,
                "host_rype_reference_idx": args.host_rype_reference_idx,
            }
            if args.host_minimap2_reference_idx is not None:
                action_context["host_minimap2_reference_idx"] = args.host_minimap2_reference_idx
            if r2_path is not None:
                action_context["reverse_fastq_path"] = str(r2_path)
            if instrument_model is not None:
                action_context["instrument_model"] = instrument_model
            ticket_body = WorkTicketCreateRequest(
                action_id=_FASTQ_TO_PARQUET_ACTION_ID,
                action_version=_FASTQ_TO_PARQUET_ACTION_VERSION,
                scope_target={
                    "kind": ScopeTargetKind.PREP_SAMPLE.value,
                    "prep_sample_idx": sample["prep_sample_idx"],
                },
                action_context=action_context,
            ).model_dump(exclude_unset=True, mode="json")
            ticket_resp, _ticket_status = _common.call_with_status(
                "POST",
                args.base_url,
                token,
                PATH_WORK_TICKET_PREFIX,
                json=ticket_body,
            )
            per_sample_results.append(
                {
                    "prep_sample_idx": sample["prep_sample_idx"],
                    "sequenced_pool_item_id": sample["sequenced_pool_item_id"],
                    "fastq_path": str(r1_path),
                    "reverse_fastq_path": str(r2_path) if r2_path is not None else None,
                    "work_ticket_idx": ticket_resp.get("work_ticket_idx"),
                }
            )

        return {
            "host_rype_reference_idx": args.host_rype_reference_idx,
            "host_minimap2_reference_idx": args.host_minimap2_reference_idx,
            "instrument_model": instrument_model,
            "sequencing_run_idx": args.sequencing_run_idx,
            "sequenced_pool_idx": args.sequenced_pool_idx,
            "samples_submitted": len(per_sample_results),
            "per_sample": per_sample_results,
        }

    return _common.run_http_subcommand(_run)


def _build_body(
    model_cls: type[BaseModel],
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> dict:
    """Construct `model_cls` from the parsed-args fields that match its
    model_fields, then return the exclude_unset JSON dump.

    Filters None out of the namespace before construction so the only
    fields Pydantic treats as "set" are the ones the caller actually
    passed (matches the server's exclude_unset semantics on the PATCH
    side; honest with the schema on the POST side). Argparse's dest
    names line up with the Pydantic field names (snake_case from
    hyphenated flags), so the filter is a single comprehension.

    On ValidationError (e.g. a too-long --title, malformed --orcid),
    flattens the errors into a single stderr line and exits 2 via
    parser.error — same code path as argparse's own validation
    failures, so callers don't see a Python traceback for invalid
    input.
    """
    fields = {
        name: getattr(args, name)
        for name in model_cls.model_fields
        if getattr(args, name, None) is not None
    }
    try:
        return model_cls(**fields).model_dump(exclude_unset=True, mode="json")
    except ValidationError as exc:
        msgs = "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors())
        parser.error(f"invalid {model_cls.__name__}: {msgs}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _common.validate_base_url(args, parser)
    return args.handler(args, parser)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
