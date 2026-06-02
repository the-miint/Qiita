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
from typing import NamedTuple

from pydantic import BaseModel, ValidationError
from qiita_common.api_paths import (
    PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION,
    PATH_BIOSAMPLE_PREFIX,
    PATH_SEQUENCED_SAMPLE_FROM_RUN,
    PATH_SEQUENCING_RUN_PREFIX,
    PATH_SEQUENCING_RUN_SEQUENCED_POOL,
    PATH_STUDY_LOOKUP_BY_ACCESSION,
    PATH_STUDY_PREFIX,
    PATH_WORK_TICKET_PREFIX,
)
from qiita_common.illumina import (
    instrument_model_from_run_folder,
    instrument_run_id_from_run_folder,
)
from qiita_common.models import (
    BiosampleImportRequest,
    BiosampleLookupByAccessionRequest,
    Platform,
    ScopeTargetKind,
    SequencedPoolCreateRequest,
    SequencedSampleCreateRequest,
    SequencingRunCreateRequest,
    StudyCreate,
    StudyLookupByAccessionRequest,
    Tier,
    UserUpdate,
    WorkTicketCreateRequest,
)

from . import _common

# action_id + version for the bundled bcl-convert submission flow. Pinned
# here so the CLI does not drift from the workflow YAML the operator's
# deploy syncs into qiita.action; bumping the workflow major version is a
# coordinated change.
_BCL_CONVERT_ACTION_ID = "bcl-convert"
_BCL_CONVERT_ACTION_VERSION = "1.0.0"


class _PreflightRow(NamedTuple):
    """One illumina_sample row pulled from the kl-run-preflight SQLite.

    Field names mirror `run_preflight.get_illumina_sample_info`'s 4-tuple.
    `secondary_project_accessions` is empty for non-control samples;
    controls carry one entry per non-primary plate project, sorted by
    accession value.
    """

    illumina_sample_idx: int
    biosample_accession: str
    primary_project_accession: str
    secondary_project_accessions: list[str]


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
    p_study_create.add_argument("--ebi-study-accession")
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
    p_study_create.set_defaults(handler=_handle_study_create)

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
    p_biosample_create.add_argument("--metadata-checklist-idx", type=int)
    p_biosample_create.add_argument(
        "--biosample-accession",
        help="External biosample accession (e.g. NCBI), if known at create time",
    )
    p_biosample_create.add_argument(
        "--matrix-tube-id",
        help="Matrix-tube identifier (digits only); validated server-side",
    )
    # --ena-sample-accession is deliberately NOT exposed: an ENA
    # accession is a submission-tracking value the submission subsystem
    # writes back after an ENA submission, not part of the interactive
    # create flow.
    p_biosample_create.set_defaults(handler=_handle_biosample_create)

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
    p_seqsample_create.add_argument("--metadata-checklist-idx", type=int)
    # ena_experiment_accession + ena_run_accession are deliberately NOT
    # exposed: ENA accessions are submission-tracking values the
    # submission subsystem writes back after an ENA submission, not part
    # of the interactive create flow.
    p_seqsample_create.set_defaults(handler=_handle_sequenced_sample_create)

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
        choices=("sequence_reference", "taxonomy_authority"),
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
            " which builds the rype negative-filter index. Requires --taxonomy."
        ),
    )
    p_reference_load.add_argument("--fasta", required=True, type=Path)
    p_reference_load.add_argument("--taxonomy", type=Path)
    p_reference_load.add_argument("--tree", type=Path)
    p_reference_load.add_argument("--jplace", type=Path)
    p_reference_load.add_argument("--genome-map", type=Path, dest="genome_map")
    p_reference_load.add_argument(
        "--data-plane-url",
        required=True,
        help="gRPC URL of the data plane (e.g. grpc://qiita-data.example.com:50051)",
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
            " ID, instrument model, and platform are derived from the"
            " --bcl-input-dir basename using the vendored Illumina prefix"
            " table (qiita_common.illumina); folder names from unsupported"
            " families (HiSeq1500, HiSeq3000, NextSeq, NovaSeqXPlus) and"
            " from PacBio fail-fast before any server round-trip. All three"
            " server-side calls are find-or-create on their natural keys,"
            " so a re-run after a partial failure converges on the existing"
            " rows without operator cleanup."
        ),
    )
    p_submit_bcl.add_argument(
        "--bcl-input-dir",
        type=Path,
        required=True,
        help=(
            "Absolute path to the Illumina BCL run folder; the folder's"
            " basename must match"
            " <YYMMDD>_<InstrumentSerial>_<RunNum>_<FlowcellID> so the"
            " parser can derive the instrument_run_id + instrument_model."
            " This same path is passed through as action_context.bcl_input_dir"
            " on the resulting work-ticket; the orchestrator binds its parent"
            " directory into the bcl-convert container at submit time."
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
    p_submit_bcl.set_defaults(handler=_handle_submit_bcl_convert)

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


def _handle_study_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
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

    body = _build_body(WorkTicketCreateRequest, args, parser)
    return _common.run_http_subcommand(lambda t: _post_work_ticket(args.base_url, t, body))


def _handle_ticket_status(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """GET a single work-ticket's full record. The CLI prints the full
    server response so a USER polling for state can render every field
    (state, retry_count, failure_*, timestamps) without a second call."""
    return _common.run_http_subcommand(
        lambda t: _get_work_ticket(args.base_url, t, args.work_ticket_idx)
    )


async def _run_reference_load(
    *,
    base_url: str,
    token: str,
    data_plane_url: str,
    args: argparse.Namespace,
) -> dict:
    """Construct real httpx + pyarrow.flight clients and drive
    `do_reference_load`. Lives next to the CLI handler so the handler
    stays a thin argparse → entry-point shim; the entry point itself
    (in cli.reference_load) takes injected clients so tests bypass this
    function entirely."""
    import httpx as _httpx
    import pyarrow.flight as flight

    from .reference_load import do_reference_load

    flight_client = flight.FlightClient(data_plane_url)
    try:
        async with _httpx.AsyncClient(
            base_url=base_url, timeout=_common.CLI_HTTP_TIMEOUT_SECONDS
        ) as http:
            return await do_reference_load(
                http=http,
                token=token,
                flight_client=flight_client,
                fasta_path=args.fasta,
                name=args.name,
                version=args.version,
                kind=args.kind,
                host=args.host,
                reference_idx=args.reference_idx,
                taxonomy_path=args.taxonomy,
                tree_path=args.tree,
                jplace_path=args.jplace,
                genome_map_path=args.genome_map,
                watch=not args.no_watch,
                poll_interval_seconds=args.poll_interval,
                timeout_seconds=args.timeout,
            )
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
    from run_preflight import get_illumina_sample_info  # noqa: PLC0415

    try:
        conn = sqlite3.connect(f"file:{preflight_blob}?mode=ro", uri=True)
    except sqlite3.DatabaseError as exc:
        parser.error(f"--preflight-blob {preflight_blob}: not a readable SQLite file: {exc}")
    try:
        illumina_samples = get_illumina_sample_info(conn)
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
        parsed.append(
            _PreflightRow(
                illumina_sample_idx=int(illumina_sample_idx),
                biosample_accession=biosample_accession,
                primary_project_accession=primary,
                secondary_project_accessions=list(secondary),
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

    1. POST /sequencing-run — instrument_run_id, instrument_model, and
       platform all derived from `--bcl-input-dir` via the shared
       qiita_common.illumina parser. Fails fast on a folder name that
       does not match Illumina convention or on a PacBio-prefixed
       serial (the parser filters PacBio out at load time so the lookup
       surfaces ``unknown instrument serial prefix``).
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

    folder_name = args.bcl_input_dir.name
    try:
        instrument_run_id = instrument_run_id_from_run_folder(folder_name)
        instrument_model = instrument_model_from_run_folder(folder_name)
    except ValueError as exc:
        parser.error(str(exc))

    # Open the preflight SQLite locally and pull the per-sample rows
    # before any network call. Errors here are operator-actionable and
    # land as parser.error / exit 2.
    preflight_rows = _read_preflight_rows(args.preflight_blob, parser)

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
            sample_body = SequencedSampleCreateRequest(
                biosample_idx=resolved_biosamples[row.biosample_accession],
                owner_idx=owner_idx,
                prep_protocol_idx=args.prep_protocol_idx,
                sequenced_pool_item_id=str(row.illumina_sample_idx),
                primary_study_idx=resolved_studies[row.primary_project_accession],
                secondary_study_idxs=secondary_study_idxs,
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
