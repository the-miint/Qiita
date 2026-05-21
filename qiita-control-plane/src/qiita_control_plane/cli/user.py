"""qiita — end-user CLI for the Qiita control plane.

Scope: actions a regular user performs against a running deployment.
The parallel operator/admin CLI is `qiita-admin`; that one stays
scoped to principal/role/token management and is deliberately separate.

This module owns the user-facing argparse surface and its subcommand
handlers. PAT file I/O, the LoginRocket loopback flow, the
authenticated HTTP call helper, and the generic token-read + invoke +
JSON-print runner live in `cli._common`.

Authentication: HTTP subcommands read the PAT from QIITA_TOKEN env or
from ~/.qiita/token (mode 0600).
"""

import argparse
import base64
import sys
from pathlib import Path

from pydantic import BaseModel, ValidationError
from qiita_common.api_paths import PATH_WORK_TICKET_PREFIX
from qiita_common.models import (
    BiosampleImportRequest,
    Platform,
    ScopeTargetKind,
    SequencedPoolCreateRequest,
    SequencedSampleCreateRequest,
    SequencingRunCreateRequest,
    StudyCreate,
    Tier,
    UserUpdate,
    WorkTicketCreateRequest,
)

from . import _common

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
    # --ena-sample-accession is deliberately NOT exposed: it's the
    # publication-lock signal set by the submission subsystem, not a
    # caller-supplied field.
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
        help="Per-pool unique item identifier (e.g., the well or library barcode)",
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
    # exposed: they're publication-lock signals set by the submission
    # subsystem, not caller-supplied fields.
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
            " the action's context_schema). For fastq-to-parquet:"
            ' \'{"fastq_path": "/abs/path/sample.fastq"}\''
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
