"""qiita user CLI — argparse parser construction.

Split out of the former single-file ``cli.user`` module; behavior unchanged.
"""

import argparse
from pathlib import Path

from qiita_common.api_paths import (
    PATH_BIOSAMPLE_BY_IDX,
    PATH_BIOSAMPLE_LIST_BY_STUDY,
    PATH_BIOSAMPLE_PREFIX,
    PATH_PREP_SAMPLE_PREFIX,
    PATH_PREP_SAMPLE_STUDY_LIST,
    PATH_SEQUENCED_SAMPLE_BY_IDX,
    PATH_SEQUENCED_SAMPLE_LIST_BY_RUN_FULL,
    PATH_SEQUENCED_SAMPLE_PREFIX,
    PATH_SEQUENCING_RUN_BY_IDX,
    PATH_SEQUENCING_RUN_PREFIX,
    PATH_STUDY_BY_IDX,
    PATH_STUDY_PREFIX,
)
from qiita_common.models import (
    HOST_FILTER_INDEX_TYPE_MINIMAP2,
    HOST_FILTER_INDEX_TYPE_RYPE,
    BiosamplePatchRequest,
    Platform,
    SequencedSamplePatchRequest,
    StudyPatchRequest,
    Tier,
    WorkTicketState,
)

from .. import _common
from ._helpers import _handle_patch, _handle_read, _lane_arg
from .auth import _handle_login, _handle_profile_set, _handle_whoami
from .biosample import _handle_biosample_create
from .pacbio import _handle_submit_pacbio_ingest
from .pool import (
    _handle_delete_sequenced_pool,
    _handle_pool_completion,
    _handle_submit_bcl_convert,
    _handle_submit_block_mask_pool,
    _handle_submit_host_filter_pool,
)
from .reference import _handle_reference_list, _handle_reference_load
from .sequencing import (
    _handle_prep_protocol_list,
    _handle_prep_sample_retire,
    _handle_run_preflight_update_lane,
    _handle_sequenced_pool_create,
    _handle_sequenced_sample_create,
    _handle_sequencing_run_create,
    _handle_sequencing_run_lookup,
)
from .study import _handle_study_create
from .ticket import (
    _handle_ticket_list,
    _handle_ticket_logs,
    _handle_ticket_run,
    _handle_ticket_status,
    _handle_ticket_submit,
)


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

    p_run_preflight = sub.add_parser("run-preflight", help="Run-preflight maintenance operations")
    p_run_preflight_sub = p_run_preflight.add_subparsers(dest="run_preflight_cmd", required=True)
    p_rp_update_lane = p_run_preflight_sub.add_parser(
        "update-lane",
        help=(
            "Bulk-reassign the lane on a pool's run-preflight blob"
            " (POST /sequencing-run/{R}/sequenced-pool/{P}/preflight/update-lane);"
            " wet_lab_admin+, refused once the run has been processed"
        ),
    )
    p_rp_update_lane.add_argument(
        "--sequencing-run-idx", type=int, required=True, dest="sequencing_run_idx"
    )
    p_rp_update_lane.add_argument(
        "--sequenced-pool-idx", type=int, required=True, dest="sequenced_pool_idx"
    )
    p_rp_update_lane.add_argument(
        "--platform",
        required=True,
        choices=("illumina", "tellseq"),
        help=(
            "Platform-specific sample table to update — the run_preflight key"
            " ('illumina' or 'tellseq'), NOT the qiita Platform enum"
        ),
    )
    p_rp_update_lane.add_argument(
        "--from-lane",
        required=True,
        type=_lane_arg,
        dest="from_lane",
        help="Source lane to move from: an integer >= 1, or 'none' for a NULL lane",
    )
    p_rp_update_lane.add_argument(
        "--to-lane",
        required=True,
        type=_lane_arg,
        dest="to_lane",
        help="Target lane to move to: an integer >= 1, or 'none' to clear to NULL",
    )
    p_rp_update_lane.add_argument(
        "--reason",
        required=True,
        help="Audit reason recorded in the preflight change_log (required)",
    )
    p_rp_update_lane.set_defaults(handler=_handle_run_preflight_update_lane)

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

    p_prepsample_retire = p_prepsample_sub.add_parser(
        "retire",
        help=(
            "Retire a prep-sample so it drops out of a pool's active set —"
            " e.g. an empty/failed-yield well (PATCH /prep-sample/{idx}/retired)"
        ),
    )
    p_prepsample_retire.add_argument("--prep-sample-idx", type=int, required=True)
    p_prepsample_retire.add_argument(
        "--reason", help="Optional retire_reason recorded on the prep_sample"
    )
    p_prepsample_retire.set_defaults(handler=_handle_prep_sample_retire, retired=True)

    p_prepsample_unretire = p_prepsample_sub.add_parser(
        "un-retire",
        help=(
            "Un-retire a misclassified prep-sample, returning it to a pool's"
            " active set (PATCH /prep-sample/{idx}/retired)"
        ),
    )
    p_prepsample_unretire.add_argument("--prep-sample-idx", type=int, required=True)
    p_prepsample_unretire.set_defaults(
        handler=_handle_prep_sample_retire, retired=False, reason=None
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
    p_reference_list = p_reference_sub.add_parser(
        "list",
        help=(
            "List references (idx/name/version/status/is_host + built index types),"
            " with optional filters — discover the idx for --host-rype-reference-idx etc."
        ),
    )
    p_reference_list.add_argument(
        "--host", action="store_true", help="Only host references (is_host=true)"
    )
    p_reference_list.add_argument(
        "--active",
        action="store_true",
        help="Only ACTIVE references (ready to use)",
    )
    p_reference_list.add_argument(
        "--index-type",
        dest="index_type",
        choices=(HOST_FILTER_INDEX_TYPE_RYPE, HOST_FILTER_INDEX_TYPE_MINIMAP2),
        help="Only references carrying this built index type",
    )
    p_reference_list.set_defaults(handler=_handle_reference_list)
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
    p_reference_load.add_argument(
        "--shard-index",
        action="store_true",
        dest="shard_index",
        help=(
            "Build per-shard ANALYSIS aligner indexes (minimap2 + bowtie2) on a plain"
            " reference, plus the ONE whole-reference rype router: after ingest,"
            " plan-shards fans out one build per lineage-sorted shard (loading ->"
            " indexing -> active). Requires --taxonomy; mutually exclusive with --host."
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
    # Index selection + build params, scoped by index type: rype knobs apply to
    # --host ONLY (a sharded reference's routing index is the auto-built
    # whole-reference router, not a per-shard rype); minimap2 knobs to --host OR
    # --shard-index; --no-bowtie2-index to --shard-index only. Default builds
    # every applicable index; the opt-out flags skip one (the entry point rejects
    # building none). --minimap2-preset tunes the HOST builder only; a sharded
    # reference's per-shard .mmi is always built with the fixed map-hifi preset.
    p_reference_load.add_argument(
        "--no-rype-index",
        action="store_true",
        help=(
            "With --host: skip the rype host-filter index. Cannot be combined with"
            " --no-minimap2-index such that neither host index is built."
        ),
    )
    p_reference_load.add_argument(
        "--no-minimap2-index",
        action="store_true",
        help=(
            "With --host/--shard-index: skip the minimap2 index. Cannot be combined"
            " with the other --no-*-index flags such that none is built."
        ),
    )
    p_reference_load.add_argument(
        "--no-bowtie2-index",
        action="store_true",
        dest="no_bowtie2_index",
        help=(
            "With --shard-index: skip the bowtie2 per-shard analysis index (bowtie2 is"
            " analysis-only, so this does not apply to --host)."
        ),
    )
    p_reference_load.add_argument(
        "--rype-w",
        type=int,
        help="With --host: rype minimizer window `w` (default 20).",
    )
    p_reference_load.add_argument(
        "--minimap2-preset",
        choices=("sr", "map-ont", "map-pb", "map-hifi", "asm5", "asm10", "asm20"),
        help=(
            "With --host only: minimap2 preset baked into the host .mmi index (default"
            " sr). Not allowed with --shard-index (per-shard .mmi is fixed at map-hifi)."
        ),
    )
    p_reference_load.add_argument(
        "--data-plane-url",
        help=(
            "gRPC URL of the data plane. From off the deploy host use the public "
            "TLS edge (e.g. grpc+tls://qiita.example.com:443); grpc://<host>:50051 "
            "is the direct/on-host form. Required for remote ingest; ignored (and "
            "optional) under --local."
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

    p_prep_protocol = sub.add_parser(
        "prep-protocol", help="Prep-protocol discovery (idxes for --prep-protocol-idx)"
    )
    p_prep_protocol_sub = p_prep_protocol.add_subparsers(dest="prep_protocol_cmd", required=True)
    p_prep_protocol_list = p_prep_protocol_sub.add_parser(
        "list", help="List prep protocols (idx/name/retired) for --prep-protocol-idx"
    )
    p_prep_protocol_list.add_argument(
        "--all",
        action="store_true",
        dest="include_retired",
        help="Include retired protocols (default: only non-retired)",
    )
    p_prep_protocol_list.set_defaults(handler=_handle_prep_protocol_list)

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
            " blob to the sequenced-pool row. The pool find-or-create key is"
            " the preflight *content* (its SHA-256), so the same bytes"
            " re-uploaded under any filename resolve to the same pool; the"
            " file basename is stored as run_preflight_filename for reference"
            " only."
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
        "--force",
        action="store_true",
        help=(
            "Re-submit even when a COMPLETED bcl-convert ticket already exists"
            " for this pool. Without it the submission is refused, because a"
            " re-run re-registers the pool's reads into the lake (duplicate"
            " rows — DuckLake has no uniqueness). Requires wet_lab_admin or"
            " system_admin. The non-force recovery is delete-sequenced-pool"
            " then resubmit."
        ),
    )
    p_submit_bcl.set_defaults(handler=_handle_submit_bcl_convert)

    p_submit_pacbio = sub.add_parser(
        "submit-pacbio-ingest",
        help=(
            "Bundled operator gesture for PacBio HiFi ingest: mint (or reuse) a"
            " sequencing-run row, attach a sequenced-pool with the preflight blob,"
            " and fan out one bam-to-parquet ingest ticket per demultiplexed sample."
        ),
        description=(
            "Submit PacBio HiFi ingest end-to-end. PacBio arrives already"
            " demultiplexed (one uBAM per barcode under"
            " {run_folder}/{smartcell}/hifi_reads/), so unlike bcl-convert there is"
            " no in-workflow demux: each sample's BAM is located on disk by its"
            " barcode and loaded by its own bam-to-parquet ticket. A barcode reused"
            " across SMRT cells fails fast (the preflight now carries a SMRT-cell"
            " field, but until it is populated the reuse cannot be disambiguated)."
            " The run + pool are"
            " find-or-create and the per-sample roster is create-missing, so"
            " re-running after a partial failure converges without cleanup —"
            " reusing what exists and retrying only the missing samples/tickets."
        ),
    )
    p_submit_pacbio.add_argument(
        "--run-folder",
        type=Path,
        required=True,
        help=(
            "Absolute path to the PacBio run folder on the shared filesystem. Must"
            " contain per-SMRT-cell well subdirectories with"
            " hifi_reads/*.hifi_reads.<barcode>.bam demultiplexed reads. Each"
            " sample's resolved BAM path is passed as action_context.bam_path on its"
            " bam-to-parquet ticket."
        ),
    )
    p_submit_pacbio.add_argument(
        "--preflight-blob",
        type=Path,
        required=True,
        help=(
            "Path to the local kl-run-preflight SQLite file. The CLI reads it"
            " (refuses empty), base64-encodes the bytes, and attaches the blob to"
            " the sequenced-pool row so a later read-mask submission can re-read the"
            " per-sample protocol columns. Same content-addressed pool find-or-create"
            " as submit-bcl-convert."
        ),
    )
    p_submit_pacbio.add_argument(
        "--instrument-run-id",
        required=True,
        help=(
            "Instrument run identifier for the sequencing-run row (PacBio has no"
            " RunInfo.xml). The find-or-create key together with the platform."
        ),
    )
    p_submit_pacbio.add_argument(
        "--instrument-model",
        default=None,
        help=(
            "Optional PacBio instrument model (e.g. 'Revio'), recorded on the"
            " sequencing-run row. Omitted -> NULL (QC's polyG gating stays off for"
            " long reads regardless)."
        ),
    )
    p_submit_pacbio.add_argument(
        "--prep-protocol-idx",
        type=int,
        required=True,
        help=(
            "Qiita prep_protocol_idx to FK every per-sample row to. Applied"
            " uniformly across the pool (the preflight carries no Qiita"
            " prep_protocol identifier), mirroring submit-bcl-convert. NOT"
            " validated against the platform — passing a non-PacBio/short-read"
            " protocol here silently mislabels every sample, so double-check it is"
            " the intended long-read protocol."
        ),
    )
    p_submit_pacbio.add_argument(
        "--force",
        action="store_true",
        help=(
            "Re-submit each sample's bam-to-parquet ticket even when a COMPLETED"
            " one already exists (a re-run re-registers reads into the lake —"
            " DuckLake has no uniqueness). Requires wet_lab_admin or system_admin."
        ),
    )
    p_submit_pacbio.set_defaults(handler=_handle_submit_pacbio_ingest)

    p_delete_pool = sub.add_parser(
        "delete-sequenced-pool",
        help=(
            "Hard-delete a full sequenced-pool (one bcl-convert sample"
            " sheet's worth of samples) and everything under it. Admin only."
        ),
        description=(
            "Fully purge a sequenced_pool: the pool row plus every"
            " sequenced-sample / prep-sample under it, their metadata, study"
            " links, and pool-/sample-scoped work tickets, PLUS the DuckLake"
            " read/read_mask rows those prep-samples produced and their durable"
            " staged read copies on disk. The parent sequencing-run and the"
            " underlying biosamples are retained. Because each prep-sample is"
            " exclusive to this pool, deleting it removes those samples from"
            " EVERY study they link to, not only one. Requires system_admin"
            " (sequenced_pool:delete). In-flight work tickets block the delete"
            " unconditionally; terminal tickets (completed/no_data/failed),"
            " published prep-samples, and ENA-submitted samples block it unless"
            " --force is passed."
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
            "Override the soft blocks: delete even when terminal"
            " (completed/no_data/failed) work tickets reference the pool,"
            " prep-samples are published into a study, or samples carry an ENA"
            " accession. Does NOT override in-flight work tickets."
        ),
    )
    p_delete_pool.set_defaults(handler=_handle_delete_sequenced_pool)

    p_submit_hf = sub.add_parser(
        "submit-host-filter-pool",
        help=(
            "Bundled operator gesture: create one read mask per sample over a"
            " pool's already-stored reads, host-filtering every sample against"
            " the host reference(s) given on THIS submission."
        ),
        description=(
            "For every active sequenced_sample in --sequenced-pool-idx, submit a"
            " read-mask work-ticket: always-on QC (fastp-equivalent"
            " adapter/polyG/length trimming) followed by host filtering, recorded"
            " as a read_mask over the reads bcl-convert already stored — this"
            " command does NOT parse FASTQ or re-store reads. The host reference"
            " is a property of THIS filtering config, not of the sample:"
            " --host-rype-reference-idx (with optional --host-minimap2-reference-idx)"
            " names the reference(s) every sample in the pool is depleted against."
            " Omit them to run QC-only with host filtering disabled (a pass-through"
            " for the whole pool). Because reads are stored once and masks are"
            " separate, the SAME pool can be re-submitted later against a different"
            " host reference to produce a second, side-by-side mask — neither"
            " re-runs ingest. Each given reference is checked for ACTIVE status +"
            " its required index up front, so a misconfiguration aborts with zero"
            " side effects. The run's instrument_model is read once (GET"
            " /sequencing-run) and forwarded per sample so QC's polyG step is"
            " gated correctly. The existing one-in-flight-per-prep_sample guard"
            " serializes concurrent masks of one sample; submit a second"
            " host-reference mask once the first pool's tickets are terminal."
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
        default=None,
        help=(
            "ACTIVE host reference_idx whose rype (.ryxdi) index every sample in"
            " the pool is depleted against for this submission. Omit to run the"
            " whole pool QC-only with host filtering disabled. Checked ACTIVE +"
            " carrying a rype index up front. Re-submitting the same pool against a"
            " different reference produces a second, side-by-side read mask."
        ),
    )
    p_submit_hf.add_argument(
        "--host-minimap2-reference-idx",
        type=int,
        default=None,
        help=(
            "Optional ACTIVE host reference_idx whose minimap2 (.mmi) index drives"
            " the second host-filter pass on rype's survivors. Requires"
            " --host-rype-reference-idx. Omit for a rype-only host filter."
        ),
    )
    p_submit_hf.add_argument(
        "--force",
        action="store_true",
        help=(
            "Proceed even when some samples' intake human_filtering intent"
            " disagrees with this submission's pool-wide host-reference choice."
            " The mismatch is printed as a warning instead of aborting."
        ),
    )
    p_submit_hf.add_argument(
        "--only-missing",
        action="store_true",
        help=(
            "Skip samples that already have a read-mask ticket (any state),"
            " submitting only those with none. Use to fill in a pool whose prior"
            " fan-out was interrupted, without duplicating already-submitted"
            " work. Off by default so re-submitting the whole pool against a"
            " different host reference still produces a side-by-side mask."
        ),
    )
    p_submit_hf.set_defaults(handler=_handle_submit_host_filter_pool)

    p_submit_block = sub.add_parser(
        "submit-block-mask-pool",
        help=(
            "Bulk-block variant of submit-host-filter-pool: mask a whole pool as"
            " fixed ~10M-read blocks (one work-ticket per block) instead of one"
            " ticket per sample."
        ),
        description=(
            "Plan + submit a pool's read masking as bulk BLOCKS in a single server"
            " call. Same filtering semantics and preflight as"
            " submit-host-filter-pool — --host-rype-reference-idx (with optional"
            " --host-minimap2-reference-idx) names the reference(s) every sample is"
            " depleted against, or omit both for a QC-only pass-through; each is"
            " checked ACTIVE + carrying its index up front — but the server"
            " partitions the pool by mask identity, tiles each partition into fixed"
            " ~10M-read blocks, and dispatches one block work-ticket per block."
            " Per-sample completion is reconciled afterward. This shrinks the"
            " fan-out surface and gives each job a predictable input size. The mask"
            " a block produces is identical to the per-sample read-mask of the same"
            " config, so the two paths interoperate."
        ),
    )
    p_submit_block.add_argument(
        "--sequencing-run-idx",
        type=int,
        required=True,
        help="sequencing_run_idx the pool belongs to (the route checks pool↔run).",
    )
    p_submit_block.add_argument(
        "--sequenced-pool-idx",
        type=int,
        required=True,
        help="sequenced_pool_idx whose samples to tile into blocks.",
    )
    p_submit_block.add_argument(
        "--host-rype-reference-idx",
        type=int,
        default=None,
        help=(
            "ACTIVE host reference_idx whose rype (.ryxdi) index every sample in the"
            " pool is depleted against. Omit to plan the whole pool QC-only."
        ),
    )
    p_submit_block.add_argument(
        "--host-minimap2-reference-idx",
        type=int,
        default=None,
        help=(
            "Optional ACTIVE host reference_idx whose minimap2 (.mmi) index drives"
            " the second host-filter pass. Requires --host-rype-reference-idx."
        ),
    )
    p_submit_block.add_argument(
        "--force",
        action="store_true",
        help=(
            "Proceed even when some samples' intake human_filtering intent"
            " disagrees with this submission's pool-wide host-reference choice"
            " (printed as a warning instead of aborting)."
        ),
    )
    p_submit_block.add_argument(
        "--only-missing",
        action="store_true",
        help=(
            "Skip samples already carrying a completion gate for their resolved"
            " mask (applied server-side), so an interrupted plan re-runs only the"
            " gap. Off by default so a re-plan against a different host reference"
            " still tiles the whole pool."
        ),
    )
    p_submit_block.set_defaults(handler=_handle_submit_block_mask_pool)

    p_pool_completion = sub.add_parser(
        "pool-completion",
        help=(
            "Read a sequenced-pool's prep-generation completion rollup: how many"
            " of its samples have a COMPLETED fastq-to-parquet ticket."
        ),
        description=(
            "GET the pool's completion status: each non-retired sequenced_sample"
            " is classified by the state of its fastq-to-parquet work tickets"
            " (completed / in-flight / failed / not-submitted) and tallied into"
            " pool-level counts, with a `complete` flag set when every sample is"
            " COMPLETED. The SPP GenPrepFileJob end-state equivalent: it tells the"
            " operator whether the per-sample fan-out from submit-host-filter-pool"
            " has finished. Compute-on-read over the work tickets — it never"
            " drifts when a sample is re-processed or deleted."
        ),
    )
    p_pool_completion.add_argument(
        "--sequencing-run-idx",
        type=int,
        required=True,
        help="sequencing_run_idx the pool belongs to (the route checks pool↔run).",
    )
    p_pool_completion.add_argument(
        "--sequenced-pool-idx",
        type=int,
        required=True,
        help="sequenced_pool_idx whose completion rollup to read.",
    )
    p_pool_completion.set_defaults(handler=_handle_pool_completion)

    return parser
