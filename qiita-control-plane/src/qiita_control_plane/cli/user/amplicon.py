"""submit-golay-demux — the amplicon (Rapid 16S) analogue of submit-bcl-convert.

The submitter names a run id; the control plane resolves it to the BCL run
folder (POST /run-folder/inspect) and reads the instrument identity from its
RunInfo.xml, just like submit-bcl-convert. The whole pool goes to ONE
pool-scoped `golay-demux` work ticket, which converts with a no-index dummy sheet
(every read to Undetermined, the Golay I1 emitted) and demuxes on the Golay
barcode.

The per-sample Golay roster (`barcode_map`) rides in action_context, built from
the preflight's `get_amplicon_sample_info`, so the demux job assigns each read to
a prep_sample without DB access. The preflight supplies only the roster and the
sample accessions; the run identity comes from the run folder.
"""

from __future__ import annotations

import argparse
import base64
import sqlite3
from pathlib import Path
from typing import Any, NamedTuple

from qiita_common.api_paths import PATH_WORK_TICKET_PREFIX
from qiita_common.models import (
    Platform,
    ScopeTargetKind,
    SequencedPoolCreateRequest,
    SequencingRunCreateRequest,
    WorkTicketCreateRequest,
)

from .. import _common
from ._helpers import _resolve_run_folder
from .pool import _provision_run_pool_roster

# Pinned to the workflow YAML the operator's deploy syncs into qiita.action; keep
# in lockstep with workflows/golay-demux/1.0.0.yaml.
_GOLAY_DEMUX_ACTION_ID = "golay-demux"
_GOLAY_DEMUX_ACTION_VERSION = "1.0.0"


class _AmpliconPreflightRow(NamedTuple):
    """One amplicon_sample pulled from the kl-run-preflight SQLite.

    `prepped_sample_idx` is the sample's UNIQUE identifier within the preflight
    (amplicon_sample has no surrogate key) and the value used as the
    `sequenced_pool_item_id`. `barcode` + `barcodes_are_rc` are the Golay roster the
    demux job matches each read's index against; `barcodes_are_rc` is per-sample
    provenance (whether the stored barcode is the reverse complement of the read
    index). The project accessions are ENA **bioproject** accessions the study
    lookup route resolves, matching the Illumina/PacBio rows;
    `secondary_project_accessions` is populated for controls.
    """

    prepped_sample_idx: int
    barcode: str
    barcodes_are_rc: bool
    biosample_accession: str
    primary_project_accession: str
    secondary_project_accessions: list[str]


def _read_amplicon_preflight_rows(
    preflight_blob: Path, parser: argparse.ArgumentParser
) -> list[_AmpliconPreflightRow]:
    """One `_AmpliconPreflightRow` per amplicon_sample.

    Operator-actionable errors (not a SQLite, empty sample set, a missing barcode,
    or a missing biosample/primary-project accession) raise via parser.error so the
    CLI surfaces one stderr line and exits 2 before any network call — matching
    `pool.py::_read_preflight_rows`.
    """
    from run_preflight import get_amplicon_sample_info, load_file  # noqa: PLC0415

    try:
        conn = load_file(str(preflight_blob))
    except (FileNotFoundError, sqlite3.DatabaseError, ValueError) as exc:
        # load_file's error contract: FileNotFoundError (no such path),
        # sqlite3.DatabaseError (truncated SQLite), ValueError (not a SQLite / bad
        # legacy CSV). All mean "not a usable preflight file" here.
        parser.error(f"--preflight-blob {preflight_blob}: not a readable SQLite file: {exc}")
    try:
        infos = get_amplicon_sample_info(conn)
    except (sqlite3.DatabaseError, ValueError) as exc:
        parser.error(
            f"--preflight-blob {preflight_blob}: preflight query failed ({exc});"
            " verify the file is a kl-run-preflight amplicon SQLite"
        )
    finally:
        conn.close()

    if not infos:
        parser.error(
            f"--preflight-blob {preflight_blob} contains no amplicon_sample rows;"
            " a golay-demux submission needs at least one sample to demultiplex"
        )

    parsed: list[_AmpliconPreflightRow] = []
    for info in infos:
        acr = info.kind_row
        if not acr.barcode:
            parser.error(
                f"--preflight-blob {preflight_blob}: prepped_sample_idx {info.sample_idx}"
                " carries no Golay barcode; a sample cannot be demultiplexed without it"
            )
        if not info.biosample_accession:
            parser.error(
                f"--preflight-blob {preflight_blob}: prepped_sample_idx {info.sample_idx}"
                " carries no biosample_accession; populate upstream before re-submitting"
            )
        if not info.primary_bioproject_accession:
            parser.error(
                f"--preflight-blob {preflight_blob}: prepped_sample_idx {info.sample_idx}"
                " carries no primary bioproject accession; populate upstream before"
                " re-submitting"
            )
        parsed.append(
            _AmpliconPreflightRow(
                prepped_sample_idx=info.sample_idx,
                barcode=acr.barcode,
                barcodes_are_rc=bool(acr.barcodes_are_rc),
                biosample_accession=info.biosample_accession,
                primary_project_accession=info.primary_bioproject_accession,
                secondary_project_accessions=list(info.secondary_bioproject_accessions),
            )
        )
    return parsed


def _handle_submit_golay_demux(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Bundle the golay-demux submission into one operator gesture.

    1-2. POST /sequencing-run + /sequenced-pool — run identity read from the
         preflight's processing_run; the blob is attached to the pool.
    3.   For each amplicon_sample: POST the sequenced-sample composer with the
         resolved biosample_idx + study_idx, the operator's prep_protocol_idx, and
         ``sequenced_pool_item_id = str(prepped_sample_idx)``.
    4.   POST /work-ticket — one pool-scoped golay-demux ticket whose
         action_context carries the multiplexed FASTQ paths and the barcode_map.

    Steps 1-3 (accession resolution, create-missing roster, fail-fast on an
    unresolved accession) are the shared `_provision_run_pool_roster` gesture,
    identical to submit-bcl-convert; only the roster (Golay barcode_map vs a
    bcl-convert sample_map) differs. The run folder is resolved from the run id
    server-side, exactly as submit-bcl-convert resolves its BCL folder.
    """
    if not args.preflight_blob.is_file():
        parser.error(f"--preflight-blob {args.preflight_blob} is not a regular file")
    blob_bytes = args.preflight_blob.read_bytes()
    if not blob_bytes:
        parser.error(f"--preflight-blob {args.preflight_blob} is empty")

    # Open the preflight locally and pull the per-sample rows before any network
    # call. Errors here are operator-actionable (parser.error, exit 2).
    preflight_rows = _read_amplicon_preflight_rows(args.preflight_blob, parser)

    pool_body = SequencedPoolCreateRequest(
        run_preflight_blob=base64.b64encode(blob_bytes).decode("ascii"),
        run_preflight_filename=args.preflight_blob.name,
    ).model_dump(exclude_unset=True, mode="json")

    def _run(token: str) -> dict[str, Any]:
        # Resolve the run folder from its run id on the control plane, and read the
        # instrument identity from its RunInfo.xml — the same server-side read
        # submit-bcl-convert does, so the gesture needs no cluster mount and the
        # submitter never handles a host path.
        inspected = _resolve_run_folder(
            args.base_url, token, args.instrument_run_id, Platform.ILLUMINA
        )
        assert inspected.illumina is not None  # platform=illumina always populates it
        instrument_run_id = inspected.illumina.instrument_run_id
        instrument_model = inspected.illumina.instrument_model
        run_body = SequencingRunCreateRequest(
            instrument_run_id=instrument_run_id,
            platform=Platform.ILLUMINA,
            instrument_model=instrument_model,
        ).model_dump(exclude_unset=True, mode="json")

        # Shared run -> pool -> roster provisioning (create-missing; fails fast on an
        # unresolved accession). Amplicon keys the pool-item-id on prepped_sample_idx.
        provision = _provision_run_pool_roster(
            args.base_url,
            token,
            preflight_rows=preflight_rows,
            run_body=run_body,
            pool_body=pool_body,
            prep_protocol_idx=args.prep_protocol_idx,
            pool_item_id=lambda row: str(row.prepped_sample_idx),
            row_label=lambda row: f"prepped_sample_idx={row.prepped_sample_idx}",
            row_noun="amplicon_sample",
        )
        sequencing_run_idx = provision.sequencing_run_idx
        sequenced_pool_idx = provision.sequenced_pool_idx

        # The demux roster: each provisioned prep_sample_idx paired with its Golay
        # barcode + orientation, carried off `s.row` (the original preflight row).
        barcode_map = [
            {
                "prep_sample_idx": s.prep_sample_idx,
                "barcode": s.row.barcode,
                "barcodes_are_rc": s.row.barcodes_are_rc,
            }
            for s in provision.samples
        ]
        per_sample_results = [
            {
                "prepped_sample_idx": s.row.prepped_sample_idx,
                "biosample_accession": s.row.biosample_accession,
                "biosample_idx": s.biosample_idx,
                "primary_study_idx": s.primary_study_idx,
                "secondary_study_idxs": s.secondary_study_idxs,
                "prep_sample_idx": s.prep_sample_idx,
                "sequenced_sample_idx": s.sequenced_sample_idx,
                "barcode": s.row.barcode,
                "barcodes_are_rc": s.row.barcodes_are_rc,
            }
            for s in provision.samples
        ]

        action_context: dict[str, Any] = {
            # The normalized form the inspect gate resolved from the run id, not a
            # value the submitter typed. amplicon selects bcl_convert_prep's
            # dummy-sheet path.
            "bcl_input_dir": inspected.path,
            "amplicon": True,
            "barcode_map": barcode_map,
        }

        ticket_body = WorkTicketCreateRequest(
            action_id=_GOLAY_DEMUX_ACTION_ID,
            action_version=_GOLAY_DEMUX_ACTION_VERSION,
            scope_target={
                "kind": ScopeTargetKind.SEQUENCED_POOL.value,
                "sequenced_pool_idx": sequenced_pool_idx,
                "sequencing_run_idx": sequencing_run_idx,
            },
            action_context=action_context,
            force=args.force,
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
                "status": "created" if provision.run_status == 201 else "reused",
            },
            "sequenced_pool": {
                "sequenced_pool_idx": sequenced_pool_idx,
                "status": "created" if provision.pool_status == 201 else "reused",
            },
            "sequenced_samples": per_sample_results,
            "work_ticket": ticket_resp,
            # Echo the run identity + args the orchestrator side will see.
            "instrument_run_id": instrument_run_id,
            "instrument_model": instrument_model,
            "prep_protocol_idx": args.prep_protocol_idx,
        }

    return _common.run_http_subcommand(_run)
