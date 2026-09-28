"""submit-golay-demux — the amplicon (Rapid 16S) analogue of submit-bcl-convert.

16S EMP data enters Qiita as a multiplexed set of FASTQ (an I1 Golay index +
R1 [+ R2]) that still needs demultiplexing — unlike a PacBio run (per-barcode
uBAM, already demuxed off the instrument) or a bcl-convert Illumina run
(demuxed by sample-sheet index). So this command hands the whole pool to ONE
pool-scoped `golay-demux` work ticket, mirroring submit-bcl-convert's
run -> pool -> per-sample roster -> ticket shape.

The per-sample Golay roster (`barcode_map`) rides in action_context, built from
the preflight's `get_amplicon_sample_info`, so the demux job assigns each read to
a prep_sample without DB access. The run identity (external run id + instrument
model) comes from the preflight's `processing_run` row — the preflight is the
single source of truth for intake, and golay-demux has no run folder to read it
from (bcl-convert/pacbio do).
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


class _RunInfo(NamedTuple):
    instrument_run_id: str | None
    instrument_model: str | None


def _read_run_info(conn: sqlite3.Connection) -> _RunInfo:
    """The run's external id + instrument model from the preflight's processing_run.

    golay-demux has no run folder to read a run id from (bcl-convert reads it from
    the BCL folder, pacbio from the run folder), so the preflight is the default
    source — the preflight is the single source of truth for intake. Either value
    may be NULL; the caller resolves against its `--instrument-run-id` /
    `--instrument-model` overrides and fails loud if no run id is available from
    either source.
    """
    from run_preflight.db import get_single_run_idx  # noqa: PLC0415

    run_idx = get_single_run_idx(conn)
    row = conn.execute(
        "SELECT external_run_id, instrument_type FROM processing_run WHERE run_idx = ?",
        (run_idx,),
    ).fetchone()
    if row is None:
        return _RunInfo(instrument_run_id=None, instrument_model=None)
    return _RunInfo(
        instrument_run_id=str(row[0]) if row[0] else None,
        instrument_model=row[1] or None,
    )


def _read_amplicon_preflight_rows(
    preflight_blob: Path, parser: argparse.ArgumentParser
) -> tuple[list[_AmpliconPreflightRow], _RunInfo]:
    """One `_AmpliconPreflightRow` per amplicon_sample, plus the run identity.

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
        run_info = _read_run_info(conn)
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
    return parsed, run_info


def _validate_fastq_arg(
    parser: argparse.ArgumentParser, name: str, value: Path | None, *, required: bool
) -> Path | None:
    """A demux FASTQ path must be absolute and exist (the compute node reads it at
    the same absolute path — bind mounts expose host paths, they do not copy)."""
    if value is None:
        if required:
            parser.error(f"{name} is required")
        return None
    if not value.is_absolute():
        parser.error(f"{name} must be absolute, got {value}")
    if not value.is_file():
        parser.error(f"{name} {value} is not a regular file")
    return value


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
    identical to submit-bcl-convert; only the demux input (FASTQ vs a BCL folder)
    and the roster (Golay barcode_map vs a bcl-convert sample_map) differ.
    """
    index_reads = _validate_fastq_arg(
        parser, "--index-reads-path", args.index_reads_path, required=True
    )
    forward_reads = _validate_fastq_arg(
        parser, "--forward-reads-path", args.forward_reads_path, required=True
    )
    reverse_reads = _validate_fastq_arg(
        parser, "--reverse-reads-path", args.reverse_reads_path, required=False
    )

    if not args.preflight_blob.is_file():
        parser.error(f"--preflight-blob {args.preflight_blob} is not a regular file")
    blob_bytes = args.preflight_blob.read_bytes()
    if not blob_bytes:
        parser.error(f"--preflight-blob {args.preflight_blob} is empty")

    # Open the preflight locally and pull the per-sample rows + run identity before
    # any network call. Errors here are operator-actionable (parser.error, exit 2).
    preflight_rows, run_info = _read_amplicon_preflight_rows(args.preflight_blob, parser)

    # Run identity: the preflight's processing_run is the default source; the CLI
    # flags override it (and cover a preflight whose external_run_id is NULL). A run
    # id is required — a blank one would mint an unidentifiable sequencing_run.
    instrument_run_id = args.instrument_run_id or run_info.instrument_run_id
    instrument_model = args.instrument_model or run_info.instrument_model
    if not instrument_run_id:
        parser.error(
            f"--preflight-blob {args.preflight_blob}: processing_run carries no"
            " external_run_id; pass --instrument-run-id or populate the run id upstream"
        )

    run_body = SequencingRunCreateRequest(
        instrument_run_id=instrument_run_id,
        platform=Platform.ILLUMINA,
        instrument_model=instrument_model,
    ).model_dump(exclude_unset=True, mode="json")
    pool_body = SequencedPoolCreateRequest(
        run_preflight_blob=base64.b64encode(blob_bytes).decode("ascii"),
        run_preflight_filename=args.preflight_blob.name,
    ).model_dump(exclude_unset=True, mode="json")

    def _run(token: str) -> dict[str, Any]:
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
            "index_reads_path": str(index_reads),
            "forward_reads_path": str(forward_reads),
            "barcode_map": barcode_map,
        }
        if reverse_reads is not None:
            action_context["reverse_reads_path"] = str(reverse_reads)

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
