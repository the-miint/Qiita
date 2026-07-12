"""qiita user CLI — PacBio HiFi ingest submission.

`submit-pacbio-ingest` is the PacBio analogue of `submit-bcl-convert`
(cli/user/pool.py): one operator gesture that stands up the sequencing_run /
sequenced_pool / sequenced_sample roster from a kl-run-preflight blob and then
ingests each sample's reads. It differs from bcl-convert in two structural ways,
both because PacBio HiFi arrives **already demultiplexed** (one BAM per barcode)
rather than as a single BCL run bcl-convert demuxes in-workflow:

  1. There is no demux step. Each sample's pre-demuxed uBAM is loaded on its own
     by the existing per-`prep_sample` `bam-to-parquet` workflow, so this command
     FANS OUT one `bam-to-parquet` ticket per sample (like submit-host-filter-pool)
     instead of submitting a single pool-scoped ticket.
  2. The command must map each sample to its BAM file on disk. PacBio's HiFi
     demux writes `{run_folder}/{smartcell_well}/hifi_reads/{movie}.hifi_reads.{barcode}.bam`
     (plus a per-cell `*.unassigned.bam`). We key the BAM index on the barcode and
     FAIL LOUD if a barcode appears under more than one SMRT cell — barcode reuse
     across cells is real and cannot be disambiguated by barcode alone. The
     preflight now records the SMRT cell per sample (`smrt_cell`, from the reader),
     so a follow-up can key resolution on `(smrt_cell, barcode)` and drop that
     collision guard (it also implies moving the pool-item-id off the bare barcode
     — see `_index_run_bams`).

Preflight read: per-sample PacBio facts come from kl-run-preflight's
`get_pacbio_sample_info` (the analogue of the Illumina `get_illumina_sample_info`
that `pool._read_preflight_rows` uses) — see `_read_pacbio_preflight_rows`.
"""

from __future__ import annotations

import argparse
import base64
import sqlite3
import sys
from pathlib import Path
from typing import NamedTuple

import httpx
from qiita_common.api_paths import (
    PATH_WORK_TICKET_PREFIX,
)
from qiita_common.models import (
    Platform,
    ScopeTargetKind,
    SequencedPoolCreateRequest,
    SequencingRunCreateRequest,
    WorkTicketCreateRequest,
)

from .. import _common
from .pool import _provision_run_pool_roster

# action_id + version for the per-sample read loader this command fans out to.
# Pinned here so the CLI does not drift from the workflow YAML the operator's
# deploy syncs into qiita.action; PacBio ingest reuses `bam-to-parquet` verbatim
# (a pre-demuxed uBAM -> the DuckLake `read` table) rather than a bespoke job.
_BAM_TO_PARQUET_ACTION_ID = "bam-to-parquet"
_BAM_TO_PARQUET_ACTION_VERSION = "1.0.0"

# Preflight sheet_type values (kl-run-preflight [Header] SheetType). syndna is
# quantified only for the absquant protocol; a bare metaG sheet carries no syndna.
_SHEET_TYPE_ABSQUANT = "pacbio_absquant"
_SHEET_TYPE_METAG = "pacbio_metag"

# Per-cell reads PacBio's demux could not assign to a barcode; never a sample.
_UNASSIGNED_BAM_SUFFIX = ".unassigned.bam"

# Per-SMRT-cell subdirectory holding demultiplexed HiFi BAMs, and the filename
# field that names it: `{run}/{well}/hifi_reads/{movie}.hifi_reads.{barcode}.bam`.
_HIFI_READS_DIR = "hifi_reads"


class _PacbioPreflightRow(NamedTuple):
    """One PacBio sample pulled from the kl-run-preflight SQLite.

    `pacbio_sample_idx` is the sample's UNIQUE identifier within the preflight —
    the PacBio parallel of `illumina_sample_idx`, and the value used as the
    `sequenced_pool_item_id`. It is the only safe unique key: `sample_name` is a
    legacy, PII-bearing field that may be blank, and `biosample_accession` is not
    unique within a preflight (replicates share one). `barcode` is carried only to
    LOCATE the sample's BAM on disk — it is NOT unique across all PacBio protocols
    and is never used as an identity/pool-item-id.

    The project accessions are ENA **bioproject** accessions (what the study lookup
    route resolves), matching the Illumina row; `secondary_project_accessions` is
    populated for controls. The three protocol columns (`sheet_type`,
    `twist_adaptor_id`, `syndna_is_twisted`) feed the read-mask mask-chain
    derivation. `smrt_cell` is the SMRT-cell well (`smrt_cell_well_sample_id`, form
    `1_A01`) when the preflight records it (else None)."""

    pacbio_sample_idx: int
    barcode: str
    biosample_accession: str
    primary_project_accession: str
    secondary_project_accessions: list[str]
    human_filtering: bool
    sheet_type: str
    twist_adaptor_id: str | None
    syndna_is_twisted: bool | None
    # The SMRT-cell well from the preflight (None when it records none).
    smrt_cell: str | None = None


def _read_pacbio_preflight_rows(
    preflight_blob: Path, parser: argparse.ArgumentParser
) -> list[_PacbioPreflightRow]:
    """Open the preflight SQLite and return one `_PacbioPreflightRow` per PacBio sample.

    Reads via kl-run-preflight's `get_pacbio_sample_info` (the PacBio analogue of
    `get_illumina_sample_info` that `pool._read_preflight_rows` uses): per sample it
    returns the biosample + primary/secondary **bioproject** accessions and a
    `PacbioSampleRow` (barcode, twist, syndna, smrt_cell, movie_context). It
    validates + raises on any missing required accession, and resolves a control's
    project to the plate primary itself, so this wrapper adds no accession SQL.
    The `pacbio_sample_idx` it returns is the sample's unique id (used as the
    pool-item-id). The one intake fact the accessor omits — the project's
    `human_filtering` flag — is read from the canonical `run_pacbio_sample` view +
    the `project` table. (Reading the run-preflight schema directly here is a known
    smell — the reader should own it; a dedicated accessor is the follow-up.)

    Operator-actionable errors (not a SQLite, a non-PacBio sheet, an empty sample
    set, a missing accession, or an impossible protocol combo) raise via
    `parser.error` so the CLI surfaces one stderr line and exits 2 before any
    network call — matching `_read_preflight_rows`.
    """
    from run_preflight import get_pacbio_sample_info, open_db_file  # noqa: PLC0415
    from run_preflight.db import get_run_legacy_format, get_single_run_idx  # noqa: PLC0415

    try:
        conn = open_db_file(preflight_blob)
    except sqlite3.DatabaseError as exc:
        parser.error(f"--preflight-blob {preflight_blob}: not a readable SQLite file: {exc}")
    try:
        run_idx = get_single_run_idx(conn)
        legacy_format = get_run_legacy_format(conn.cursor(), run_idx)
        if legacy_format is None:
            parser.error(
                f"--preflight-blob {preflight_blob}: run has no legacy sheet format;"
                " verify the file is a kl-run-preflight SQLite"
            )
        sheet_type = legacy_format[1]
        # Effective project_name per pacbio_sample (the accessor omits it): the
        # canonical run-scoped view resolves it (incl. a control's plate primary).
        project_by_idx = {
            idx: project
            for idx, project in conn.execute(
                "SELECT pacbio_sample_idx, project_name FROM run_pacbio_sample WHERE run_idx = ?",
                (run_idx,),
            ).fetchall()
        }
        filtering_by_project = {
            name: bool(flag)
            for name, flag in conn.execute(
                "SELECT project_name, human_filtering FROM project"
            ).fetchall()
        }
        # The accessor's ValueError is meaning-specific (a missing biosample /
        # bioproject accession, or a control/project invariant violation) and
        # carries a clear per-sample message, so it is caught SEPARATELY from the
        # generic-preflight errors below (a multi-run blob from get_single_run_idx,
        # or a metadata-query shape drift) and surfaced verbatim.
        try:
            infos = get_pacbio_sample_info(conn)
        except ValueError as exc:
            parser.error(f"--preflight-blob {preflight_blob}: {exc}")
    except (sqlite3.DatabaseError, ValueError) as exc:
        parser.error(
            f"--preflight-blob {preflight_blob}: preflight query failed ({exc});"
            " verify the file is a kl-run-preflight PacBio SQLite"
        )
    finally:
        conn.close()

    # Fail loud on a non-PacBio sheet: an Illumina preflight fed here would also
    # surface as "no pacbio_sample rows", but naming the actual sheet_type is a
    # clearer operator signal (and guards the syndna/lima derivation downstream,
    # which only defines these two sheet types).
    if sheet_type not in (_SHEET_TYPE_ABSQUANT, _SHEET_TYPE_METAG):
        parser.error(
            f"--preflight-blob {preflight_blob}: sheet_type {sheet_type!r} is not a"
            f" PacBio sheet ({_SHEET_TYPE_ABSQUANT!r} or {_SHEET_TYPE_METAG!r});"
            " submit-pacbio-ingest requires a PacBio preflight"
        )

    if not infos:
        parser.error(
            f"--preflight-blob {preflight_blob} contains no pacbio_sample rows;"
            " a PacBio ingest needs at least one demultiplexed sample"
        )

    parsed: list[_PacbioPreflightRow] = []
    for info in infos:
        pbs = info.kind_row
        # The accessor's sample_idx set is a subset of the view (same run, do_not_use
        # excluded), so a miss cannot happen for a well-formed preflight — fail loud
        # rather than silently default the project.
        if info.sample_idx not in project_by_idx:
            parser.error(
                f"--preflight-blob {preflight_blob}: pacbio_sample_idx {info.sample_idx}"
                " is absent from the run_pacbio_sample view — the preflight is"
                " internally inconsistent"
            )
        project_name = project_by_idx[info.sample_idx]
        if not pbs.barcode_id:
            parser.error(
                f"--preflight-blob {preflight_blob}: pacbio_sample_idx {info.sample_idx}"
                " carries no barcode_id; a PacBio sample cannot be located on disk"
                " without it"
            )
        row = _PacbioPreflightRow(
            pacbio_sample_idx=info.sample_idx,
            barcode=pbs.barcode_id,
            biosample_accession=info.biosample_accession,
            primary_project_accession=info.primary_bioproject_accession,
            secondary_project_accessions=list(info.secondary_bioproject_accessions),
            # project_name is always a real project here (the view resolves it,
            # incl. a control's plate primary), so the default is unreachable —
            # but keep it the privacy-SAFE direction (filter), matching the schema
            # (project.human_filtering NOT NULL DEFAULT 1).
            human_filtering=filtering_by_project.get(project_name, True),
            # sheet_type is a RUN-level property (a run is one protocol), read once
            # from the legacy format above and stamped on every row — there is no
            # per-sample sheet_type.
            sheet_type=sheet_type,
            twist_adaptor_id=pbs.twist_adaptor_id or None,
            # Already coerced to bool | None by the accessor.
            syndna_is_twisted=pbs.syndna_is_twisted,
            smrt_cell=pbs.smrt_cell_well_sample_id,
        )
        _validate_pacbio_protocol(row, preflight_blob, parser)
        parsed.append(row)
    return parsed


def _validate_pacbio_protocol(
    row: _PacbioPreflightRow, preflight_blob: Path, parser: argparse.ArgumentParser
) -> None:
    """Fail-fast on protocol-column combinations that cannot describe a real run.

    Reads the same three columns the read-mask submission later derives
    `syndna_enabled` / `lima_enabled` from, so an incoherent preflight is caught
    at ingest rather than surfacing as a confused mask chain downstream. Both
    guards key on `syndna_is_twisted is True` — a *twisted-syndna* claim:
      * a twisted syndna requires a twist adapter to have been attached, so it
        cannot appear with an empty `twist_adaptor_id`;
      * a bare metaG sheet quantifies no syndna, so it cannot carry a twisted one.
    `False` (an untwisted syndna, as in protocol 5) and `None` (no syndna, e.g.
    metaG) are both valid and never trip these guards.
    """
    if row.syndna_is_twisted is True and not row.twist_adaptor_id:
        parser.error(
            f"--preflight-blob {preflight_blob}: pacbio_sample_idx {row.pacbio_sample_idx}"
            " marks its syndna twisted with no twist_adaptor_id; syndna can only be"
            " twisted when a twist adapter was attached"
        )
    if row.syndna_is_twisted is True and row.sheet_type == _SHEET_TYPE_METAG:
        parser.error(
            f"--preflight-blob {preflight_blob}: pacbio_sample_idx {row.pacbio_sample_idx}"
            f" marks its syndna twisted on a {_SHEET_TYPE_METAG!r} sheet, which"
            " quantifies no syndna; only the absquant protocol carries syndna"
        )


def _index_run_bams(run_folder: Path) -> tuple[dict[str, Path], set[str]]:
    """Index a PacBio run folder's per-barcode HiFi BAMs.

    Globs `{run_folder}/*/hifi_reads/*.bam` — each SMRT cell is a well
    subdirectory (`1_A01`, `1_B01`, ...) holding its demultiplexed reads — and
    keys each BAM on its barcode, the second-to-last dot field of the filename
    (`m84137_..._s1.hifi_reads.bc2073.bam` -> `bc2073`). Per-cell
    `*.unassigned.bam` files are skipped (reads with no barcode are not samples).

    Returns `(index, duplicated)`: `index` maps barcode -> BAM for every barcode
    that resolves to exactly one file; `duplicated` is the set of barcodes seen
    under more than one SMRT cell. A duplicated barcode is left OUT of `index` and
    is a hard error at resolution time — barcode reuse across SMRT cells within a
    run is real (e.g. bc2083 under both 1_B01 and 1_C01) and cannot be
    disambiguated without the SMRT cell. This is the graceful-degradation rule:
    unique barcodes just resolve; a collision on a barcode a sample actually needs
    fails loud rather than silently binding the wrong cell's reads. (The preflight
    now carries a SMRT-cell field; once it is populated, key on `(smrt_cell, barcode)`
    — matching the well subdirectory or the movie name's `s#` token — and this
    collision set becomes empty.)
    """
    index: dict[str, Path] = {}
    duplicated: set[str] = set()
    for bam in sorted(run_folder.glob(f"*/{_HIFI_READS_DIR}/*.bam")):
        if bam.name.endswith(_UNASSIGNED_BAM_SUFFIX):
            continue
        parts = bam.name.split(".")
        # Require the exact demux shape "<movie>.hifi_reads.<barcode>.bam" so a
        # non-demuxed combined BAM ("<movie>.hifi_reads.bam") isn't indexed under a
        # spurious barcode ("hifi_reads"). ["m84_s1", "hifi_reads", "bc2073", "bam"].
        if len(parts) < 4 or parts[-3] != _HIFI_READS_DIR:
            continue
        barcode = parts[-2]
        if barcode in index or barcode in duplicated:
            duplicated.add(barcode)
            index.pop(barcode, None)
        else:
            index[barcode] = bam
    return index, duplicated


def _resolve_sample_bams(
    rows: list[_PacbioPreflightRow],
    run_folder: Path,
    parser: argparse.ArgumentParser,
) -> dict[str, Path]:
    """Resolve every sample's absolute BAM path before any network call.

    Returns barcode -> absolute BAM path. Barcode is the sample's BAM-locating key
    (NOT its identity — that is `pacbio_sample_idx`), and it is not guaranteed
    unique across PacBio protocols, so a barcode shared by two samples, or one that
    maps to BAMs in more than one SMRT cell, is AMBIGUOUS: without the SMRT cell we
    cannot bind each sample to its own reads, so it is a hard error (not a silent
    wrong-BAM bind). Fails via `parser.error` (exit 2) on an empty run folder, a
    sample whose barcode has no BAM, or either ambiguity — one actionable error
    instead of N FAILED `bam-to-parquet` tickets.
    """
    index, duplicated = _index_run_bams(run_folder)
    if not index and not duplicated:
        parser.error(
            f"--run-folder {run_folder} contains no HiFi BAMs (expected */hifi_reads/*.bam)"
        )
    # A barcode two samples both claim can't be split between them without the SMRT
    # cell — treat it like a cross-cell collision (ambiguous), not a silent shared BAM.
    barcode_rows: dict[str, list[int]] = {}
    for row in rows:
        barcode_rows.setdefault(row.barcode, []).append(row.pacbio_sample_idx)
    shared = {bc for bc, idxs in barcode_rows.items() if len(idxs) > 1}

    resolved: dict[str, Path] = {}
    missing: list[str] = []
    ambiguous: list[str] = []
    for row in rows:
        label = f"pacbio_sample_idx {row.pacbio_sample_idx} ({row.barcode})"
        if row.barcode in duplicated or row.barcode in shared:
            ambiguous.append(label)
        elif row.barcode not in index:
            missing.append(label)
        else:
            # .absolute(), not .resolve(): the orchestrator binds the BAM's parent
            # dir by its given absolute path, so dereferencing symlinks here could
            # yield a path outside that bind mount (invisible to the compute node).
            resolved[row.barcode] = index[row.barcode].absolute()
    if ambiguous:
        parser.error(
            f"--run-folder {run_folder}: barcode(s) reused across samples or SMRT"
            f" cells for {len(ambiguous)} sample(s) cannot be disambiguated without"
            f" SMRT-cell information: {', '.join(ambiguous)}"
        )
    if missing:
        parser.error(
            f"--run-folder {run_folder}: no HiFi BAM found for {len(missing)}"
            f" sample(s): {', '.join(missing)}"
        )
    return resolved


def _handle_submit_pacbio_ingest(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Bundle the PacBio HiFi ingest flow into one operator gesture.

    1. Parse the preflight (`_read_pacbio_preflight_rows`) and resolve every
       sample's BAM on disk (`_resolve_sample_bams`) — both before any network
       call, so a bad preflight / run folder exits 2 with no side effects.
    2. Resolve every biosample + study accession up front; a miss on either prints
       one combined block and exits 1 with nothing created.
    3. POST /sequencing-run (platform PACBIO_SMRT; instrument_run_id +
       instrument_model from args, since PacBio has no RunInfo.xml) and
       /sequencing-run/{run}/sequenced-pool (attaching the preflight blob). The
       blob is attached per the store-once design so a later read-mask submission
       can re-read the protocol columns (human_filtering, twist_adaptor_id,
       syndna_is_twisted) server-side and derive the mask chain, as
       submit-host-filter-pool already does for the Illumina blob. No PacBio
       server-side re-read parser exists yet: human_filtering is echoed in this
       command's summary for operator reference only and is not forwarded onward.
    4. GET the pool roster and create only the MISSING sequenced-samples
       (`sequenced_pool_item_id = pacbio_sample_idx`, the sample's unique preflight
       id — the exact analogue of bcl-convert's illumina_sample_idx; the barcode is
       NOT used here, it is not unique across PacBio protocols). The composer 409s
       on a duplicate (pool, item_id), so create-missing (not blind-POST) is what
       makes a retry converge instead of aborting on the first already-created sample.
    5. Fan out one `bam-to-parquet` ticket per sample (scope prep_sample,
       action_context {bam_path, expect_unaligned: true}). Per-sample resilient:
       one sample's ticket failure is recorded and the fan-out continues. A 409
       (sample already COMPLETED under disallow-without-delete, or already
       in-flight) is recorded as SKIPPED — the convergence signal, not a failure —
       so re-running to retry a failed sample never reports the finished ones as
       failures. The command exits non-zero only if a real (non-409) failure
       occurred (mirrors submit-host-filter-pool).

    Convergent retry: find-or-create on the run + pool, create-missing on the
    roster (step 4), and the 409-as-skip fan-out (step 5) together mean re-running
    the identical gesture after a partial failure reuses everything already made,
    skips the already-done samples (exit 0), and only re-submits the still-missing
    / previously-FAILED ones (the route resets a FAILED ticket). --force is the
    separate, deliberate re-ingest path (it re-registers reads → lake duplicates),
    NOT the retry route. All calls share one PAT.
    """
    if not args.run_folder.is_absolute():
        parser.error(f"--run-folder must be absolute, got {args.run_folder}")
    if not args.run_folder.is_dir():
        parser.error(f"--run-folder {args.run_folder} is not a directory")
    if not args.preflight_blob.is_file():
        parser.error(f"--preflight-blob {args.preflight_blob} is not a regular file")
    blob_bytes = args.preflight_blob.read_bytes()
    if not blob_bytes:
        parser.error(f"--preflight-blob {args.preflight_blob} is empty")

    preflight_rows = _read_pacbio_preflight_rows(args.preflight_blob, parser)
    # Resolve BAMs before any network call — a missing/ambiguous BAM is
    # operator-actionable and must not create a half-populated pool.
    bam_by_barcode = _resolve_sample_bams(preflight_rows, args.run_folder, parser)

    run_body = SequencingRunCreateRequest(
        instrument_run_id=args.instrument_run_id,
        platform=Platform.PACBIO_SMRT,
        instrument_model=args.instrument_model,
    ).model_dump(exclude_unset=True, mode="json")
    pool_body = SequencedPoolCreateRequest(
        run_preflight_blob=base64.b64encode(blob_bytes).decode("ascii"),
        run_preflight_filename=args.preflight_blob.name,
    ).model_dump(exclude_unset=True, mode="json")

    def _run(token: str) -> dict:
        # Shared run → pool → roster provisioning (create-missing; fails fast on an
        # unresolved accession). PacBio keys the pool-item-id on pacbio_sample_idx —
        # the sample's unique preflight id (the barcode is only the BAM-locating key
        # and is not unique across protocols).
        provision = _provision_run_pool_roster(
            args.base_url,
            token,
            preflight_rows=preflight_rows,
            run_body=run_body,
            pool_body=pool_body,
            prep_protocol_idx=args.prep_protocol_idx,
            pool_item_id=lambda row: str(row.pacbio_sample_idx),
            row_label=lambda row: f"pacbio_sample_idx {row.pacbio_sample_idx}",
            row_noun="sample",
        )
        sequencing_run_idx = provision.sequencing_run_idx
        sequenced_pool_idx = provision.sequenced_pool_idx
        per_sample = [
            {
                "pacbio_sample_idx": s.row.pacbio_sample_idx,
                "barcode": s.row.barcode,
                "bam_path": str(bam_by_barcode[s.row.barcode]),
                "biosample_idx": s.biosample_idx,
                "primary_study_idx": s.primary_study_idx,
                "human_filtering": s.row.human_filtering,
                "prep_sample_idx": s.prep_sample_idx,
                "sequenced_sample_idx": s.sequenced_sample_idx,
                "reused": s.reused,
            }
            for s in provision.samples
        ]

        # Fan out one bam-to-parquet ingest ticket per sample. Per-sample
        # resilient: a single ticket's failure is recorded and the loop CONTINUES,
        # so one bad sample never strands the rest (mirrors submit-host-filter-pool).
        #
        # A 409 is NOT a failure — it is the convergence signal: a sample already
        # COMPLETED (disallow-without-delete) or already in-flight
        # (PENDING/QUEUED/PROCESSING) rejects a duplicate submit with 409. That is
        # exactly "already done / already running", so we record it as SKIPPED and
        # do NOT count it toward the non-zero exit — re-running the gesture to
        # retry a failed sample must not report the finished ones as failures.
        # (A FAILED sample's ticket is reset by the route and re-submitted 201,
        # so it converges without a skip. --force is the separate, deliberate
        # re-ingest path and intentionally NOT the recovery route here.)
        failures: list[dict] = []
        skipped: list[dict] = []
        for entry in per_sample:
            ticket_body = WorkTicketCreateRequest(
                action_id=_BAM_TO_PARQUET_ACTION_ID,
                action_version=_BAM_TO_PARQUET_ACTION_VERSION,
                scope_target={
                    "kind": ScopeTargetKind.PREP_SAMPLE.value,
                    "prep_sample_idx": entry["prep_sample_idx"],
                },
                action_context={
                    "bam_path": entry["bam_path"],
                    "expect_unaligned": True,
                },
                force=args.force,
            ).model_dump(exclude_unset=True, mode="json")
            try:
                ticket_resp, _status = _common.call_with_status(
                    "POST", args.base_url, token, PATH_WORK_TICKET_PREFIX, json=ticket_body
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 409:
                    # Already ingested (COMPLETED) or already in-flight — converged,
                    # not failed. Skip without contributing to the non-zero exit.
                    skipped.append(
                        {
                            "prep_sample_idx": entry["prep_sample_idx"],
                            "barcode": entry["barcode"],
                            "reason": exc.response.text[:500],
                        }
                    )
                    continue
                failures.append(
                    {
                        "prep_sample_idx": entry["prep_sample_idx"],
                        "barcode": entry["barcode"],
                        "status_code": exc.response.status_code,
                        "error": exc.response.text[:500],
                    }
                )
                continue
            except httpx.HTTPError as exc:
                failures.append(
                    {
                        "prep_sample_idx": entry["prep_sample_idx"],
                        "barcode": entry["barcode"],
                        "status_code": None,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            entry["work_ticket_idx"] = ticket_resp.get("work_ticket_idx")

        summary = {
            "sequencing_run": {
                "sequencing_run_idx": sequencing_run_idx,
                "status": "created" if provision.run_status == 201 else "reused",
            },
            "sequenced_pool": {
                "sequenced_pool_idx": sequenced_pool_idx,
                "status": "created" if provision.pool_status == 201 else "reused",
            },
            "samples_submitted": len(per_sample) - len(failures) - len(skipped),
            "samples_skipped": len(skipped),
            "samples_failed": len(failures),
            "skipped": skipped,
            "failed": failures,
            "per_sample": per_sample,
            "instrument_run_id": args.instrument_run_id,
            "instrument_model": args.instrument_model,
            "prep_protocol_idx": args.prep_protocol_idx,
        }
        if failures:
            import json  # noqa: PLC0415

            # Partial fan-out: emit the summary to stderr (it's an error report —
            # the success path returns it for stdout) and exit non-zero so a
            # scripted caller can't mistake a partial run for success.
            print(json.dumps(summary, indent=2), file=sys.stderr)
            sys.exit(1)
        return summary

    return _common.run_http_subcommand(_run)
