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
     (plus a per-cell `*.unassigned.bam`). Today the preflight carries no SMRT-cell
     column, so we key the BAM index on the barcode alone and FAIL LOUD if a
     barcode appears under more than one SMRT cell (barcode reuse across cells is
     real and cannot be disambiguated without the cell). When the preflight grows a
     SMRT-cell field, `_index_run_bams` keys on `(smartcell, barcode)` and the
     collision guard falls away — see `_index_run_bams`.

PROVISIONAL preflight read: `kl-run-preflight` exposes a public reader for
Illumina samples (`get_illumina_sample_info`) but NOT for PacBio — confirmed
absent on the pinned build AND on `main`. `_read_pacbio_preflight_rows` therefore
reads the `pacbio_sample` table with a direct join that mirrors the Illumina
reader's shape. It is isolated as the single swap point: when
`get_pacbio_sample_info` ships upstream, only that function changes. See its
docstring.
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
    PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION,
    PATH_BIOSAMPLE_PREFIX,
    PATH_SEQUENCED_SAMPLE_FROM_RUN,
    PATH_SEQUENCED_SAMPLE_LIST_BY_POOL,
    PATH_SEQUENCING_RUN_PREFIX,
    PATH_SEQUENCING_RUN_SEQUENCED_POOL,
    PATH_STUDY_LOOKUP_BY_ACCESSION,
    PATH_STUDY_PREFIX,
    PATH_WORK_TICKET_PREFIX,
)
from qiita_common.models import (
    BiosampleLookupByAccessionRequest,
    Platform,
    ScopeTargetKind,
    SequencedPoolCreateRequest,
    SequencedSampleCreateRequest,
    SequencingRunCreateRequest,
    StudyLookupByAccessionRequest,
    WorkTicketCreateRequest,
)

from .. import _common
from .pool import _dedup_accessions, _lookup_accessions, _print_missing_accession_error

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


class _PacbioPreflightRow(NamedTuple):
    """One PacBio sample pulled from the kl-run-preflight SQLite.

    Mirrors `pool._PreflightRow` for the fields the submit flow shares
    (biosample / project accessions + the intake `human_filtering` intent), plus
    the PacBio-specific `barcode` (used to locate the sample's BAM and as the
    pool-item-id) and the three protocol-determining columns
    (`sheet_type`, `twist_adaptor_id`, `syndna_is_twisted`) the read-mask
    submission later derives the mask chain from. `secondary_project_accessions`
    is a list for parity with the Illumina row; PacBio control/secondary-study
    resolution is not yet wired, so it is always empty today (see
    `_read_pacbio_preflight_rows`).

    `smrt_cell` is RESERVED for the announced upstream reader: once populated it
    lets BAM resolution key on `(smrt_cell, barcode)` instead of failing on
    barcode reuse across cells. It defaults None until then (barcode-only path)."""

    sample_name: str
    barcode: str
    biosample_accession: str
    primary_project_accession: str
    secondary_project_accessions: list[str]
    human_filtering: bool
    sheet_type: str
    twist_adaptor_id: str | None
    syndna_is_twisted: bool | None
    # Reserved for the upstream reader; unused until then (default keeps every
    # current constructor + the barcode-only resolution path unchanged).
    smrt_cell: str | None = None


# The verified pacbio_sample join. pacbio_sample keys on prepped_sample_idx;
# prepped_sample -> compression_sample carries the run scope (cs.run_idx) and the
# link out to input_sample (sample_name + biosample_accession + plate + project).
# This is the same table graph get_illumina_sample_info walks, minus the
# run_illumina_sample entry point PacBio has no analogue for.
#
# The project is COALESCE(own project, plate primary): a standard sample owns its
# project (input_sample.project_idx); a control (blank/positive) has a NULL
# project_idx and inherits the plate's primary_project_idx — the same fallback
# get_illumina_sample_info applies. Without it, every control row would resolve to
# a NULL accession and fail-fast. Verified against kl-run-preflight's own
# good_pacbio_absquantv11.csv fixture (which carries a control blank).
_PACBIO_SAMPLE_JOIN = """
    SELECT
        COALESCE(prs.sample_name, ins.sample_name) AS sample_name,
        pbs.barcode_id,
        pbs.twist_adaptor_id,
        pbs.syndna_is_twisted,
        ins.biosample_accession,
        COALESCE(own_proj.external_project_id, primary_proj.external_project_id)
            AS external_project_id,
        COALESCE(own_proj.human_filtering, primary_proj.human_filtering)
            AS human_filtering
    FROM pacbio_sample pbs
    JOIN prepped_sample prs
        ON prs.prepped_sample_idx = pbs.prepped_sample_idx
    JOIN compression_sample cs
        ON prs.compression_sample_idx = cs.compression_sample_idx
    JOIN input_sample ins
        ON cs.input_sample_idx = ins.input_sample_idx
    JOIN input_plate ip
        ON ins.input_plate_idx = ip.input_plate_idx
    JOIN project primary_proj
        ON ip.primary_project_idx = primary_proj.project_idx
    LEFT JOIN project own_proj
        ON ins.project_idx = own_proj.project_idx
    WHERE cs.run_idx = ?
      AND ins.do_not_use = 0
    ORDER BY sample_name
"""


def _read_pacbio_preflight_rows(
    preflight_blob: Path, parser: argparse.ArgumentParser
) -> list[_PacbioPreflightRow]:
    """Open the preflight SQLite and return one `_PacbioPreflightRow` per PacBio sample.

    PROVISIONAL — the single swap point for the missing upstream reader.
    `kl-run-preflight` ships no `get_pacbio_sample_info` (verified absent on the
    pinned SHA and on `main`; cf. the Illumina `get_illumina_sample_info` that
    `pool._read_preflight_rows` uses), so this reads the `pacbio_sample` table
    directly via `_PACBIO_SAMPLE_JOIN` + the library's `get_single_run_idx` /
    `get_run_legacy_format`. When that reader lands, replace this body with a call
    to it (dropping `_PACBIO_SAMPLE_JOIN`) — the rest of the flow depends only on
    the `_PacbioPreflightRow` contract, so nothing else changes.

    Operator-actionable errors (not a SQLite, an empty sample set, a row missing
    biosample_accession / primary_project_accession, or an impossible protocol
    combo) raise via `parser.error` so the CLI surfaces one stderr line and exits
    2 before any network call — matching `_read_preflight_rows`.
    """
    from run_preflight import open_db_file  # noqa: PLC0415
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
        raw_rows = conn.execute(_PACBIO_SAMPLE_JOIN, (run_idx,)).fetchall()
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

    if not raw_rows:
        parser.error(
            f"--preflight-blob {preflight_blob} contains no pacbio_sample rows;"
            " a PacBio ingest needs at least one demultiplexed sample"
        )

    parsed: list[_PacbioPreflightRow] = []
    for (
        sample_name,
        barcode_id,
        twist_adaptor_id,
        syndna_is_twisted,
        biosample_accession,
        external_project_id,
        human_filtering,
    ) in raw_rows:
        if not barcode_id:
            parser.error(
                f"--preflight-blob {preflight_blob}: sample {sample_name!r} carries no"
                " barcode_id; a PacBio sample cannot be located on disk without it"
            )
        if not biosample_accession:
            parser.error(
                f"--preflight-blob {preflight_blob}: sample {sample_name!r} carries no"
                " biosample_accession; populate upstream before re-submitting"
            )
        if not external_project_id:
            parser.error(
                f"--preflight-blob {preflight_blob}: sample {sample_name!r} maps to no"
                " project with an external accession; verify the file is a"
                " kl-run-preflight SQLite"
            )
        # syndna_is_twisted is stored as 0/1/NULL; human_filtering as 0/1.
        twisted = None if syndna_is_twisted is None else bool(syndna_is_twisted)
        row = _PacbioPreflightRow(
            sample_name=sample_name,
            barcode=barcode_id,
            biosample_accession=biosample_accession,
            primary_project_accession=external_project_id,
            # Control/secondary-study resolution is not yet wired for PacBio
            # (the synthetic fixture has none); populate when the upstream reader
            # lands. Empty keeps parity with the Illumina row shape.
            secondary_project_accessions=[],
            human_filtering=bool(human_filtering),
            # sheet_type is a RUN-level property (a run is one protocol), read once
            # from the legacy format above and stamped on every row — there is no
            # per-sample sheet_type.
            sheet_type=sheet_type,
            twist_adaptor_id=twist_adaptor_id or None,
            syndna_is_twisted=twisted,
        )
        _validate_pacbio_protocol(row, preflight_blob, parser)
        parsed.append(row)

    # Barcode must be unique across samples: it is this flow's pool-item-id AND the
    # key both `_resolve_sample_bams` and the create-missing roster loop dedup on,
    # so two rows sharing a barcode would silently collapse into one sample (the
    # second overwriting the first, its reads dropped). A run demuxes each sample
    # to a distinct barcode, so a duplicate is a corrupt preflight — fail loud.
    by_barcode: dict[str, list[str]] = {}
    for row in parsed:
        by_barcode.setdefault(row.barcode, []).append(row.sample_name)
    collisions = {bc: names for bc, names in by_barcode.items() if len(names) > 1}
    if collisions:
        detail = "; ".join(f"{bc}: {', '.join(names)}" for bc, names in sorted(collisions.items()))
        parser.error(
            f"--preflight-blob {preflight_blob}: barcode reused across samples ({detail});"
            " each sample in a run must carry a distinct barcode"
        )
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
            f"--preflight-blob {preflight_blob}: sample {row.sample_name!r} marks its"
            " syndna twisted with no twist_adaptor_id; syndna can only be twisted"
            " when a twist adapter was attached"
        )
    if row.syndna_is_twisted is True and row.sheet_type == _SHEET_TYPE_METAG:
        parser.error(
            f"--preflight-blob {preflight_blob}: sample {row.sample_name!r} marks its"
            f" syndna twisted on a {_SHEET_TYPE_METAG!r} sheet, which quantifies no"
            " syndna; only the absquant protocol carries syndna"
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
    disambiguated while the preflight carries no SMRT-cell column. This is the
    graceful-degradation rule: unique barcodes just resolve; a collision on a
    barcode a sample actually needs fails loud rather than silently binding the
    wrong cell's reads. (When the preflight grows a SMRT-cell field, key on
    `(smartcell, barcode)` — derivable from the well subdirectory or the movie
    name's `s#` token — and this collision set becomes empty.)
    """
    index: dict[str, Path] = {}
    duplicated: set[str] = set()
    for bam in sorted(run_folder.glob("*/hifi_reads/*.bam")):
        if bam.name.endswith(_UNASSIGNED_BAM_SUFFIX):
            continue
        parts = bam.name.split(".")
        # Require the exact demux shape "<movie>.hifi_reads.<barcode>.bam" so a
        # non-demuxed combined BAM ("<movie>.hifi_reads.bam") isn't indexed under a
        # spurious barcode ("hifi_reads"). ["m84_s1", "hifi_reads", "bc2073", "bam"].
        if len(parts) < 4 or parts[-3] != "hifi_reads":
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

    Returns barcode -> absolute BAM path. Fails via `parser.error` (exit 2) on an
    empty run folder, a sample whose barcode has no BAM, or a barcode that
    collides across SMRT cells (see `_index_run_bams`) — so the operator gets one
    actionable error instead of N FAILED `bam-to-parquet` tickets.
    """
    index, duplicated = _index_run_bams(run_folder)
    if not index and not duplicated:
        parser.error(
            f"--run-folder {run_folder} contains no HiFi BAMs (expected */hifi_reads/*.bam)"
        )
    resolved: dict[str, Path] = {}
    missing: list[str] = []
    ambiguous: list[str] = []
    for row in rows:
        if row.barcode in duplicated:
            ambiguous.append(f"{row.sample_name} ({row.barcode})")
        elif row.barcode not in index:
            missing.append(f"{row.sample_name} ({row.barcode})")
        else:
            # .absolute(), not .resolve(): the orchestrator binds the BAM's parent
            # dir by its given absolute path, so dereferencing symlinks here could
            # yield a path outside that bind mount (invisible to the compute node).
            resolved[row.barcode] = index[row.barcode].absolute()
    if ambiguous:
        parser.error(
            f"--run-folder {run_folder}: barcode reuse across SMRT cells for"
            f" {len(ambiguous)} sample(s) cannot be disambiguated without SMRT-cell"
            f" information: {', '.join(ambiguous)}"
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
       blob is attached per the store-once design so a FUTURE read-mask submission
       can re-read the protocol columns (human_filtering, twist_adaptor_id,
       syndna_is_twisted) server-side and derive the case's mask chain — the same
       way submit-host-filter-pool reads human_filtering from the stored Illumina
       blob today. That PacBio server-side re-read parser is NOT built yet (it
       lands with the case-5 mask PR); until then human_filtering is echoed in this
       command's summary for operator reference only and is not forwarded onward.
    4. GET the pool roster and create only the MISSING sequenced-samples
       (`sequenced_pool_item_id = barcode`, the PacBio demux identifier — the
       analogue of bcl-convert's illumina_sample_idx). The composer 409s on a
       duplicate (pool, item_id), so create-missing (not blind-POST) is what makes
       a retry converge instead of aborting on the first already-created sample.
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

    unique_biosamples, unique_studies = _dedup_accessions(preflight_rows)

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
        owner_idx = _common.whoami(args.base_url, token)["principal_idx"]

        resolved_biosamples, missing_biosamples = _lookup_accessions(
            args.base_url,
            token,
            f"{PATH_BIOSAMPLE_PREFIX}{PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION}",
            unique_biosamples,
            BiosampleLookupByAccessionRequest,
        )
        resolved_studies, missing_studies = _lookup_accessions(
            args.base_url,
            token,
            f"{PATH_STUDY_PREFIX}{PATH_STUDY_LOOKUP_BY_ACCESSION}",
            unique_studies,
            StudyLookupByAccessionRequest,
        )
        if missing_biosamples or missing_studies:
            _print_missing_accession_error(
                preflight_rows,
                missing_biosamples,
                missing_studies,
                row_label=lambda row: f"sample {row.sample_name}",
                row_noun="sample",
            )
            sys.exit(1)

        run_resp, run_status = _common.call_with_status(
            "POST", args.base_url, token, PATH_SEQUENCING_RUN_PREFIX, json=run_body
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

        # One sequenced-sample per row; sequenced_pool_item_id = barcode (unique
        # within the pool by the same no-barcode-reuse rule the BAM index enforces).
        #
        # Create-missing, not blind-create: the composer 409s on a duplicate
        # (pool, item_id), so a plain POST loop would abort a retry on the FIRST
        # already-created sample — defeating the resilient fan-out below (the whole
        # point of which is "re-run to retry a failed ticket"). So GET the pool's
        # existing roster first and reuse those rows, POSTing only the samples not
        # yet present. This makes the whole gesture convergent (mirrors
        # submit-host-filter-pool's roster-GET pattern), which is what the docstring
        # promises. On a fresh pool the roster is empty and every sample is created.
        roster_path = PATH_SEQUENCED_SAMPLE_LIST_BY_POOL.format(
            sequencing_run_idx=sequencing_run_idx,
            sequenced_pool_idx=sequenced_pool_idx,
        )
        roster = _common.call(
            "GET", args.base_url, token, f"{PATH_SEQUENCING_RUN_PREFIX}{roster_path}"
        )
        existing_by_item_id = {s["sequenced_pool_item_id"]: s for s in roster.get("samples", [])}
        sample_path = PATH_SEQUENCED_SAMPLE_FROM_RUN.format(
            sequencing_run_idx=sequencing_run_idx,
            sequenced_pool_idx=sequenced_pool_idx,
        )
        per_sample: list[dict] = []
        for row in preflight_rows:
            existing = existing_by_item_id.get(row.barcode)
            if existing is not None:
                prep_sample_idx = existing["prep_sample_idx"]
                sequenced_sample_idx = existing.get("sequenced_sample_idx")
            else:
                secondary_study_idxs = [
                    resolved_studies[a] for a in row.secondary_project_accessions
                ]
                sample_body = SequencedSampleCreateRequest(
                    biosample_idx=resolved_biosamples[row.biosample_accession],
                    owner_idx=owner_idx,
                    prep_protocol_idx=args.prep_protocol_idx,
                    sequenced_pool_item_id=row.barcode,
                    primary_study_idx=resolved_studies[row.primary_project_accession],
                    secondary_study_idxs=secondary_study_idxs,
                ).model_dump(exclude_unset=True, mode="json")
                sample_resp = _common.call(
                    "POST",
                    args.base_url,
                    token,
                    f"{PATH_SEQUENCING_RUN_PREFIX}{sample_path}",
                    json=sample_body,
                )
                prep_sample_idx = sample_resp["prep_sample_idx"]
                sequenced_sample_idx = sample_resp["sequenced_sample_idx"]
            per_sample.append(
                {
                    "sample_name": row.sample_name,
                    "barcode": row.barcode,
                    "bam_path": str(bam_by_barcode[row.barcode]),
                    "biosample_idx": resolved_biosamples[row.biosample_accession],
                    "primary_study_idx": resolved_studies[row.primary_project_accession],
                    "human_filtering": row.human_filtering,
                    "prep_sample_idx": prep_sample_idx,
                    "sequenced_sample_idx": sequenced_sample_idx,
                    "reused": existing is not None,
                }
            )

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
                "status": "created" if run_status == 201 else "reused",
            },
            "sequenced_pool": {
                "sequenced_pool_idx": sequenced_pool_idx,
                "status": "created" if pool_status == 201 else "reused",
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
