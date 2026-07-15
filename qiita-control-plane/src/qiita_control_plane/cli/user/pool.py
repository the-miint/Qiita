"""qiita user CLI — bcl-convert and pool-masking operator gestures.

Split out of the former single-file ``cli.user`` module; behavior unchanged.
"""

import argparse
import base64
import json
import sqlite3
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

import httpx
from pydantic import BaseModel
from qiita_common.actions import READ_MASK_ACTION_ID
from qiita_common.api_paths import (
    PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION,
    PATH_BIOSAMPLE_PREFIX,
    PATH_REFERENCE_BY_IDX,
    PATH_REFERENCE_INDEX,
    PATH_REFERENCE_PREFIX,
    PATH_SEQUENCED_POOL_BLOCK_MASK_PLAN,
    PATH_SEQUENCED_POOL_BY_IDX,
    PATH_SEQUENCED_POOL_COMPLETION,
    PATH_SEQUENCED_SAMPLE_FROM_RUN,
    PATH_SEQUENCED_SAMPLE_LIST_BY_POOL,
    PATH_SEQUENCING_RUN_BY_IDX,
    PATH_SEQUENCING_RUN_PREFIX,
    PATH_SEQUENCING_RUN_SEQUENCED_POOL,
    PATH_STUDY_LOOKUP_BY_ACCESSION,
    PATH_STUDY_PREFIX,
    PATH_WORK_TICKET_PREFIX,
)
from qiita_common.host_filter_plan import (
    PoolPlanRefusal,
    SampleHostFilter,
    plan_pool_host_filter,
)
from qiita_common.illumina import read_instrument_run_info
from qiita_common.models import (
    HOST_FILTER_INDEX_TYPE_MINIMAP2,
    HOST_FILTER_INDEX_TYPE_RYPE,
    BiosampleLookupByAccessionRequest,
    BlockMaskPlanRequest,
    HostFilterResolution,
    Platform,
    ReferenceStatus,
    ScopeTargetKind,
    SequencedPoolCreateRequest,
    SequencedSampleCreateRequest,
    SequencingRunCreateRequest,
    StudyLookupByAccessionRequest,
    WorkTicketCreateRequest,
)

from ...preflight import SHEET_TYPE_PACBIO_ABSQUANT
from .. import _common

# action_id + version for the bundled bcl-convert submission flow. Pinned
# here so the CLI does not drift from the workflow YAML the operator's
# deploy syncs into qiita.action; bumping the workflow major version is a
# coordinated change.
_BCL_CONVERT_ACTION_ID = "bcl-convert"

_BCL_CONVERT_ACTION_VERSION = "1.0.0"

# action_id for the submit-host-filter-pool fan-out — imported from the shared
# action contract (qiita_common.actions) so the submitter and any reader of these
# tickets (e.g. the pool completion rollup) key off one value. The version is
# pinned locally. read-mask creates one mask over a sample's already-stored
# reads; reads are stored once by the bcl-convert workflow's ingest_reads step.
_READ_MASK_ACTION_VERSION = "1.0.0"


class _PreflightRow(NamedTuple):
    """One illumina_sample row pulled from the kl-run-preflight SQLite.

    Mirrors `run_preflight.get_illumina_sample_info`'s 4-tuple.
    `secondary_project_accessions` is empty for non-control samples; controls carry
    one entry per non-primary plate project, sorted by accession value.

    Host filtering is NOT read from the pre-flight: a sample's host is resolved from
    its own `host_taxon_id` metadata, not from the project it was booked under.
    """

    illumina_sample_idx: int
    biosample_accession: str
    primary_project_accession: str
    secondary_project_accessions: list[str]


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


def _read_preflight_rows(
    preflight_blob: Path, parser: argparse.ArgumentParser
) -> list[_PreflightRow]:
    """Open the preflight SQLite and return one `_PreflightRow` per illumina_sample row.

    Errors that the operator can fix (file not a SQLite, library raises
    on a malformed row, a row missing biosample_accession or
    primary_project_accession) raise via parser.error so the CLI
    surfaces a single stderr line and exits 2 before any network call.
    """
    from run_preflight import get_illumina_sample_info, open_db_file  # noqa: PLC0415

    try:
        conn = open_db_file(preflight_blob)
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


def _dedup_accessions(preflight_rows: list[Any]) -> tuple[list[str], list[str]]:
    """One-pass order-preserving dedup of the biosample + study accessions across
    `preflight_rows`, so the lookup routes' `missing` echo is deterministic; the
    study side pools each row's primary + secondaries so controls land their full
    set. Returns (unique_biosample_accessions, unique_study_accessions).

    Row-shape-agnostic: works on any preflight row exposing `biosample_accession`,
    `primary_project_accession`, and `secondary_project_accessions` — shared by the
    Illumina (`_PreflightRow`) and PacBio (`_PacbioPreflightRow`) submit flows."""
    unique_biosamples: list[str] = []
    unique_studies: list[str] = []
    seen_biosample: set[str] = set()
    seen_study: set[str] = set()
    for row in preflight_rows:
        if row.biosample_accession not in seen_biosample:
            seen_biosample.add(row.biosample_accession)
            unique_biosamples.append(row.biosample_accession)
        for study_accession in (row.primary_project_accession, *row.secondary_project_accessions):
            if study_accession not in seen_study:
                seen_study.add(study_accession)
                unique_studies.append(study_accession)
    return unique_biosamples, unique_studies


def _build_missing_section(
    *,
    label: str,
    missing: list[str],
    preflight_rows: list[Any],
    row_accessions: Callable[[Any], list[str]],
    row_label: Callable[[Any], str],
    row_noun: str,
) -> str | None:
    """Build one labeled section naming every preflight row that carries
    a missing accession in this class. Returns None if `missing` is empty.

    `row_accessions` extracts the row's accessions in the relevant class
    (one for biosamples, primary + secondaries for studies). `row_label`
    renders a row's per-bullet identifier (e.g. `illumina_sample_idx=5` or
    `sample sample.1`) and `row_noun` names the row kind in the header, so the
    Illumina and PacBio flows share this with their own row shapes. The header
    counts distinct missing accessions and the rows affected, so the per-row
    bullet count is no longer ambiguous against the dedup count.
    """
    if not missing:
        return None
    missing_set = set(missing)
    bullets: list[str] = []
    for row in preflight_rows:
        row_misses = [a for a in row_accessions(row) if a in missing_set]
        if row_misses:
            bullets.append(f"  - {', '.join(row_misses)} ({row_label(row)})")
    acc_plural = "s" if len(missing) != 1 else ""
    rows_plural = "s" if len(bullets) != 1 else ""
    return (
        f"{len(missing)} distinct preflight {label} accession{acc_plural}"
        f" not found in qiita, affecting {len(bullets)} {row_noun} row{rows_plural}:\n"
        + "\n".join(bullets)
    )


def _print_missing_accession_error(
    preflight_rows: list[Any],
    missing_biosamples: list[str],
    missing_studies: list[str],
    *,
    row_label: Callable[[Any], str],
    row_noun: str,
) -> None:
    """Emit one combined stderr block naming every offending preflight row.

    Each present class (biosample, study) gets its own header + bullet list, built
    by `_build_missing_section`. `row_label` / `row_noun` are threaded through so
    the Illumina and PacBio flows reuse this with their own row identifiers.
    """
    sections = [
        s
        for s in (
            _build_missing_section(
                label="biosample",
                missing=missing_biosamples,
                preflight_rows=preflight_rows,
                row_accessions=lambda row: [row.biosample_accession],
                row_label=row_label,
                row_noun=row_noun,
            ),
            _build_missing_section(
                label="study",
                missing=missing_studies,
                preflight_rows=preflight_rows,
                row_accessions=lambda row: [
                    row.primary_project_accession,
                    *row.secondary_project_accessions,
                ],
                row_label=row_label,
                row_noun=row_noun,
            ),
        )
        if s is not None
    ]
    print(
        "error: " + "\n".join(sections) + "\nimport the missing record(s) and re-run.",
        file=sys.stderr,
    )


class _ProvisionedSample(NamedTuple):
    """One sample the shared provisioner resolved/created in the pool roster.

    `row` is the caller's own preflight row (Illumina `_PreflightRow` or PacBio
    `_PacbioPreflightRow`), so each bundled gesture builds its platform-specific
    summary + work-ticket tail from it. `reused` is True when the sample already
    existed in the pool (a convergent re-run) rather than being created now."""

    row: Any
    pool_item_id: str
    biosample_idx: int
    primary_study_idx: int
    secondary_study_idxs: list[int]
    prep_sample_idx: int
    sequenced_sample_idx: int | None
    reused: bool


class _RunPoolProvision(NamedTuple):
    """Result of `_provision_run_pool_roster`: the run + pool ids/statuses and the
    resolved per-sample roster the caller fans a work-ticket tail out over."""

    sequencing_run_idx: int
    sequenced_pool_idx: int
    run_status: int
    pool_status: int
    owner_idx: int
    samples: list[_ProvisionedSample]


def _provision_run_pool_roster(
    base_url: str,
    token: str,
    *,
    preflight_rows: list[Any],
    run_body: dict[str, Any],
    pool_body: dict[str, Any],
    prep_protocol_idx: int,
    pool_item_id: Callable[[Any], str],
    row_label: Callable[[Any], str],
    row_noun: str,
) -> _RunPoolProvision:
    """Shared run → pool → sequenced-sample provisioning for the bundled submit
    gestures (`submit-bcl-convert`, `submit-pacbio-ingest`).

    The two platforms differ only in how they read the preflight, how they build
    `run_body` (platform + instrument source), and what work ticket(s) they submit
    afterwards. Everything in between — resolve the caller's principal, resolve +
    fail-fast on the biosample/study accessions, POST the run, POST the pool, and
    populate the per-sample roster — is identical, so it lives here once. The
    caller parameterizes the per-row `pool_item_id` (Illumina: illumina_sample_idx;
    PacBio: pacbio_sample_idx) and the `row_label`/`row_noun` for the
    missing-accession report, and builds its own summary + ticket tail from the
    returned roster.

    Roster creation is CREATE-MISSING, not blind-create: it GETs the pool roster
    first and reuses samples already present, POSTing only the absent ones. So a
    re-run after a partial failure converges (reuses the run + pool + existing
    samples) instead of 409ing on the first already-created sample. On a fresh pool
    the roster is empty and every sample is created.

    Raises SystemExit(1) (after printing one combined block to stderr) if any
    biosample or study accession is unresolved — before the run/pool are created,
    so a fixable preflight leaves nothing behind."""
    # Resolve the caller's principal_idx once for the per-sample owner_idx — the
    # composer requires it and the route does not auto-fill it server-side.
    owner_idx = _common.whoami(base_url, token)["principal_idx"]

    # Resolve every accession before any side effect; both lookups always run so
    # the operator sees biosample + study misses in a single round trip.
    unique_biosamples, unique_studies = _dedup_accessions(preflight_rows)
    resolved_biosamples, missing_biosamples = _lookup_accessions(
        base_url,
        token,
        f"{PATH_BIOSAMPLE_PREFIX}{PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION}",
        unique_biosamples,
        BiosampleLookupByAccessionRequest,
    )
    resolved_studies, missing_studies = _lookup_accessions(
        base_url,
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
            row_label=row_label,
            row_noun=row_noun,
        )
        raise SystemExit(1)

    run_resp, run_status = _common.call_with_status(
        "POST", base_url, token, PATH_SEQUENCING_RUN_PREFIX, json=run_body
    )
    sequencing_run_idx = run_resp["sequencing_run_idx"]
    pool_resp, pool_status = _common.call_with_status(
        "POST",
        base_url,
        token,
        f"{PATH_SEQUENCING_RUN_PREFIX}"
        f"{PATH_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=sequencing_run_idx)}",
        json=pool_body,
    )
    sequenced_pool_idx = pool_resp["sequenced_pool_idx"]

    # Create-missing: the composer 409s on a duplicate (pool, item_id), so GET the
    # existing roster and reuse those rows, POSTing only the samples not yet
    # present. This is what makes a retry converge instead of aborting on the first
    # already-created sample.
    roster_path = PATH_SEQUENCED_SAMPLE_LIST_BY_POOL.format(
        sequencing_run_idx=sequencing_run_idx, sequenced_pool_idx=sequenced_pool_idx
    )
    roster = _common.call("GET", base_url, token, f"{PATH_SEQUENCING_RUN_PREFIX}{roster_path}")
    existing_by_item_id = {s["sequenced_pool_item_id"]: s for s in roster.get("samples", [])}
    sample_path = PATH_SEQUENCED_SAMPLE_FROM_RUN.format(
        sequencing_run_idx=sequencing_run_idx, sequenced_pool_idx=sequenced_pool_idx
    )

    samples: list[_ProvisionedSample] = []
    for row in preflight_rows:
        item_id = pool_item_id(row)
        biosample_idx = resolved_biosamples[row.biosample_accession]
        primary_study_idx = resolved_studies[row.primary_project_accession]
        secondary_study_idxs = [resolved_studies[a] for a in row.secondary_project_accessions]
        existing = existing_by_item_id.get(item_id)
        if existing is not None:
            # Reuse is convergent, NOT a silent overwrite: a re-run cannot change an
            # existing sample's identity. Guard the one identity field the roster
            # exposes — biosample_idx — so a re-run pointing an item_id at a
            # different biosample fails loud instead of pretending the correction
            # landed. (The roster does not carry primary/secondary study_idx or
            # prep_protocol_idx, so those cannot be reconciled here; reuse trusts
            # the existing row for them — correcting them needs a pool-sample delete
            # + re-create, not a re-run.)
            existing_biosample_idx = existing.get("biosample_idx")
            if existing_biosample_idx is not None and existing_biosample_idx != biosample_idx:
                print(
                    f"error: pool item {item_id!r} already exists in sequenced_pool"
                    f" {sequenced_pool_idx} with biosample_idx={existing_biosample_idx}, but this"
                    f" submission resolves it to biosample_idx={biosample_idx}. A re-run cannot"
                    " change an existing sample's biosample — delete the pool sample (or fix the"
                    " preflight) and re-run.",
                    file=sys.stderr,
                )
                raise SystemExit(1)
            prep_sample_idx = existing["prep_sample_idx"]
            sequenced_sample_idx = existing.get("sequenced_sample_idx")
            reused = True
        else:
            sample_body = SequencedSampleCreateRequest(
                biosample_idx=biosample_idx,
                owner_idx=owner_idx,
                prep_protocol_idx=prep_protocol_idx,
                sequenced_pool_item_id=item_id,
                primary_study_idx=primary_study_idx,
                secondary_study_idxs=secondary_study_idxs,
            ).model_dump(exclude_unset=True, mode="json")
            try:
                sample_resp = _common.call(
                    "POST",
                    base_url,
                    token,
                    f"{PATH_SEQUENCING_RUN_PREFIX}{sample_path}",
                    json=sample_body,
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 409:
                    # The item_id is absent from the roster (which filters retired
                    # rows) yet the composer's (pool, item_id) uniqueness — which
                    # counts retired rows — rejects it. A create-missing re-run
                    # can't resolve a retired-slot collision; surface it actionably
                    # instead of letting a raw 409 abort the gesture opaquely.
                    print(
                        f"error: pool item {item_id!r} conflicts with an existing (possibly"
                        f" retired) sample in sequenced_pool {sequenced_pool_idx}; resolve it"
                        " before re-running.",
                        file=sys.stderr,
                    )
                    raise SystemExit(1) from exc
                raise
            prep_sample_idx = sample_resp["prep_sample_idx"]
            sequenced_sample_idx = sample_resp["sequenced_sample_idx"]
            reused = False
        samples.append(
            _ProvisionedSample(
                row=row,
                pool_item_id=item_id,
                biosample_idx=biosample_idx,
                primary_study_idx=primary_study_idx,
                secondary_study_idxs=secondary_study_idxs,
                prep_sample_idx=prep_sample_idx,
                sequenced_sample_idx=sequenced_sample_idx,
                reused=reused,
            )
        )
    return _RunPoolProvision(
        sequencing_run_idx=sequencing_run_idx,
        sequenced_pool_idx=sequenced_pool_idx,
        run_status=run_status,
        pool_status=pool_status,
        owner_idx=owner_idx,
        samples=samples,
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

    # Host filtering is not decided here. bcl-convert only demultiplexes the run;
    # what a sample is depleted against is resolved from its own host_taxon_id
    # metadata at submit-host-filter-pool, so intake carries no host-filter intent
    # to echo.

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
        # Shared run → pool → roster provisioning (create-missing; fails fast on an
        # unresolved accession). Illumina keys the pool-item-id on illumina_sample_idx.
        provision = _provision_run_pool_roster(
            args.base_url,
            token,
            preflight_rows=preflight_rows,
            run_body=run_body,
            pool_body=pool_body,
            prep_protocol_idx=args.prep_protocol_idx,
            pool_item_id=lambda row: str(row.illumina_sample_idx),
            row_label=lambda row: f"illumina_sample_idx={row.illumina_sample_idx}",
            row_noun="illumina_sample",
        )
        sequencing_run_idx = provision.sequencing_run_idx
        sequenced_pool_idx = provision.sequenced_pool_idx

        per_sample_results = [
            {
                "illumina_sample_idx": s.row.illumina_sample_idx,
                "biosample_accession": s.row.biosample_accession,
                "biosample_idx": s.biosample_idx,
                "primary_study_idx": s.primary_study_idx,
                "secondary_study_idxs": s.secondary_study_idxs,
                "prep_sample_idx": s.prep_sample_idx,
                "sequenced_sample_idx": s.sequenced_sample_idx,
            }
            for s in provision.samples
        ]

        # Step 4: submit the bcl-convert work_ticket against the pool. The pool
        # roster (prep_sample_idx ↔ sequenced_pool_item_id) rides in
        # action_context so the orchestrator's `ingest_reads` step can store
        # each sample's reads after demux without DB access — the CP already
        # enumerated every sample above, so it just hands them over.
        sample_map = [
            {
                "prep_sample_idx": r["prep_sample_idx"],
                "pool_item_id": str(r["illumina_sample_idx"]),
            }
            for r in per_sample_results
        ]
        ticket_body = WorkTicketCreateRequest(
            action_id=_BCL_CONVERT_ACTION_ID,
            action_version=_BCL_CONVERT_ACTION_VERSION,
            scope_target={
                "kind": ScopeTargetKind.SEQUENCED_POOL.value,
                "sequenced_pool_idx": sequenced_pool_idx,
                "sequencing_run_idx": sequencing_run_idx,
            },
            action_context={
                "bcl_input_dir": str(args.bcl_input_dir),
                "sample_map": sample_map,
            },
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


# PacBio read-mask gates, derived per sample from the roster's pre-flight facts.
# The roster surfaces the raw facts (sheet_type / twist_adaptor_id /
# syndna_is_twisted); the POLICY that turns them into gates lives here, in the
# submit, so a generic roster response carries no read-mask semantics.
_LIMA_PRESET_TWIST = "ASYMMETRIC"


def _pacbio_gates(sample: dict) -> dict | None:
    """The sample's `(lima_enabled, syndna_enabled)` gates, or None when the pool
    is not PacBio (`sheet_type` absent — an Illumina roster carries no such field).

        syndna_enabled = sheet_type == 'pacbio_absquant'   (protocols 2, 4, 5)
        lima_enabled   = twist_adaptor_id filled AND NOT syndna_is_twisted  (5)

    `syndna_is_twisted is False` rather than `not ...`: a NULL means the pre-flight
    never answered, which is not the same as "no", and must not silently enable
    lima."""
    sheet_type = sample.get("sheet_type")
    if not sheet_type:
        return None
    return {
        "syndna_enabled": sheet_type == SHEET_TYPE_PACBIO_ABSQUANT,
        "lima_enabled": bool(sample.get("twist_adaptor_id"))
        and sample.get("syndna_is_twisted") is False,
    }


def _dry_run_summary(
    samples: list[dict],
    decisions: dict[str, SampleHostFilter],
    sequenced_pool_idx: int,
) -> dict:
    """Print what the submission WOULD do, per sample, and return the summary dict.

    Grouped by the decision rather than listed per sample: a 384-sample pool that
    resolves to two groups is comprehensible; 384 lines is not. Blanks are counted
    separately from the samples that gave them their host, because "131 blanks are
    being filtered against human" is the fact an operator most wants to sanity-check
    — it is the one decision no sample made for itself.
    """
    is_control = {
        s["sequenced_pool_item_id"]: (s.get("host_filter") or {}).get("outcome") == "control"
        for s in samples
    }
    groups: dict[SampleHostFilter, list[str]] = {}
    for item_id, decision in decisions.items():
        groups.setdefault(decision, []).append(item_id)

    print(f"sequenced_pool {sequenced_pool_idx}: {len(samples)} sample(s)", file=sys.stderr)
    for decision, item_ids in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        blanks = sum(1 for i in item_ids if is_control.get(i))
        if decision.enabled:
            what = f"host_filter -> rype {decision.rype_reference_idx}"
            if decision.minimap2_reference_idx is not None:
                what += f" + minimap2 {decision.minimap2_reference_idx}"
        else:
            what = "no host filtering (QC-only pass-through)"
        blank_note = f" (incl. {blanks} blank(s), inheriting the pool's host)" if blanks else ""
        print(f"  {len(item_ids):>5}  {what}{blank_note}", file=sys.stderr)
    print("\nDRY RUN — nothing submitted.", file=sys.stderr)

    return {
        "sequenced_pool_idx": sequenced_pool_idx,
        "dry_run": True,
        "samples": len(samples),
        "per_sample": [
            {
                "sequenced_pool_item_id": item_id,
                "host_filter_enabled": d.enabled,
                "host_rype_reference_idx": d.rype_reference_idx if d.enabled else None,
                "host_minimap2_reference_idx": d.minimap2_reference_idx if d.enabled else None,
            }
            for item_id, d in decisions.items()
        ],
    }


def _abort(message: str) -> None:
    """Print an operator-actionable message to stderr and exit non-zero.

    Shared by the pool-submit preflights: the message goes to stderr so an
    operator's stdout stays clean for the JSON summary the success path prints.
    """
    print(message, file=sys.stderr)
    sys.exit(1)


def _validate_host_ref_override_args(args, parser: argparse.ArgumentParser) -> bool:
    """Shared host-ref argument coherence for both pool submitters. Returns
    `overriding` (whether a rype reference was supplied).

    minimap2 is the optional second stage and never runs without rype. And the
    references are no longer INPUTS to the decision — each sample's host is
    resolved from its own metadata — so a bare `--host-rype-reference-idx` is an
    OVERRIDE that requires `--force`. Without it the flag would either be silently
    dropped (block path) or silently ignored (fan-out path); an override that does
    nothing is the worst outcome, so it errors. Both submitters must enforce this
    identically, which is why it lives here.
    """
    if args.host_minimap2_reference_idx is not None and args.host_rype_reference_idx is None:
        parser.error("--host-minimap2-reference-idx requires --host-rype-reference-idx")
    overriding = args.host_rype_reference_idx is not None
    if overriding and not args.force:
        parser.error(
            "--host-rype-reference-idx / --host-minimap2-reference-idx are overrides:"
            " host filtering is normally resolved per sample from its host_taxon_id"
            " metadata. Pass --force to apply the given reference(s) pool-wide"
            " instead, bypassing resolution."
        )
    return overriding


def _assert_resolved_references_ready(
    base_url: str,
    token: str,
    decisions: dict[str, SampleHostFilter],
) -> None:
    """Verify every reference the plan resolved to is ACTIVE with its index built.

    Deduped across the pool: one pool resolves to one host, so this is normally a
    single rype (+ optional minimap2) pair regardless of how many samples there
    are. A pool that resolves to no filtering checks nothing.

    The references now come from `host_filter_profile`, not from an operator flag —
    so a profile pointing at a reference whose index was never built is precisely
    the failure this catches, and it is a CONFIG error, not a typo. The error text
    says so, because "check --host-rype-reference-idx" would send an operator
    looking at a flag they never passed.
    """
    rype = {d.rype_reference_idx for d in decisions.values() if d.enabled}
    minimap2 = {
        d.minimap2_reference_idx
        for d in decisions.values()
        if d.enabled and d.minimap2_reference_idx is not None
    }
    for idx in sorted(rype - {None}):
        _assert_host_reference_ready(
            base_url, token, idx, HOST_FILTER_INDEX_TYPE_RYPE, "the resolved host_filter_profile"
        )
    for idx in sorted(minimap2):
        _assert_host_reference_ready(
            base_url,
            token,
            idx,
            HOST_FILTER_INDEX_TYPE_MINIMAP2,
            "the resolved host_filter_profile",
        )


def _resolved_decisions(
    samples: list[dict],
    parser: argparse.ArgumentParser,
    sequenced_pool_idx: int,
) -> dict[str, SampleHostFilter]:
    """Turn the roster's per-sample host-filter resolutions into a submittable plan.

    The server already did the resolving — each roster item carries a `host_filter`
    block (what that sample WOULD get, from its own `host_taxon_id` metadata plus
    the run's platform). All that is left client-side is the POOL-level join: a
    blank has no host of its own, so what it gets depleted against comes from its
    neighbours. That rule lives in `qiita_common.host_filter_plan` because the block
    planner needs the identical answer — two implementations would let one pool be
    masked two ways.

    Aborts (SystemExit 1, message to stderr so stdout stays clean for the JSON
    summary) rather than guessing. Every refusal names the samples to blame: telling
    an operator "this pool has a problem" when it has 384 samples is not a message,
    it is a scavenger hunt.
    """

    missing = [s["sequenced_pool_item_id"] for s in samples if not s.get("host_filter")]
    if missing:
        _abort(
            f"sequenced_pool {sequenced_pool_idx}: {len(missing)} sample(s) carry no"
            f" host_filter resolution on the roster (e.g. {missing[:3]}). The server"
            " is older than this client, or the roster read failed. Refusing to guess"
            " what to deplete."
        )

    resolutions = {
        s["sequenced_pool_item_id"]: HostFilterResolution(**s["host_filter"]) for s in samples
    }
    plan = plan_pool_host_filter(resolutions)

    if plan.refusal is PoolPlanRefusal.UNRESOLVED_SAMPLES:
        shown = list(plan.offending[:5])
        reasons = {key: resolutions[key].reason for key in plan.offending[:3]}
        _abort(
            f"sequenced_pool {sequenced_pool_idx}: {len(plan.offending)} sample(s)"
            f" have no resolvable host (e.g. {shown}). Refusing to submit — a sample"
            " whose host we cannot determine would be masked against the wrong thing,"
            " or against nothing.\n"
            + "\n".join(f"  {k}: {v}" for k, v in reasons.items())
            + "\n\nFix the samples' host_taxon_id metadata (see `qiita-admin backfill"
            " host-taxon-id`), or pass --force with an explicit"
            " --host-rype-reference-idx to override resolution pool-wide."
        )
    if plan.refusal is PoolPlanRefusal.MULTI_HOST:
        _abort(
            f"sequenced_pool {sequenced_pool_idx} spans more than one host"
            f" (established by e.g. {list(plan.offending[:5])}). Its blanks have no"
            " single reference to be depleted against, and filtering them against the"
            " union of the pool's hosts is not supported yet. Submit a single-host"
            " subset, or pass --force with an explicit --host-rype-reference-idx."
        )

    filtered = sum(1 for d in plan.decisions.values() if d.enabled)
    print(
        f"sequenced_pool {sequenced_pool_idx}: resolved {filtered}/{len(plan.decisions)}"
        f" sample(s) to host filtering"
        + (
            f" against host terminology term {plan.pool_host_term_idx}"
            if plan.pool_host_term_idx is not None
            else " (no host in this pool — QC-only pass-through)"
        ),
        file=sys.stderr,
    )
    return plan.decisions


def _assert_pacbio_submission_coherent(
    samples: list[dict],
    *,
    decisions: dict[str, SampleHostFilter],
    sequenced_pool_idx: int,
    syndna_reference_idx: int | None,
) -> None:
    """Fail fast, before any ticket, on a PacBio submission that cannot succeed.

    Host filtering itself is now platform-agnostic — resolved per sample and
    already decided by the caller. What stays PacBio-specific is two things.

    LONG READS ARE RYPE-ONLY. The long-read host_filter chain binds no minimap2
    index, so a resolution that carries a minimap2 stage on a PacBio pool cannot be
    honoured. Silently dropping the stage would mask against less than the profile
    declares, so it aborts. If long-read minimap2 ever becomes the intent, that is
    a change to the chain, not something to paper over here.

    And the SynDNA reference must exist for every gate some sample turns on.

    Aborts with the message on stderr, then `sys.exit(1)`, so an operator's stdout
    stays clean for the JSON summary the success path prints."""

    with_minimap2 = [
        item_id
        for item_id, d in decisions.items()
        if d.enabled and d.minimap2_reference_idx is not None
    ]
    if with_minimap2:
        # The stage can come from a resolved profile OR from a forced
        # --host-minimap2-reference-idx; name both remedies rather than assuming
        # the profile, because --force could be how we got here.
        _abort(
            f"sequenced_pool {sequenced_pool_idx}: long-read host filtering is"
            " rype-only, but a minimap2 stage is set for"
            f" {len(with_minimap2)} sample(s) (e.g. {with_minimap2[:3]}). The long-read"
            " chain cannot bind it. Drop --host-minimap2-reference-idx, or NULL the"
            " pacbio_smrt profile's minimap2_reference_idx if it came from resolution."
        )
    gates = [g for g in (_pacbio_gates(s) for s in samples) if g is not None]
    if any(g["syndna_enabled"] for g in gates) and syndna_reference_idx is None:
        _abort(
            f"sequenced_pool {sequenced_pool_idx}: sheet_type is"
            f" {SHEET_TYPE_PACBIO_ABSQUANT!r}, so its samples carry SynDNA spike-ins;"
            " --syndna-reference-idx is required"
        )
    if not any(g["syndna_enabled"] for g in gates) and syndna_reference_idx is not None:
        _abort(
            "--syndna-reference-idx given but no sample in this pool carries SynDNA"
            f" spike-ins (sheet_type is not {SHEET_TYPE_PACBIO_ABSQUANT!r})"
        )


def _handle_submit_host_filter_pool(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """Create one read mask per active sequenced_sample in a pool, host-filtering
    every sample against the host reference(s) given on THIS submission.

    The pool's reads were stored once by the bcl-convert workflow's ingest_reads
    step; this gesture does NOT parse FASTQ or re-store reads. It submits one
    read-mask ticket per sample (always-on QC + host filtering), each
    recorded as a read_mask over the stored reads.

    Host filtering is a property of the filtering config, not of the sample:
    --host-rype-reference-idx (with optional --host-minimap2-reference-idx) names
    the reference(s) every sample in the pool is depleted against for this
    submission, parameterizing the read mask the run produces. Omitting both runs
    the whole pool QC-only with host filtering disabled (a pass-through). Because
    reads are stored once and masks are separate, the same pool can be
    re-submitted later against a different reference to produce a second,
    side-by-side mask — neither re-runs ingest.

    Flow:
      1. Validate host-ref argument coherence before any network call.
      2. List the pool's active samples (pool-scoped route). Each carries its
         RESOLVED host filtering — what that sample would get, derived server-side
         from its own `host_taxon_id` metadata plus the run's platform. Join the
         blanks against the pool's host, then abort before any ticket POST if any
         sample is unresolvable or the pool spans more than one host.
      3. Pre-flight the reference(s) the plan RESOLVED to: a rype reference must be
         ACTIVE + carry a rype index; a minimap2 reference must be ACTIVE + carry a
         minimap2 index. A bad reference here would otherwise fail every ticket at
         the runner's submission stage (_resolve_host_filter_indexes) — N FAILED
         tickets instead of one actionable error. (A pool that resolves to no
         filtering skips this.)
      4. Read the run's instrument_model once (GET /sequencing-run) to forward
         per sample so QC's polyG step is gated correctly (nullable).
      5. POST one read-mask ticket per sample (always-on QC; host filtering
         enabled with the given reference(s), or a pass-through when none is
         given), scoped to the sample's prep_sample_idx. The runner binds each
         sample's stored reads (failing the ticket if it was never ingested).

    Per-sample resilience: each ticket POST is isolated, so one sample's error
    (a transient 5xx, a 409 in-flight, a network blip) is recorded and the
    fan-out CONTINUES to the rest — it never strands the remaining samples the
    way an un-caught raise would. The summary lists every submitted and every
    failed sample, and the command exits non-zero if any sample failed, so a
    partial fan-out is visible and re-runnable rather than silent. A sample
    whose reads were never stored fails its own ticket at the runner's
    staged-read resolution (and shows up in the per-sample summary, not here).

    `--only-missing` skips samples that already have a read-mask ticket (any
    state, via the roster's has_read_mask_ticket flag), submitting only those
    with none — the clean way to fill a pool whose prior fan-out was
    interrupted without duplicating already-submitted work. Off by default so a
    deliberate re-submit against a different host reference still fans out
    pool-wide.
    """
    overriding = _validate_host_ref_override_args(args, parser)

    def _run(token: str) -> dict:
        # Step 1: enumerate the pool's active samples (single round trip). The
        # roster carries each sample's RESOLVED host filtering, derived server-side
        # from its own host_taxon_id metadata — no local file, no operator flag.
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
            print(
                f"sequenced_pool {args.sequenced_pool_idx} has no active"
                " sequenced_samples to process",
                file=sys.stderr,
            )
            sys.exit(1)

        # Step 1.25: --only-missing drops samples that already carry a read-mask
        # ticket (any state), so a re-run fills only the gap left by a prior
        # interrupted fan-out instead of duplicating submitted work. Applied
        # before the coherence checks and the loop so everything downstream sees
        # only the to-submit set. Falls back to submitting a sample if the roster
        # didn't carry the flag (older server) — never silently drops on absence.
        skipped_existing = 0
        if args.only_missing:
            before = len(samples)
            samples = [s for s in samples if not s.get("has_read_mask_ticket")]
            skipped_existing = before - len(samples)
            if not samples:
                summary = {
                    "sequencing_run_idx": args.sequencing_run_idx,
                    "sequenced_pool_idx": args.sequenced_pool_idx,
                    "samples_submitted": 0,
                    "samples_skipped_existing": skipped_existing,
                    "samples_failed": 0,
                    "failed": [],
                    "per_sample": [],
                }
                sys.stderr.write(
                    f"--only-missing: all {skipped_existing} active sample(s) in"
                    f" sequenced_pool {args.sequenced_pool_idx} already have a"
                    " read-mask ticket; nothing to submit\n"
                )
                return summary

        # Step 1.5: decide each sample's host filtering.
        #
        # ONE path for both platforms. Illumina used to require pool-uniform host
        # filtering (a single operator-chosen reference applied to everything) while
        # PacBio was already per-sample; that split existed only because the two had
        # different SOURCES for the decision. They now share one — each sample's own
        # `host_taxon_id` metadata, resolved server-side and reported on the roster —
        # so the branch collapses.
        is_pacbio_pool = any(s.get("sheet_type") for s in samples)
        if not is_pacbio_pool and args.syndna_reference_idx is not None:
            parser.error("--syndna-reference-idx applies only to a PacBio pool")

        if args.force:
            # The escape hatch: ignore what the samples say and apply the operator's
            # flags verbatim, pool-wide, blanks included. This is the old behaviour,
            # kept for the case where the metadata is wrong or absent and the
            # operator knows better. With no reference given it disables filtering
            # pool-wide — an explicit, deliberate pass-through.
            decisions = {
                s["sequenced_pool_item_id"]: SampleHostFilter(
                    enabled=overriding,
                    rype_reference_idx=args.host_rype_reference_idx,
                    minimap2_reference_idx=args.host_minimap2_reference_idx,
                )
                for s in samples
            }
            applied = (
                f"host reference {args.host_rype_reference_idx}"
                if overriding
                else "NO host filtering"
            )
            print(
                f"--force: bypassing per-sample resolution; applying {applied}"
                f" to all {len(samples)} sample(s), blanks included",
                file=sys.stderr,
            )
        else:
            decisions = _resolved_decisions(samples, parser, args.sequenced_pool_idx)

        if is_pacbio_pool:
            _assert_pacbio_submission_coherent(
                samples,
                decisions=decisions,
                sequenced_pool_idx=args.sequenced_pool_idx,
                syndna_reference_idx=args.syndna_reference_idx,
            )

        # Step 1.6: the run's metadata, read once. Its instrument_model gates QC's
        # polyG (forwarded per sample below); its platform drives the PacBio
        # no-facts refusal just below. Read BEFORE the dry-run return so a dry run
        # refuses exactly what a real submit would.
        run = _common.call(
            "GET",
            args.base_url,
            token,
            f"{PATH_SEQUENCING_RUN_PREFIX}"
            f"{PATH_SEQUENCING_RUN_BY_IDX.format(sequencing_run_idx=args.sequencing_run_idx)}",
        )
        instrument_model = run.get("instrument_model")

        # Step 1.7: a PacBio run whose roster carries NO protocol facts means the
        # server could not parse the pool's stored pre-flight — NOT that the pool is
        # Illumina. Refuse, before any ticket AND before the dry-run preview.
        #
        # The direction of this check is the whole point. `is_pacbio_pool` above is
        # derived from `any(sheet_type)`, i.e. it infers "not PacBio" from ABSENCE —
        # so an unparseable blob silently takes the Illumina branch and writes
        # `lima_enabled: false, syndna_enabled: false` onto every ticket. That is a
        # case-5 pool masked with no lima and no syndna, whose spike-in count is then
        # structurally zero: exactly what the chain's step order exists to prevent.
        #
        # This keys on the run's `platform` column, which is authoritative and depends
        # on neither the blob nor the host-filter resolution. --force does NOT bypass
        # it: forcing host filtering off is a choice, masking a PacBio pool with the
        # long-read chain silently off is not.
        if run.get("platform") == Platform.PACBIO_SMRT.value:
            no_facts = [s["sequenced_pool_item_id"] for s in samples if not s.get("sheet_type")]
            if no_facts:
                _abort(
                    f"sequencing_run {args.sequencing_run_idx} is PacBio, but"
                    f" {len(no_facts)} sample(s) carry no protocol facts on the roster"
                    f" (e.g. {no_facts[:3]}). The pool's stored pre-flight could not be"
                    " parsed server-side, so lima and syndna cannot be gated. Refusing:"
                    " masking a PacBio pool with the long-read chain silently off would"
                    " leave its spike-in count structurally zero. Check the pool's"
                    " run_preflight_blob. (--force does not bypass this.)"
                )

        # Step 1.75: --dry-run stops here, after everything that could REFUSE has
        # run and before anything that could WRITE.
        #
        # This is the only way to see a pool's plan before fanning out hundreds of
        # tickets against it. It matters more than a preview usually would: host
        # filtering is now derived from each sample's metadata rather than chosen on
        # the command line, and this is where an operator sees that derivation before
        # it acts.
        if args.dry_run:
            return _dry_run_summary(samples, decisions, args.sequenced_pool_idx)

        # Step 2: pre-flight the host reference(s) the plan actually resolved to,
        # before any ticket — one actionable error instead of N FAILED tickets.
        #
        # Checks the RESOLVED references, not the flags: the profile is what decides
        # now, so a profile pointing at a reference whose index was never built is
        # exactly the failure this is here to catch. A pool that resolves to no
        # filtering at all checks nothing. Deduped — one pool resolves to one host,
        # so this is normally a single pair.
        _assert_resolved_references_ready(args.base_url, token, decisions)
        # The syndna reference is just another minimap2 reference — same index type
        # as the host filter's minimap2 arm, same readiness check. One actionable
        # error instead of N FAILED tickets.
        if args.syndna_reference_idx is not None:
            _assert_host_reference_ready(
                args.base_url,
                token,
                args.syndna_reference_idx,
                HOST_FILTER_INDEX_TYPE_MINIMAP2,
                "--syndna-reference-idx",
            )

        # Step 5: one read-mask ticket per sample — always-on QC plus that sample's
        # OWN host-filter decision, taken from the plan resolved above. Host
        # filtering is no longer a property of the submission (one reference applied
        # to everything); it is a property of the sample. The runner reads these
        # action_context refs to mint the read mask and drive host_filter, and binds
        # the sample's already-stored reads. instrument_model is forwarded only when
        # the run records it (QC defaults polyG OFF when it's absent).
        per_sample_results: list[dict] = []
        failures: list[dict] = []
        for sample in samples:
            gates = _pacbio_gates(sample)
            host = decisions[sample["sequenced_pool_item_id"]]
            # `when:` is DEFAULT-ON — an absent gate key RUNS the step. All three are
            # written explicitly on EVERY ticket, or a short-read read-mask ticket
            # would execute the long-read lima chain, and a pass-through sample would
            # be host-filtered against nothing.
            action_context: dict[str, Any] = {
                "lima_enabled": False,
                "syndna_enabled": False,
                "host_filter_enabled": host.enabled,
            }
            if host.enabled:
                action_context["host_rype_reference_idx"] = host.rype_reference_idx
                if host.minimap2_reference_idx is not None:
                    action_context["host_minimap2_reference_idx"] = host.minimap2_reference_idx
            if gates is not None:
                # PacBio: the lima / syndna gates are prep facts, still per sample
                # from the stored pre-flight. (Long reads bind no minimap2 index —
                # _assert_pacbio_submission_coherent refuses a plan that carries one.)
                action_context.update(gates)
                if gates["lima_enabled"]:
                    action_context["lima_preset"] = _LIMA_PRESET_TWIST
                if gates["syndna_enabled"]:
                    action_context["syndna_reference_idx"] = args.syndna_reference_idx
            if instrument_model is not None:
                action_context["instrument_model"] = instrument_model
            ticket_body = WorkTicketCreateRequest(
                action_id=READ_MASK_ACTION_ID,
                action_version=_READ_MASK_ACTION_VERSION,
                scope_target={
                    "kind": ScopeTargetKind.PREP_SAMPLE.value,
                    "prep_sample_idx": sample["prep_sample_idx"],
                },
                action_context=action_context,
            ).model_dump(exclude_unset=True, mode="json")
            # Isolate each POST: one sample's failure (transient 5xx, 409
            # in-flight, or a transport blip) is recorded and the loop CONTINUES
            # to the rest. A bare raise here would abandon every later sample —
            # the fan-out fragility this command is being fixed for.
            try:
                ticket_resp, _ticket_status = _common.call_with_status(
                    "POST",
                    args.base_url,
                    token,
                    PATH_WORK_TICKET_PREFIX,
                    json=ticket_body,
                )
            except httpx.HTTPStatusError as exc:
                failures.append(
                    {
                        "prep_sample_idx": sample["prep_sample_idx"],
                        "sequenced_pool_item_id": sample["sequenced_pool_item_id"],
                        "status_code": exc.response.status_code,
                        "error": exc.response.text[:500],
                    }
                )
                continue
            except httpx.HTTPError as exc:
                # Transport-level failure (connect/read/timeout): no response.
                failures.append(
                    {
                        "prep_sample_idx": sample["prep_sample_idx"],
                        "sequenced_pool_item_id": sample["sequenced_pool_item_id"],
                        "status_code": None,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            per_sample_results.append(
                {
                    "prep_sample_idx": sample["prep_sample_idx"],
                    "sequenced_pool_item_id": sample["sequenced_pool_item_id"],
                    # What THIS sample got, not what the submission chose — they are
                    # no longer the same thing, and a pool can now legitimately mix
                    # filtered and pass-through samples.
                    "host_filter_enabled": host.enabled,
                    "host_rype_reference_idx": host.rype_reference_idx if host.enabled else None,
                    "host_minimap2_reference_idx": (
                        host.minimap2_reference_idx if host.enabled else None
                    ),
                    "work_ticket_idx": ticket_resp.get("work_ticket_idx"),
                }
            )

        summary = {
            "instrument_model": instrument_model,
            "sequencing_run_idx": args.sequencing_run_idx,
            "sequenced_pool_idx": args.sequenced_pool_idx,
            "samples_submitted": len(per_sample_results),
            "samples_skipped_existing": skipped_existing,
            "samples_failed": len(failures),
            "samples_host_filtered": sum(1 for r in per_sample_results if r["host_filter_enabled"]),
            "samples_passthrough": sum(
                1 for r in per_sample_results if not r["host_filter_enabled"]
            ),
            "failed": failures,
            "per_sample": per_sample_results,
        }
        if failures:
            # Each failure was non-fatal on its own, but exit non-zero so a
            # partial fan-out isn't mistaken for full success by an operator or a
            # script. Print the summary here since the raise skips the default
            # success printer.
            print(json.dumps(summary, indent=2))
            raise SystemExit(1)
        return summary

    return _common.run_http_subcommand(_run)


def _handle_submit_block_mask_pool(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """Bulk-block variant of submit-host-filter-pool: mask a whole pool as fixed
    ~10M-read BLOCKS instead of one ticket per sample.

    Same filtering semantics and the same client-side preflight as
    submit-host-filter-pool — per-sample resolution, the pool-level blank join, and
    the up-front host-reference ACTIVE+index readiness check — but the actual
    submission is a SINGLE server call to the
    block-mask-plan endpoint, not a per-sample fan-out. The server resolves each
    sample's mask identity, partitions by mask, tiles each partition into blocks,
    and dispatches one block work-ticket per block; per-sample completion is
    reconciled afterward. This collapses the fan-out surface (few fat blocks
    instead of one ticket per sample) and gives each job a predictable ~10M-read
    envelope.

    `--only-missing` is applied SERVER-side (skip samples already carrying a
    completion gate for their resolved mask), so an interrupted plan re-runs only
    the gap. instrument_model is read server-side (the endpoint owns it), so
    there is no per-run GET here."""
    # Host-ref argument coherence, validated before any network call (mirrors
    # submit-host-filter-pool): minimap2 is the optional second stage.
    _validate_host_ref_override_args(args, parser)

    def _run(token: str) -> dict:
        # Step 1: enumerate the pool's active samples for the intent preflight.
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
            print(
                f"sequenced_pool {args.sequenced_pool_idx} has no active"
                " sequenced_samples to process",
                file=sys.stderr,
            )
            sys.exit(1)

        # Step 1.5: resolve the pool, then require the answer to be UNIFORM.
        #
        # The block path masks a whole pool under ONE recipe — the planner takes a
        # single host reference and applies it to every block. That is still correct
        # whenever the resolved plan happens to be uniform, which is the normal case:
        # a single-host pool's blanks inherit that host, so "apply H to everything"
        # IS the per-sample answer. It stops being correct the moment a pool mixes
        # samples that filter with samples that pass through, because pool-wide would
        # then deplete a host-less sample.
        #
        # So: resolve, and refuse a non-uniform pool rather than flatten it. Driving
        # the planner off the per-sample plan properly is server-side work
        # (block_planner takes pool-wide references today) and is tracked separately;
        # until then this refuses instead of silently doing the wrong thing.
        if args.force:
            block_decision = SampleHostFilter(
                enabled=args.host_rype_reference_idx is not None,
                rype_reference_idx=args.host_rype_reference_idx,
                minimap2_reference_idx=args.host_minimap2_reference_idx,
            )
        else:
            decisions = _resolved_decisions(samples, parser, args.sequenced_pool_idx)
            distinct = set(decisions.values())
            if len(distinct) > 1:
                print(
                    f"sequenced_pool {args.sequenced_pool_idx} resolves to"
                    f" {len(distinct)} different host-filter decisions across its"
                    " samples, but block masking applies ONE recipe to the whole"
                    " pool. Submit per-sample with `submit-host-filter-pool`"
                    " instead, or pass --force with an explicit"
                    " --host-rype-reference-idx to apply one reference pool-wide.",
                    file=sys.stderr,
                )
                sys.exit(1)
            block_decision = next(iter(distinct))

        # Step 2: pre-flight the reference(s) the pool RESOLVED to — one actionable
        # error instead of a whole plan's worth of failed blocks. A pool that
        # resolves to no filtering checks nothing.
        _assert_resolved_references_ready(args.base_url, token, {"pool": block_decision})

        # Step 3: one call plans + submits the whole pool. The server tiles,
        # persists the cover-map + gate, creates one block ticket per block, and
        # dispatches; it returns the plan summary (blocks + tickets + counts).
        plan_path = PATH_SEQUENCED_POOL_BLOCK_MASK_PLAN.format(
            sequencing_run_idx=args.sequencing_run_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
        )
        body = BlockMaskPlanRequest(
            host_rype_reference_idx=block_decision.rype_reference_idx
            if block_decision.enabled
            else None,
            host_minimap2_reference_idx=block_decision.minimap2_reference_idx
            if block_decision.enabled
            else None,
            only_missing=args.only_missing,
        ).model_dump(mode="json")
        return _common.call(
            "POST",
            args.base_url,
            token,
            f"{PATH_SEQUENCING_RUN_PREFIX}{plan_path}",
            json=body,
        )

    return _common.run_http_subcommand(_run)


def _handle_pool_completion(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """GET the pool's end-to-end processing rollup and print its JSON body:
    the demux (bcl-convert) `demux_state`, the per-sample read-mask buckets with
    the host-masking `complete` flag, and `fully_processed` (demux + masking).

    Dedicated (rather than the generic `read` command) because the route is keyed
    on two ids — sequencing_run_idx and sequenced_pool_idx — which `read` (single
    idx) can't fill.
    """
    path = f"{PATH_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_POOL_COMPLETION}".format(
        sequencing_run_idx=args.sequencing_run_idx,
        sequenced_pool_idx=args.sequenced_pool_idx,
    )
    return _common.run_http_subcommand(lambda t: _common.call("GET", args.base_url, t, path))
