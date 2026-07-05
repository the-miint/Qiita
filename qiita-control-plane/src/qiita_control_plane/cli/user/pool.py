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
from qiita_common.illumina import read_instrument_run_info
from qiita_common.models import (
    HOST_FILTER_INDEX_TYPE_MINIMAP2,
    HOST_FILTER_INDEX_TYPE_RYPE,
    BiosampleLookupByAccessionRequest,
    BlockMaskPlanRequest,
    Platform,
    ReferenceStatus,
    ScopeTargetKind,
    SequencedPoolCreateRequest,
    SequencedSampleCreateRequest,
    SequencingRunCreateRequest,
    StudyLookupByAccessionRequest,
    WorkTicketCreateRequest,
)

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

    # Host filtering is not decided here: it is a filtering-config choice made at
    # submit-host-filter-pool, where the host reference parameterizes the read
    # mask. bcl-convert only demultiplexes the run; the preflight's per-project
    # human_filtering flag is still echoed per sample below so the operator knows
    # which samples the run intended to deplete when choosing those later args.

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
                    # The preflight's per-project human_filtering flag is echoed
                    # for operator reference — it no longer pins a host reference
                    # on the sample; host filtering is chosen at
                    # submit-host-filter-pool time.
                    "human_filtering": row.human_filtering,
                    "prep_sample_idx": sample_resp["prep_sample_idx"],
                    "sequenced_sample_idx": sample_resp["sequenced_sample_idx"],
                }
            )

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


def _assert_pool_intent_matches(
    samples: list[dict],
    *,
    sequenced_pool_idx: int,
    host_rype_reference_idx: int | None,
    applying_host_filter: bool,
    force: bool,
) -> None:
    """Preflight shared by the per-sample (submit-host-filter-pool) and block
    (submit-block-mask-pool) pool-masking submitters.

    Host filtering is applied pool-wide, so flag any sample whose intake
    human_filtering intent disagrees with this submission's choice (a host
    reference depletes human reads; none is a pass-through). A flagged-human
    sample submitted with no host reference would keep its human reads; a
    not-flagged sample submitted with a host reference would be filtered against
    the operator's intent. Either is a likely mistake — abort (SystemExit 1)
    before any submission unless `force` downgrades the mismatch to a warning. A
    null intent (the bcl-convert/preflight coupling is broken for that pool item)
    always aborts.
    """
    mismatched: list[str] = []
    for sample in samples:
        item_id = sample["sequenced_pool_item_id"]
        intent = sample.get("human_filtering")
        if intent is None:
            print(
                f"sequenced_pool {sequenced_pool_idx} has no stored"
                f" preflight intent for sequenced_pool_item_id {item_id!r}"
                f" (prep_sample {sample['prep_sample_idx']}); verify the pool"
                " was created by submit-bcl-convert with its run preflight",
                file=sys.stderr,
            )
            sys.exit(1)
        if intent != applying_host_filter:
            mismatched.append(
                f"  - sequenced_pool_item_id {item_id} (prep_sample"
                f" {sample['prep_sample_idx']}): intake human_filtering="
                f"{intent}"
            )
    if mismatched:
        choice = (
            f"apply host reference {host_rype_reference_idx}"
            if applying_host_filter
            else "run QC-only with host filtering disabled (a pass-through)"
        )
        header = (
            "host filtering is applied pool-wide, but these samples' intake"
            f" human_filtering intent disagrees with this submission, which"
            f" would {choice} for every sample:\n"
            + "\n".join(mismatched)
            + "\nSubmit the matching subset, re-run with the opposite"
            " host-reference choice, or pass --force to apply this pool-wide"
            " choice anyway."
        )
        if not force:
            print(header, file=sys.stderr)
            sys.exit(1)
        print("WARNING (--force): " + header, file=sys.stderr)


def _handle_submit_host_filter_pool(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """Create one read mask per active sequenced_sample in a pool, host-filtering
    every sample against the host reference(s) given on THIS submission.

    The pool's reads were stored once by the bcl-convert workflow's ingest_reads
    step; this gesture does NOT parse FASTQ or re-store reads. It submits one
    read-mask/1.0.0 ticket per sample (always-on QC + host filtering), each
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
         intake human_filtering intent, derived server-side from the pool's
         STORED run-preflight blob (no operator-supplied file — the database is
         the source of truth, so a later `preflight update-lane` is reflected
         automatically). Flag any sample whose intent disagrees with this
         submission's pool-wide host-ref choice; abort before any ticket POST
         unless --force downgrades the mismatch to a warning.
      3. Pre-flight the given host reference(s): a rype reference must be ACTIVE +
         carry a rype index; a minimap2 reference must be ACTIVE + carry a
         minimap2 index. A bad reference here would otherwise fail every ticket at
         the runner's submission stage (_resolve_host_filter_indexes) — N FAILED
         tickets instead of one actionable error. (No host refs given skips this.)
      4. Read the run's instrument_model once (GET /sequencing-run) to forward
         per sample so QC's polyG step is gated correctly (nullable).
      5. POST one read-mask/1.0.0 ticket per sample (always-on QC; host filtering
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
    # Host-ref argument coherence, validated before any network call: minimap2 is
    # the optional second stage and never runs without rype.
    if args.host_minimap2_reference_idx is not None and args.host_rype_reference_idx is None:
        parser.error("--host-minimap2-reference-idx requires --host-rype-reference-idx")

    applying_host_filter = args.host_rype_reference_idx is not None

    def _run(token: str) -> dict:
        # Step 1: enumerate the pool's active samples (single round trip). The
        # roster carries each sample's intake human_filtering intent, derived
        # server-side from the pool's stored run-preflight blob — no local file.
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

        # Step 1.5: host filtering is applied pool-wide, so flag any sample whose
        # intake human_filtering intent disagrees with this submission's choice.
        # Roster-driven: every pool member is compared against its intake intent
        # (resolved server-side from the pool's stored preflight). Shared with
        # submit-block-mask-pool via _assert_pool_intent_matches.
        _assert_pool_intent_matches(
            samples,
            sequenced_pool_idx=args.sequenced_pool_idx,
            host_rype_reference_idx=args.host_rype_reference_idx,
            applying_host_filter=applying_host_filter,
            force=args.force,
        )

        # Step 2: pre-flight the given host reference(s) before any ticket — one
        # actionable error instead of N FAILED tickets. No host refs given (the
        # whole-pool pass-through) checks nothing.
        if args.host_rype_reference_idx is not None:
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

        # Step 3: the run's instrument_model gates QC's polyG; read it once and
        # forward per sample. Nullable (a non-bcl run may not record it).
        run = _common.call(
            "GET",
            args.base_url,
            token,
            f"{PATH_SEQUENCING_RUN_PREFIX}"
            f"{PATH_SEQUENCING_RUN_BY_IDX.format(sequencing_run_idx=args.sequencing_run_idx)}",
        )
        instrument_model = run.get("instrument_model")

        # Step 5: one read-mask/1.0.0 ticket per sample — always-on QC + the host
        # filtering chosen on this submission, uniform across the pool. A given
        # rype reference filters every sample against it (plus the optional
        # minimap2 reference); with none given each ticket sets
        # host_filter_enabled=False explicitly (a QC-only pass-through), so the
        # ticket records the deliberate no-filter decision. The runner reads these
        # action_context refs to mint the read mask and drive host_filter, and
        # binds the sample's already-stored reads. instrument_model is forwarded
        # only when the run records it (QC defaults polyG OFF when it's absent).
        host_rype = args.host_rype_reference_idx
        host_minimap2 = args.host_minimap2_reference_idx
        host_filter_enabled = host_rype is not None
        per_sample_results: list[dict] = []
        failures: list[dict] = []
        for sample in samples:
            action_context: dict[str, Any] = {
                "host_filter_enabled": host_filter_enabled,
            }
            if host_filter_enabled:
                action_context["host_rype_reference_idx"] = host_rype
                if host_minimap2 is not None:
                    action_context["host_minimap2_reference_idx"] = host_minimap2
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
                    "host_filter_enabled": host_filter_enabled,
                    "host_rype_reference_idx": host_rype if host_filter_enabled else None,
                    "host_minimap2_reference_idx": host_minimap2 if host_filter_enabled else None,
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
    submit-host-filter-pool — host-ref coherence, the intake human_filtering
    intent check (with --force), and the up-front host-reference ACTIVE+index
    readiness check — but the actual submission is a SINGLE server call to the
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
    if args.host_minimap2_reference_idx is not None and args.host_rype_reference_idx is None:
        parser.error("--host-minimap2-reference-idx requires --host-rype-reference-idx")

    applying_host_filter = args.host_rype_reference_idx is not None

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

        # Step 1.5: intent-mismatch preflight over the whole active roster (shared
        # with submit-host-filter-pool). --only-missing is applied server-side, so
        # the check runs over every active sample; an already-gated sample the
        # server will skip is compared too (conservative, never under-checks).
        _assert_pool_intent_matches(
            samples,
            sequenced_pool_idx=args.sequenced_pool_idx,
            host_rype_reference_idx=args.host_rype_reference_idx,
            applying_host_filter=applying_host_filter,
            force=args.force,
        )

        # Step 2: pre-flight the given host reference(s) — one actionable error
        # instead of a whole plan's worth of failed blocks. No host refs → skip.
        if args.host_rype_reference_idx is not None:
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

        # Step 3: one call plans + submits the whole pool. The server tiles,
        # persists the cover-map + gate, creates one block ticket per block, and
        # dispatches; it returns the plan summary (blocks + tickets + counts).
        plan_path = PATH_SEQUENCED_POOL_BLOCK_MASK_PLAN.format(
            sequencing_run_idx=args.sequencing_run_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
        )
        body = BlockMaskPlanRequest(
            host_rype_reference_idx=args.host_rype_reference_idx,
            host_minimap2_reference_idx=args.host_minimap2_reference_idx,
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
