"""Runner reference-index and QC-adapter resolution."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import asyncpg
from qiita_common.models import (
    HOST_FILTER_INDEX_TYPE_MINIMAP2,
    HOST_FILTER_INDEX_TYPE_RYPE,
    ReferenceStatus,
)

import qiita_control_plane.runner as _runner_pkg

from ..actions.reference import (
    ReferenceNotFound,
)
from ..auth.tickets import sign_ticket
from ._upload import _submission_bad_input

# =============================================================================
# Reference-index resolution
# =============================================================================


class ReferenceIndexNotBuilt(ValueError):
    """The reference is ACTIVE but carries no index of the requested type.

    A `ValueError` subclass so existing callers / tests that catch `ValueError`
    still match, while `_resolve_host_filter_indexes` can catch THIS narrowly to
    treat a missing index type as "skip that host-filter stage" — distinct from a
    non-active reference (a plain `ValueError`), which stays a hard error."""


async def _resolve_reference_index_path(
    pool: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    index_type: str,
) -> str:
    """Resolve the on-disk path of the newest `index_type` index for an
    ACTIVE reference — a host-filter index path the `host_filter` step is
    injected with (the rype `.ryxdi` or minimap2 `.mmi` for a host reference;
    see `_resolve_host_filter_indexes`).

    `qiita.reference_index` has no UNIQUE(reference_idx, index_type) by design
    (growing a reference appends a newer generation), so "newest wins":
    ordered by created_at then reference_index_idx, both descending, so a
    same-timestamp tie still resolves deterministically to the latest row.

    Raises:
      * ReferenceNotFound — the reference row doesn't exist.
      * ValueError — the reference exists but isn't `active` (an index built
        against a still-`indexing`/failed reference must not be served; the
        build may be mid-flight).
      * ReferenceIndexNotBuilt (a ValueError subclass) — the reference is active
        but no `index_type` index exists yet. Narrower than the not-active case
        so `_resolve_host_filter_indexes` can treat a single missing index type
        as "skip that stage" while still hard-failing a non-active reference."""
    status = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    if status is None:
        raise ReferenceNotFound(reference_idx)
    if status != ReferenceStatus.ACTIVE.value:
        raise ValueError(
            f"reference {reference_idx} status is {status!r}, must be "
            f"{ReferenceStatus.ACTIVE.value!r} to resolve its {index_type!r} index"
        )
    fs_path = await pool.fetchval(
        "SELECT fs_path FROM qiita.reference_index"
        " WHERE reference_idx = $1 AND index_type = $2"
        " ORDER BY created_at DESC, reference_index_idx DESC"
        " LIMIT 1",
        reference_idx,
        index_type,
    )
    if fs_path is None:
        raise ReferenceIndexNotBuilt(
            f"reference {reference_idx} has no {index_type!r} index built yet"
        )
    return fs_path


def _coerce_reference_idx(value: Any, field: str) -> int:
    """Validate a host-filter reference idx pulled from `action_context`. `type(...)
    is int` (not isinstance) rejects a JSON bool — an int subclass — rather than
    silently treating it as 0/1. Raises a SUBMISSION BAD_INPUT on a missing /
    non-positive / wrong-typed value."""
    if type(value) is not int or value <= 0:
        raise _submission_bad_input(f"{field} must be a positive integer, got {value!r}")
    return value


async def _resolve_required_host_index(
    pool: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    index_type: str,
    field: str,
) -> Path:
    """Resolve `index_type` from `reference_idx`, mapping EVERY failure mode
    (unknown reference, non-active, index not built) to a SUBMISSION BAD_INPUT —
    the two-reference layout designates a reference explicitly for this index
    type, so a missing index is a hard error (not a skipped stage as in the legacy
    single-reference layout)."""
    try:
        return Path(await _resolve_reference_index_path(pool, reference_idx, index_type))
    except ReferenceNotFound as exc:
        raise _submission_bad_input(
            f"{field}={reference_idx} references an unknown reference"
        ) from exc
    except ValueError as exc:
        # Non-active reference OR ReferenceIndexNotBuilt (a ValueError subclass) —
        # both hard errors here (the reference was designated for this index).
        raise _submission_bad_input(str(exc)) from exc


async def _resolve_host_filter_indexes(
    pool: asyncpg.Pool | asyncpg.Connection,
    *,
    action_context: dict[str, Any],
) -> dict[str, Path]:
    """Resolve the host-filter index paths when host filtering is enabled, else {}.

    Gated by `host_filter_enabled` (bool) in `action_context`. Two layouts are
    accepted, never mixed:

    * **Two-reference** (fastq-to-parquet/1.2.0): an independent reference per
      tool — `host_rype_reference_idx` (REQUIRED) supplies the rype `.ryxdi`, and
      the optional `host_minimap2_reference_idx` supplies the minimap2 `.mmi`.
      Each is bound from its OWN reference, which MUST be ACTIVE and MUST carry the
      named index type (a designated reference missing its index is a hard error).
      minimap2 omitted → only `host_rype_path` is bound.
    * **Legacy single-reference** (fastq-to-parquet/1.1.0): `host_reference_idx`
      names ONE active reference; whichever of its rype/minimap2 indexes exist are
      bound (>=1 required; a missing one just skips that stage). Kept for
      back-compat.

    Both bind `host_rype_path` / `host_minimap2_path` — the `host_filter` step's
    optional inputs; the step skips the stage whose path is None, so
    `host_filter.py` is unchanged across both layouts. When disabled (flag
    false/absent) nothing is resolved and the step runs as a pass-through.

    Mirrors `_resolve_upload_handles`: every failure (a required idx absent /
    non-positive, a reference unknown / non-active / missing its designated index,
    NEITHER index in the legacy case, or mixing the two layouts) raises a typed
    `BackendFailure(BAD_INPUT)` at stage=SUBMISSION that `run_workflow` turns into
    a FAILED work_ticket. None of these keys end in `_upload_idx`, so
    `_resolve_upload_handles` leaves them untouched."""
    if not action_context.get("host_filter_enabled"):
        return {}

    legacy_idx = action_context.get("host_reference_idx")
    rype_idx = action_context.get("host_rype_reference_idx")
    minimap2_idx = action_context.get("host_minimap2_reference_idx")

    # The two layouts are mutually exclusive — mixing them is a contract error,
    # not a silent precedence pick.
    if legacy_idx is not None and (rype_idx is not None or minimap2_idx is not None):
        raise _submission_bad_input(
            "host filtering accepts EITHER host_reference_idx (legacy single "
            "reference) OR host_rype_reference_idx (+ optional "
            "host_minimap2_reference_idx), not both"
        )

    # Enabled but no reference key at all: name BOTH layouts so a caller who
    # dropped (or typo'd) their key isn't pointed at a key they never set — the
    # bare two-reference fallthrough below would otherwise blame
    # host_rype_reference_idx even for a legacy 1.1.0 submission.
    if legacy_idx is None and rype_idx is None and minimap2_idx is None:
        raise _submission_bad_input(
            "host_filter_enabled requires host_reference_idx (legacy single "
            "reference) or host_rype_reference_idx (two-reference layout)"
        )

    if legacy_idx is not None:
        return await _resolve_host_filter_legacy(pool, legacy_idx)
    return await _resolve_host_filter_two_reference(pool, rype_idx, minimap2_idx)


async def _resolve_host_filter_two_reference(
    pool: asyncpg.Pool | asyncpg.Connection,
    rype_idx: Any,
    minimap2_idx: Any,
) -> dict[str, Path]:
    """Two-reference host filter (fastq-to-parquet/1.2.0): bind the rype index from
    the REQUIRED `host_rype_reference_idx` and, when set, the minimap2 index from
    `host_minimap2_reference_idx` — each from its own reference. See
    `_resolve_host_filter_indexes`."""
    bound: dict[str, Path] = {
        "host_rype_path": await _resolve_required_host_index(
            pool,
            _coerce_reference_idx(rype_idx, "host_rype_reference_idx"),
            HOST_FILTER_INDEX_TYPE_RYPE,
            "host_rype_reference_idx",
        )
    }
    if minimap2_idx is not None:
        bound["host_minimap2_path"] = await _resolve_required_host_index(
            pool,
            _coerce_reference_idx(minimap2_idx, "host_minimap2_reference_idx"),
            HOST_FILTER_INDEX_TYPE_MINIMAP2,
            "host_minimap2_reference_idx",
        )
    return bound


async def _resolve_syndna_index(
    pool: asyncpg.Pool | asyncpg.Connection,
    *,
    action_context: dict[str, Any],
) -> dict[str, Path]:
    """Resolve the syndna spike-in minimap2 index when syndna is enabled, else {}.

    A syndna reference is just another **minimap2** reference —
    `_resolve_reference_index_path` is generic on `index_type`, so no new index type,
    builder, or `derived_store` path is needed. Binds `syndna_minimap2_path` for the
    syndna step's input. (The spike-in inserts are the subject sequences; a read
    aligning to one at high identity is a spike-in — see `jobs/syndna.py`.)

    Run before the step loop (and so before the mask mint) for the same reason the
    host arms are: a stale, deleted, or non-active `syndna_reference_idx` must fail
    at SUBMISSION rather than sail into the mask identity hash and die deep in the
    step loop. Gated by `syndna_enabled`; enabled without a reference is a contract
    error, not a silent no-op.

    `_resolve_required_host_index` is named for its first caller but is generic —
    it maps unknown / non-active / index-not-built to a SUBMISSION BAD_INPUT.
    """
    if not action_context.get("syndna_enabled"):
        return {}
    syndna_idx = action_context.get("syndna_reference_idx")
    if syndna_idx is None:
        raise _submission_bad_input("syndna_enabled requires syndna_reference_idx")
    return {
        SYNDNA_MINIMAP2_BINDING: await _resolve_required_host_index(
            pool,
            _coerce_reference_idx(syndna_idx, "syndna_reference_idx"),
            HOST_FILTER_INDEX_TYPE_MINIMAP2,
            "syndna_reference_idx",
        )
    }


async def _resolve_host_filter_legacy(
    pool: asyncpg.Pool | asyncpg.Connection,
    host_reference_idx: Any,
) -> dict[str, Path]:
    """Legacy single-reference host filter (fastq-to-parquet/1.1.0):
    `host_reference_idx` names ONE active reference; bind whichever of its
    rype/minimap2 indexes exist (>=1 required; a missing one skips that stage).
    Preserved for 1.1.0 back-compat. See `_resolve_host_filter_indexes`."""
    host_reference_idx = _coerce_reference_idx(host_reference_idx, "host_reference_idx")

    # Resolve each index type independently: a host reference may carry only one
    # (rype-only / minimap2-only). A missing index type (ReferenceIndexNotBuilt)
    # is non-fatal — that stage is simply skipped — but an unknown or non-active
    # reference is a hard BAD_INPUT, and a reference with NEITHER index can't
    # filter anything, so it's rejected too.
    bound: dict[str, Path] = {}
    for index_type, binding in (
        (HOST_FILTER_INDEX_TYPE_RYPE, "host_rype_path"),
        (HOST_FILTER_INDEX_TYPE_MINIMAP2, "host_minimap2_path"),
    ):
        try:
            bound[binding] = Path(
                await _resolve_reference_index_path(pool, host_reference_idx, index_type)
            )
        except ReferenceNotFound as exc:
            raise _submission_bad_input(
                f"host_reference_idx={host_reference_idx} references an unknown reference"
            ) from exc
        except ReferenceIndexNotBuilt:
            # This index type wasn't built for the reference — skip its stage.
            continue
        except ValueError as exc:
            # Reference not active (build may be mid-flight) — hard error.
            raise _submission_bad_input(str(exc)) from exc
    if not bound:
        raise _submission_bad_input(
            f"host_reference_idx={host_reference_idx} has neither a "
            f"{HOST_FILTER_INDEX_TYPE_RYPE!r} nor a {HOST_FILTER_INDEX_TYPE_MINIMAP2!r} index; "
            "a host reference must carry at least one host-filter index"
        )
    return bound


# Binding name the runner stages the canonical adapter set (a Parquet) under. A
# step that lists this in its `inputs` (the qc step) signals the runner to
# materialize the adapter set before the step loop (see `_resolve_qc_adapters`).
# Binding for the syndna spike-in minimap2 index (the syndna step's input).
SYNDNA_MINIMAP2_BINDING = "syndna_minimap2_path"

QC_ADAPTER_BINDING = "adapter_parquet"

# The DuckLake table holding actual sequence bytes (reference_sequences is
# metadata only). Must match the data plane's ALLOWED_TABLES whitelist and the
# route's _DOGET_ALLOWED_TABLES.
_REFERENCE_CHUNKS_TABLE = "reference_sequence_chunks"


def _do_get_reference_sequence_chunks(
    data_plane_url: str, ticket_bytes: bytes
) -> list[tuple[int, int, str]]:
    """Synchronous Flight DoGet of a reference's sequence chunks — runs in a
    thread executor (pyarrow.flight is sync). Returns (feature_idx, chunk_index,
    chunk_data) rows. Mirrors `actions.library._do_action`'s client
    use; pyarrow imported lazily to keep it off the module hot path. Isolated as
    a module function so unit tests stub the real DoGet."""
    import pyarrow.flight as flight  # noqa: PLC0415

    with flight.FlightClient(data_plane_url) as client:
        table = client.do_get(flight.Ticket(ticket_bytes)).read_all()
    cols = {
        name: table.column(name).to_pylist()
        for name in ("feature_idx", "chunk_index", "chunk_data")
    }
    return list(zip(cols["feature_idx"], cols["chunk_index"], cols["chunk_data"], strict=True))


def _write_adapter_parquet(rows: list[tuple[int, int, str]], out_path: Path) -> int:
    """Reassemble chunked sequences (group by feature_idx, order by chunk_index,
    concat chunk_data — the same string_agg the data plane documents) into a
    Parquet at `out_path`, one row per feature with columns `feature_idx` (BIGINT,
    provenance) and `sequence` (VARCHAR, the adapter). Rows are sorted by
    feature_idx for determinism; the qc job reads only `sequence` via
    `read_parquet`. Returns the sequence count. Raises ValueError on an empty set
    — an adapter reference with no sequences is a misconfiguration, not a valid QC
    input.

    Parquet (not FASTA) keeps the adapter set in the same columnar format as the
    reads it trims, so the qc job reads it with `read_parquet` and no FASTA
    parsing. pyarrow (already this module's Flight dependency) writes it directly
    from the reassembled rows — no DuckDB connection needed on the control plane's
    pre-loop path.

    Input contract (the reference-load flow, jobs/reference_load.py): chunk_data
    is a substring of a parsed FASTA record, so it is newline-free, and a feature
    is loaded exactly once with monotonic chunk_index (a reference is loaded once,
    pending→loading→active), so (feature_idx, chunk_index) is unique. Hence no
    newline sanitation or chunk dedup here — both would mask a real corruption we
    want to surface."""
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    by_feature: dict[int, list[tuple[int, str]]] = {}
    for feature_idx, chunk_index, chunk_data in rows:
        by_feature.setdefault(feature_idx, []).append((chunk_index, chunk_data))
    if not by_feature:
        raise ValueError("adapter reference returned no sequences")
    feature_ids = sorted(by_feature)
    sequences = [
        "".join(chunk for _, chunk in sorted(by_feature[feature_idx]))
        for feature_idx in feature_ids
    ]
    table = pa.table(
        {
            "feature_idx": pa.array(feature_ids, type=pa.int64()),
            "sequence": pa.array(sequences, type=pa.string()),
        }
    )
    pq.write_table(table, str(out_path))
    return len(by_feature)


def _workflow_needs_adapters(steps: list[Any]) -> bool:
    """True iff some entry declares `adapter_parquet` as an (optional) input — the
    signal the runner must materialize the adapter set before the step loop."""
    for entry in steps:
        names = list(getattr(entry, "inputs", []) or []) + list(
            getattr(entry, "optional_inputs", []) or []
        )
        if QC_ADAPTER_BINDING in names:
            return True
    return False


async def _resolve_qc_adapters(
    pool: asyncpg.Pool | asyncpg.Connection,
    *,
    default_adapter_reference_idx: int | None,
    data_plane_url: str,
    signing_key: bytes,
    workspace: Path,
) -> dict[str, Path]:
    """Materialize the canonical adapter set as a local one-`sequence`-column
    Parquet for the QC step.

    Run before the step loop when `_workflow_needs_adapters`. Resolves the
    configured `artifact_sequence_set` reference, signs + DoGets its sequence
    chunks from the data plane, reassembles them, and writes
    `<workspace>/adapters.parquet` (the shared-FS ticket root every compute node
    sees) — bound to the qc step as `adapter_parquet`. Re-run safe: a resume
    re-materializes the same file (DoGet is read-only).

    Like `_resolve_host_filter_indexes`, every failure raises a
    SUBMISSION-attributed BAD_INPUT the outer handler turns into a FAILED ticket:
    no configured default, an unknown / wrong-kind / non-active reference, or an
    empty adapter set."""
    if default_adapter_reference_idx is None:
        raise _submission_bad_input(
            "this workflow needs an adapter set but no default adapter reference is "
            "configured — set QIITA_DEFAULT_ADAPTER_REFERENCE_IDX to the loaded "
            "artifact_sequence_set reference_idx"
        )
    # NOTE: single-gate (kind/status checked here, then DoGet) — same TOCTOU
    # shape as _resolve_reference_index_path. Safe for a canonical, static
    # adapter set that nothing transitions out of `active` mid-run; revisit if
    # the adapter reference ever gains a rotation lifecycle.
    row = await pool.fetchrow(
        "SELECT kind, status FROM qiita.reference WHERE reference_idx = $1",
        default_adapter_reference_idx,
    )
    if row is None:
        raise _submission_bad_input(
            f"default adapter reference {default_adapter_reference_idx} does not exist"
        )
    if row["kind"] != "artifact_sequence_set":
        raise _submission_bad_input(
            f"default adapter reference {default_adapter_reference_idx} has kind "
            f"{row['kind']!r}, expected 'artifact_sequence_set'"
        )
    if row["status"] != ReferenceStatus.ACTIVE.value:
        raise _submission_bad_input(
            f"default adapter reference {default_adapter_reference_idx} status is "
            f"{row['status']!r}, must be {ReferenceStatus.ACTIVE.value!r}"
        )

    ticket = sign_ticket(
        table=_REFERENCE_CHUNKS_TABLE,
        filter={"reference_idx": [default_adapter_reference_idx]},
        secret=signing_key,
    )
    # A Flight failure (data plane unreachable / errored) raises
    # pyarrow.flight.FlightError, which is NOT a BackendFailure — letting it
    # escape this pre-loop pass would hit run_workflow's bare `except Exception`,
    # which records stage=STEP_RUN with step_name=None and so VIOLATES the
    # work_ticket_failure_step_name_consistent CHECK (step_run ⇒ step_name NOT
    # NULL) — the failure transition itself would throw and strand the ticket in
    # PROCESSING. Wrap it as a SUBMISSION failure like every other pre-loop
    # resolver. (Not retried in place: the operator resubmits if the data plane
    # was down.)
    try:
        rows = await asyncio.get_event_loop().run_in_executor(
            None, _runner_pkg._do_get_reference_sequence_chunks, data_plane_url, ticket
        )
    except Exception as exc:
        raise _submission_bad_input(
            f"could not fetch adapter sequences for reference "
            f"{default_adapter_reference_idx} from the data plane: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    workspace.mkdir(parents=True, exist_ok=True)
    adapter_parquet = workspace / "adapters.parquet"
    try:
        _write_adapter_parquet(rows, adapter_parquet)
    except ValueError as exc:
        adapter_parquet.unlink(missing_ok=True)
        raise _submission_bad_input(
            f"default adapter reference {default_adapter_reference_idx}: {exc}"
        ) from exc
    return {QC_ADAPTER_BINDING: adapter_parquet}
