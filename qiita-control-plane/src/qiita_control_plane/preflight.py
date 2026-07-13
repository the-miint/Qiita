"""Reading per-sample facts back out of a stored run-preflight SQLite blob.

The blob is stored raw on `sequenced_pool.run_preflight_blob` and is the SINGLE
SOURCE OF TRUTH for a sample's intake intent — nothing is copied into a
sequenced_sample column. The pool-roster route therefore parses it server-side to
surface those facts on the roster, which is where `submit-host-filter-pool` reads
them to derive each sample's read-mask gates.

Two DIFFERENT kinds of fact live here, and they have different lifetimes:

  * PROTOCOL facts (`PacbioProtocol`: sheet_type / twist_adaptor_id /
    syndna_is_twisted) — library-prep truths about how the sample was built. They
    drive `lima_enabled` / `syndna_enabled`. The pre-flight is their only home and
    will stay so.
  * HOST-FILTERING intent (`human_filtering`) — policy, not prep. It drives
    `host_filter_enabled`. Its source is MOVING to sample metadata, so the blob
    stops being where it comes from; see the DOOMED section below.

They are deliberately kept in SEPARATE readers, rather than one row type carrying
both, so that migration is an excision and not surgery: delete the human_filtering
reader and its Illumina twin, repoint the one seam in the roster route, and the
protocol reader is untouched.

The PacBio facts come from kl-run-preflight's own `get_pacbio_sample_info` — the
same accessor `cli/user/pacbio.py::_read_pacbio_preflight_rows` uses at ingest, so
the values the roster reports are the values ingest validated. A parity test pins
the two readers to each other (`tests/test_preflight.py`).
"""

from __future__ import annotations

import sqlite3
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

# Sheet types the PacBio path recognizes. `pacbio_absquant` is the absolute-
# quantification protocol — the one that carries SynDNA spike-ins. The ingest CLI
# imports this constant rather than re-spelling the literal, so the sheet type has
# one spelling across the client and the server.
SHEET_TYPE_PACBIO_ABSQUANT = "pacbio_absquant"
_PACBIO_SHEET_PREFIX = "pacbio"


@dataclass(frozen=True, slots=True)
class PacbioProtocol:
    """The per-sample PacBio PREP facts the read-mask submit derives its gates from.

    `sheet_type` is run-level (a pre-flight carries exactly one), so one pre-flight
    is one protocol; it is repeated per sample so a caller holding a single roster
    row needs no second lookup.

    The read-mask gates, for reference (derived by the submit, not here — this
    module reports facts, not policy):
        syndna_enabled = sheet_type == 'pacbio_absquant'
        lima_enabled   = twist_adaptor_id filled AND syndna_is_twisted is False

    `human_filtering` is deliberately NOT a field here: it is host-filtering
    policy, not a prep fact, and its source is moving to sample metadata. Keeping
    it out means that move never has to un-pick a field out of this type.
    """

    sheet_type: str
    twist_adaptor_id: str | None
    syndna_is_twisted: bool | None


def is_pacbio_sheet_type(sheet_type: str | None) -> bool:
    return bool(sheet_type) and sheet_type.startswith(_PACBIO_SHEET_PREFIX)


@contextmanager
def open_blob(blob: bytes) -> Iterator[sqlite3.Connection]:
    """Open a stored pre-flight blob as a run_preflight sqlite3 connection.

    run_preflight operates on a FILE-backed connection, so the blob is
    materialized to a private temp file (the same shape
    `routes/sequencing_run.py::_apply_preflight_lane_update` uses). The
    run_preflight import is lazy and local so the git-pinned dependency loads only
    on this path.

    `open_db_file` opens read-WRITE and applies schema patches in place, which is
    exactly why the copy must be private and disposable — never the caller's bytes.
    """
    from run_preflight import open_db_file  # noqa: PLC0415

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "preflight.db"
        db_path.write_bytes(blob)
        conn = open_db_file(str(db_path))
        try:
            yield conn
        finally:
            conn.close()


def run_sheet_type(conn: sqlite3.Connection) -> str | None:
    """The pre-flight's run-level sheet_type; None only when the run RECORDS none.

    Used to route a blob to the Illumina or the PacBio reader. `get_run_legacy_format`
    takes a CURSOR (not a connection) and returns `(legacy_format_idx, sheet_type,
    version)`; `get_single_run_idx` takes the connection. Mismatching the two is an
    AttributeError deep inside run_preflight, not a clean failure — hence the explicit
    `.cursor()`.

    RAISES on an unreadable or internally inconsistent blob — it does NOT degrade to
    None. The distinction is load-bearing, and swallowing it caused a real bug: a
    caller cannot tell "this blob says it is not PacBio" from "this blob could not be
    read", and every caller treats the first as `{}` (no PacBio facts). A PacBio pool
    whose blob failed to parse would therefore present as a NON-PacBio pool — the
    roster would report `sheet_type: null`, `submit-host-filter-pool` would take the
    Illumina branch, and every ticket would be written `lima_enabled: false,
    syndna_enabled: false`. That is a case-5 pool masked with no lima and no syndna,
    whose spike-in count is then structurally zero: precisely the failure this chain's
    step order exists to prevent, reintroduced silently through the error path.

    `IndexError` / `TypeError` mean run_preflight's return shape drifted under a
    dependency bump — a broken contract, not a missing sheet type — so they are
    re-raised as ValueError rather than mistaken for "no sheet type".
    """
    from run_preflight.db import get_run_legacy_format, get_single_run_idx  # noqa: PLC0415

    try:
        run_idx = get_single_run_idx(conn)
        row = get_run_legacy_format(conn.cursor(), run_idx)
    except (IndexError, TypeError) as exc:
        raise ValueError(
            f"run_preflight returned an unexpected shape for the run's legacy format "
            f"({type(exc).__name__}: {exc}); the pinned run_preflight may have drifted"
        ) from exc
    return None if row is None else row[1]


def pacbio_protocol_by_sample_idx(conn: sqlite3.Connection) -> dict[str, PacbioProtocol]:
    """Map each PacBio sample's `pacbio_sample_idx` to its protocol facts.

    Keyed on `str(pacbio_sample_idx)` because THAT is the `sequenced_pool_item_id`
    the PacBio composer assigns (see `cli/user/pacbio.py` — the barcode is only the
    BAM-locating key, and it is not unique across PacBio protocols), so this map
    joins the pool roster directly. The Illumina analogue keys on
    `str(illumina_sample_idx)`.

    Returns `{}` when the blob is not a PacBio pre-flight, so a caller can probe
    both readers without branching on the exception type. Propagates
    `sqlite3.DatabaseError` / `ValueError` for an unreadable or internally
    inconsistent blob — the roster route degrades that to "unknown", the CLI fails
    fast.
    """
    from run_preflight.db import get_pacbio_sample_info  # noqa: PLC0415

    sheet_type = run_sheet_type(conn)
    if not is_pacbio_sheet_type(sheet_type):
        return {}
    return {
        str(info.sample_idx): PacbioProtocol(
            # Run-level, stamped onto every row: there is no per-sample sheet_type.
            sheet_type=sheet_type,
            twist_adaptor_id=info.kind_row.twist_adaptor_id or None,
            # Already coerced to bool | None by the accessor.
            syndna_is_twisted=info.kind_row.syndna_is_twisted,
        )
        for info in get_pacbio_sample_info(conn)
    }


def pacbio_protocol_from_blob(blob: bytes) -> dict[str, PacbioProtocol]:
    """`pacbio_protocol_by_sample_idx` over a stored blob."""
    with open_blob(blob) as conn:
        return pacbio_protocol_by_sample_idx(conn)


# =============================================================================
# Host-filtering intent — the DOOMED reader
# =============================================================================
#
# `human_filtering` is host-filtering POLICY, and its source is moving from the
# pre-flight blob to sample metadata. When that lands, this function and its
# Illumina twin (`routes/sequenced_sample.py::_human_filtering_by_item_id`) are
# DELETED, and the single seam that calls them
# (`routes/sequenced_sample.py::_pool_sample_facts_by_item_id`) is repointed at the
# metadata lookup. Nothing else moves: the roster still carries a `human_filtering`
# field and the CLI still reads it off the roster row, so no client changes.
#
# Keep this reader free of protocol facts so that deletion stays a clean excision.


def pacbio_human_filtering_by_sample_idx(conn: sqlite3.Connection) -> dict[str, bool]:
    """Map each PacBio sample's `pacbio_sample_idx` to its intake human_filtering.

    `get_pacbio_sample_info` omits this one intake fact, so it is read from the
    canonical run-scoped `run_pacbio_sample` view (which resolves a control's
    project to its plate primary) joined to `project.human_filtering` — the same
    pair `cli/user/pacbio.py` reads at ingest. Keyed to match
    `pacbio_protocol_by_sample_idx`.

    A sample whose project cannot be resolved is simply ABSENT from the map — the
    caller then reports None ("intent unknown"), and the submit guard turns that
    into an actionable abort rather than guessing. Defaulting here would either
    leak human reads or silently drop a filter the operator asked for.
    """
    from run_preflight.db import get_single_run_idx  # noqa: PLC0415

    if not is_pacbio_sheet_type(run_sheet_type(conn)):
        return {}
    run_idx = get_single_run_idx(conn)
    project_by_idx = conn.execute(
        "SELECT pacbio_sample_idx, project_name FROM run_pacbio_sample WHERE run_idx = ?",
        (run_idx,),
    ).fetchall()
    filtering_by_project = {
        name: bool(flag)
        for name, flag in conn.execute(
            "SELECT project_name, human_filtering FROM project"
        ).fetchall()
    }
    return {
        str(idx): filtering_by_project[project_name]
        for idx, project_name in project_by_idx
        if project_name in filtering_by_project
    }


def pacbio_human_filtering_from_blob(blob: bytes) -> dict[str, bool]:
    """`pacbio_human_filtering_by_sample_idx` over a stored blob."""
    with open_blob(blob) as conn:
        return pacbio_human_filtering_by_sample_idx(conn)
