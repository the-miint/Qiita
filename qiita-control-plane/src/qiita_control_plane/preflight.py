"""Reading per-sample facts back out of a stored run-preflight SQLite blob.

The blob is stored raw on `sequenced_pool.run_preflight_blob` and is the SINGLE
SOURCE OF TRUTH for a sample's intake intent — nothing is copied into a
sequenced_sample column. Both the client (the submit gestures) and the server (the
pool-roster route) therefore parse it, and this module is where the parsing lives
so the two cannot drift.

That drift is the bug this module exists to prevent: `cli/user/pacbio.py` and the
`sequenced-sample` roster route both need the same `pacbio_sample` join, and a
copy in each is the same class of defect as a duplicated SQL predicate in two
languages.

**PacBio is a provisional seam.** `run_preflight` ships `get_illumina_sample_info`
but no `get_pacbio_sample_info` (verified absent on the pinned SHA), so the PacBio
facts are read by joining `pacbio_sample` directly. When that upstream reader
lands, replace `pacbio_protocol_by_barcode`'s body with a call to it and delete
`PACBIO_SAMPLE_JOIN`; callers depend only on the `PacbioProtocol` contract.
"""

from __future__ import annotations

import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path

# Sheet types the PacBio path recognizes. `pacbio_absquant` is the absolute-
# quantification protocol — the one that carries SynDNA spike-ins.
SHEET_TYPE_PACBIO_ABSQUANT = "pacbio_absquant"
_PACBIO_SHEET_PREFIX = "pacbio"


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
PACBIO_SAMPLE_JOIN = """
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


@dataclass(frozen=True, slots=True)
class PacbioProtocol:
    """The per-sample PacBio facts the read-mask submit derives its gates from.

    `sheet_type` is run-level (every row of a pre-flight carries the same one), so
    one pre-flight file is one protocol; it is repeated per sample so a caller
    holding a single row needs no second lookup.

    The read-mask gates, for reference (derived by the submit, not here — this
    module reports facts, not policy):
        syndna_enabled = sheet_type == 'pacbio_absquant'
        lima_enabled   = twist_adaptor_id filled AND NOT syndna_is_twisted
    """

    sheet_type: str
    twist_adaptor_id: str | None
    syndna_is_twisted: bool | None
    human_filtering: bool | None


def is_pacbio_sheet_type(sheet_type: str | None) -> bool:
    return bool(sheet_type) and sheet_type.startswith(_PACBIO_SHEET_PREFIX)


def run_sheet_type(conn: sqlite3.Connection) -> str | None:
    """The pre-flight's run-level sheet_type, or None when it cannot be read.

    Used to route a blob to the Illumina or the PacBio reader. `get_run_legacy_format`
    takes a CURSOR (not a connection) and returns `(legacy_format_idx, sheet_type,
    version)`; `get_single_run_idx` takes the connection. Mismatching the two is an
    AttributeError deep inside run_preflight, not a clean failure — hence the
    explicit `.cursor()`."""
    from run_preflight.db import get_run_legacy_format, get_single_run_idx  # noqa: PLC0415

    try:
        run_idx = get_single_run_idx(conn)
        row = get_run_legacy_format(conn.cursor(), run_idx)
    except sqlite3.DatabaseError, ValueError, IndexError, TypeError:
        return None
    return None if row is None else row[1]


def pacbio_protocol_by_barcode(conn: sqlite3.Connection) -> dict[str, PacbioProtocol]:
    """Map each PacBio sample's `barcode_id` to its protocol facts.

    The barcode IS the `sequenced_pool_item_id` the PacBio composer assigns (see
    `cli/user/pacbio.py`), so this keys the pool roster directly — the PacBio
    analogue of the Illumina map keyed on `str(illumina_sample_idx)`.

    Returns `{}` when the blob is not a PacBio pre-flight, so a caller can try both
    readers without branching on the exception type. Propagates
    `sqlite3.DatabaseError` for an unreadable blob — the roster route degrades that
    to "unknown", the CLI fails fast."""
    from run_preflight.db import get_single_run_idx  # noqa: PLC0415

    sheet_type = run_sheet_type(conn)
    if not is_pacbio_sheet_type(sheet_type):
        return {}
    run_idx = get_single_run_idx(conn)
    rows = conn.execute(PACBIO_SAMPLE_JOIN, (run_idx,)).fetchall()
    out: dict[str, PacbioProtocol] = {}
    for row in rows:
        barcode = row[1]
        if barcode is None:
            continue
        out[str(barcode)] = PacbioProtocol(
            sheet_type=sheet_type,
            twist_adaptor_id=row[2],
            # sqlite has no bool; the columns are 0/1/NULL.
            syndna_is_twisted=None if row[3] is None else bool(row[3]),
            human_filtering=None if row[6] is None else bool(row[6]),
        )
    return out


def pacbio_protocol_from_blob(blob: bytes) -> dict[str, PacbioProtocol]:
    """`pacbio_protocol_by_barcode` over a stored blob.

    run_preflight operates on a file-backed sqlite3 connection, so the blob is
    materialized to a private temp file — the same shape
    `_human_filtering_by_item_id` and `_apply_preflight_lane_update` use. The
    run_preflight import is lazy and local so the git-pinned dependency loads only
    on this path."""
    from run_preflight import open_db_file  # noqa: PLC0415

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "preflight.db"
        db_path.write_bytes(blob)
        conn = open_db_file(str(db_path))
        try:
            return pacbio_protocol_by_barcode(conn)
        finally:
            conn.close()
