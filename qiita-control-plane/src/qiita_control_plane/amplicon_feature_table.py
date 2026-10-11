"""An amplicon run's feature table: ASV sequences by publicly named prep_samples.

Two halves, split so each is testable alone. `fetch_amplicon_run` reads the run's
cohort from the data plane (`amplicon_membership` and the ASV bytes in
`amplicon_sequence_chunks`, both scoped to the run by one signed ticket shape).
`render_feature_table_tsv` turns those into a table carrying public handles only:
each row is an ASV's own sequence, each column the label its caller supplies.
Neither `feature_idx` nor `prep_sample_idx` survives into the output; they are
join keys.

The output is BIOM-style TSV rather than BIOM because the web UI reads it as
text; `biom convert` turns it into BIOM.
"""

from __future__ import annotations

import io

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from qiita_common.amplicon_constants import (
    AMPLICON_MEMBERSHIP_TABLE,
    AMPLICON_SEQUENCE_CHUNKS_TABLE,
)

from .auth.tickets import run_signed_flight_call, sign_ticket

# The conventional first header cell of a BIOM-style TSV.
FEATURE_TABLE_ID_HEADER = "#OTU ID"


async def fetch_amplicon_run(
    *,
    data_plane_url: str,
    signing_key: bytes,
    processing_idx: int,
    prep_sample_idxs: list[int],
) -> tuple[pa.Table, pa.Table]:
    """Read one run's membership and ASV chunks for a non-empty cohort. A data
    plane failure raises `pyarrow.flight.FlightError`."""
    run_filter = {"processing_idx": [processing_idx], "prep_sample_idx": prep_sample_idxs}

    def _read(ticket: bytes) -> pa.Table:
        with flight.FlightClient(data_plane_url) as client:
            return client.do_get(flight.Ticket(ticket)).read_all()

    tables = []
    for table in (AMPLICON_MEMBERSHIP_TABLE, AMPLICON_SEQUENCE_CHUNKS_TABLE):
        tables.append(
            await run_signed_flight_call(
                lambda table=table: sign_ticket(table=table, filter=run_filter, secret=signing_key),
                _read,
            )
        )
    return tables[0], tables[1]


def render_feature_table_tsv(membership: pa.Table, chunks: pa.Table, labels: dict[int, str]) -> str:
    """Render `membership` as a TSV: one row per ASV (its sequence), one column per
    `labels` entry in the dict's order, zeros where a prep_sample has no count.

    Refuses rather than guesses: an ASV with no sequence would need its
    `feature_idx` as a row name, and a counted prep_sample with no label would need its
    `prep_sample_idx` as a column name — both internal identifiers."""
    with duckdb.connect() as duck:
        duck.register("membership", membership)
        duck.register("chunks", chunks)
        unlabelled = sorted(
            row[0]
            for row in duck.execute("SELECT DISTINCT prep_sample_idx FROM membership").fetchall()
            if row[0] not in labels
        )
        if unlabelled:
            raise RuntimeError(f"{len(unlabelled)} counted prep_sample(s) have no label")
        rows = duck.execute(
            "WITH seq AS ("
            "  SELECT feature_idx, string_agg(chunk_data, '' ORDER BY chunk_index) AS sequence"
            "  FROM chunks GROUP BY feature_idx"
            ")"
            " SELECT m.feature_idx, s.sequence, m.prep_sample_idx, m.count"
            " FROM membership m LEFT JOIN seq s USING (feature_idx)"
        ).fetchall()

    missing = {feature_idx for feature_idx, sequence, _, _ in rows if sequence is None}
    if missing:
        raise RuntimeError(f"{len(missing)} ASV(s) in the run have no sequence in the lake")
    counts: dict[str, dict[int, int]] = {}
    for _, sequence, prep_sample_idx, count in rows:
        counts.setdefault(sequence, {})[prep_sample_idx] = count

    order = list(labels)
    out = io.StringIO()
    out.write("\t".join([FEATURE_TABLE_ID_HEADER, *(labels[idx] for idx in order)]) + "\n")
    for sequence in sorted(counts):
        by_sample = counts[sequence]
        out.write("\t".join([sequence, *(str(by_sample.get(idx, 0)) for idx in order)]) + "\n")
    return out.getvalue()
