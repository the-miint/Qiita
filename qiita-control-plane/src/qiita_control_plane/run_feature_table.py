"""A run feature table: a query over a cohort, answered as sparse COO.

A feature table is not a run's artifact. It is a selection — one or more amplicon
runs, over a study's samples or a named list — of the ASV counts the lake already
holds as COO rows, one per processed sample and feature. ASVs are content-deduped
into one feature space, so the same sequence found by two runs is one feature,
named by the md5 of its sequence, which the table carries.

Merging runs is that selection, nothing more, provided they are runs whose
features compare (`Source.merge_keys` — one trim length and orientation). Three
steps, each testable alone:
  * `resolve_cohort` picks the processed samples (one run's view of one
    prep_sample) the request names and gives each a public column name;
  * `cohort_tickets` signs the reads of their values and the ASV bytes, which
    `fetch_runs` performs;
  * `stage_values` turns those into the features `(feature, sequence, length)` and
    the values `(sample, feature, value)`, public handles only.

Nothing is made dense: a rapid-16S pool is ~8% dense, and density falls as runs
and samples are added. Internal idxs are join keys and never reach the output.
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass

import asyncpg
import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from qiita_common.actions import AMPLICON_ACTION_ID
from qiita_common.amplicon_constants import (
    AMPLICON_MEMBERSHIP_TABLE,
    AMPLICON_SEQUENCE_CHUNKS_TABLE,
)
from qiita_common.models.run_feature_table import (
    FeatureTableRequest,
    FeatureTableSample,
    FeatureTableValueKind,
)

from .auth.tickets import sign_ticket
from .sample_names import BIOSAMPLE_NAME_JOIN, BIOSAMPLE_NAME_SQL

ARROW_STREAM_MEDIA_TYPE = "application/vnd.apache.arrow.stream"


@dataclass(frozen=True)
class Source:
    """Where a method's runs keep their values, and what makes two comparable."""

    method: str
    value_kind: FeatureTableValueKind
    values_table: str
    # The values table's column holding a cell's value.
    value_column: str
    # The ASV-bytes table, keyed by feature_idx.
    sequences_table: str
    # The run params whose difference makes two runs' features incomparable.
    merge_keys: tuple[str, ...]
    # What a run counts, for refusals ("these runs' ASVs ...").
    noun: str


# The registry of methods this endpoint can answer. A method belongs here when its
# values are a persisted per-cell COO — one stored value per (sample, feature). The
# metagenomic OGU table is cohort-pooled compute over alignment data rather than a
# stored COO (served separately by `feature-table build`), so it is not one here; the
# two are expected to converge behind this request shape, a `Source` carrying how it
# produces its COO (a lake selection or a compute job) and `value_kind` widening past
# "count", once the compute path can answer under this route.
SOURCES: dict[str, Source] = {
    # The trim length is the ASV's length, and orientation decides which strand it
    # is read on: ASVs from runs differing in these are different features, so
    # merging them would yield disjoint blocks under one table's name. Other params
    # (the SortMeRNA reference, the workflow version) change which reads survive,
    # not what an ASV is.
    AMPLICON_ACTION_ID: Source(
        method=AMPLICON_ACTION_ID,
        value_kind="count",
        values_table=AMPLICON_MEMBERSHIP_TABLE,
        value_column="count",
        sequences_table=AMPLICON_SEQUENCE_CHUNKS_TABLE,
        merge_keys=("trim", "orient_primer"),
        noun="ASVs",
    ),
}

# Dictionary-encoded names: each sample and feature id is sent once, and each row
# carries two small integer codes and its value. Plain strings would repeat a
# 32-character md5 per observation, making the stream larger than a dense TSV.
_NAME = pa.dictionary(pa.int32(), pa.string())
VALUES_SCHEMA = pa.schema([("sample", _NAME), ("feature", _NAME), ("value", pa.float64())])
FEATURES_SCHEMA = pa.schema(
    [
        ("feature", pa.string()),
        ("sequence", pa.string()),
        ("length", pa.int32()),
    ]
)


class CohortError(Exception):
    """A request the cohort cannot answer; `status` is the HTTP status it maps to."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


@dataclass(frozen=True)
class ProcessedSample:
    processing_idx: int
    prep_sample_idx: int
    column: FeatureTableSample


@dataclass(frozen=True)
class Cohort:
    source: Source
    runs: list[int]
    samples: list[ProcessedSample]

    @property
    def prep_sample_idxs(self) -> list[int]:
        return sorted({s.prep_sample_idx for s in self.samples})


def _params(row: asyncpg.Record) -> dict:
    params = row["params"]
    return json.loads(params) if isinstance(params, str) else (params or {})


def _source_of(runs: list[int], found: dict[int, asyncpg.Record]) -> tuple[Source, dict]:
    """The one method the runs share, or a refusal naming what is wrong."""
    missing = sorted(p for p in runs if p not in found)
    foreign = {
        p: found[p]["workflow"] for p in runs if p in found and found[p]["workflow"] not in SOURCES
    }
    if missing or foreign:
        # Name what each bad run IS, not only what it is not: a run of another
        # method (e.g. long-read-assembly) belongs to a different feature table,
        # and a missing idx is a different mistake again.
        parts = []
        if foreign:
            parts.append("; ".join(f"{p} is {w}" for p, w in sorted(foreign.items())))
        if missing:
            parts.append(f"no run at {missing}")
        raise CohortError(
            404,
            f"feature-table runs serves {', '.join(sorted(SOURCES))} runs only: "
            + "; ".join(parts),
        )
    methods = {p: found[p]["workflow"] for p in runs}
    if len(set(methods.values())) > 1:
        detail = "; ".join(f"{p}: {m}" for p, m in methods.items())
        raise CohortError(409, f"these runs are of different methods: {detail}")
    source = SOURCES[next(iter(methods.values()))]
    settings = {p: tuple(_params(found[p]).get(k) for k in source.merge_keys) for p in runs}
    if len(set(settings.values())) > 1:
        detail = "; ".join(
            f"{p}: " + ", ".join(f"{k}={v}" for k, v in zip(source.merge_keys, s, strict=True))
            for p, s in settings.items()
        )
        raise CohortError(
            409,
            f"these runs' {source.noun} are not comparable"
            f" ({', '.join(source.merge_keys)} differ): {detail}",
        )
    return source, _params(found[runs[0]])


async def resolve_cohort(pool: asyncpg.Pool, request: FeatureTableRequest) -> Cohort:
    """The processed samples `request` names, each with its column name.

    The runs must be runs of one method and comparable (`_source_of`). A processed
    sample is a live QM (`exported_identifier`) of one of the runs — each workflow
    mints one per sample it reported on — whose prep_sample is in the named study
    (live link) or the named list, and not ENA-flagged. A named list that includes
    a prep_sample none of the runs reported on is refused rather than trimmed.
    """
    runs = sorted(set(request.processing_idx))
    rows = await pool.fetch(
        "SELECT processing_idx, workflow, params FROM qiita.processing"
        " WHERE processing_idx = ANY($1::bigint[])",
        runs,
    )
    source, _params_row = _source_of(runs, {r["processing_idx"]: r for r in rows})

    processed = await pool.fetch(
        "SELECT ei.processing_idx, ei.prep_sample_idx, ei.export_id,"
        "       COALESCE(b.biosample_accession, b.ena_sample_accession) AS accession,"
        f"      {BIOSAMPLE_NAME_SQL} AS biosample"
        "  FROM qiita.exported_identifier ei"
        "  JOIN qiita.prep_sample ps ON ps.idx = ei.prep_sample_idx"
        "  JOIN qiita.biosample b ON b.idx = ps.biosample_idx"
        f" {BIOSAMPLE_NAME_JOIN}"
        "  LEFT JOIN qiita.sequenced_sample ss ON ss.prep_sample_idx = ps.idx"
        " WHERE ei.processing_idx = ANY($1::bigint[]) AND NOT ei.retired"
        "   AND ss.ena_status IS NULL"
        "   AND ($2::bigint IS NULL OR EXISTS ("
        "     SELECT 1 FROM qiita.prep_sample_to_study pst"
        "      WHERE pst.prep_sample_idx = ps.idx AND pst.study_idx = $2 AND NOT pst.retired))"
        "   AND ($3::bigint[] IS NULL OR ps.idx = ANY($3::bigint[]))"
        " ORDER BY ei.prep_sample_idx, ei.processing_idx",
        runs,
        request.study_idx,
        request.prep_sample_idx,
    )
    if request.prep_sample_idx is not None:
        uncounted = sorted(set(request.prep_sample_idx) - {r["prep_sample_idx"] for r in processed})
        if uncounted:
            raise CohortError(
                404,
                f"{len(uncounted)} named prep_sample(s) were not processed by these runs"
                f" (prep_sample_idx {uncounted[:5]})",
            )
    if not processed:
        raise CohortError(404, "these runs processed none of the named samples")
    if any(r["biosample"] is None for r in processed):
        raise RuntimeError("a processed sample's biosample has no accession and no QB")

    # A deposited accession names one column only when no other column of this
    # table shares it (one biosample, one processed sample); otherwise each of
    # those columns falls back to its own QM, so the two stay distinguishable.
    per_accession: dict[str, int] = {}
    for r in processed:
        if r["accession"]:
            per_accession[r["accession"]] = per_accession.get(r["accession"], 0) + 1
    samples = [
        ProcessedSample(
            processing_idx=r["processing_idx"],
            prep_sample_idx=r["prep_sample_idx"],
            column=FeatureTableSample(
                sample=(
                    r["accession"]
                    if r["accession"] and per_accession[r["accession"]] == 1
                    else r["export_id"]
                ),
                export_id=r["export_id"],
                biosample=r["biosample"],
                processing_idx=r["processing_idx"],
            ),
        )
        for r in processed
    ]
    return Cohort(source=source, runs=runs, samples=samples)


def cohort_scope(cohort: Cohort) -> dict[str, list[int]]:
    """The one ticket filter every read of `cohort` signs: its runs over its
    prep_samples — the only two keys the data plane's DoGet scopes these tables by."""
    return {"processing_idx": cohort.runs, "prep_sample_idx": cohort.prep_sample_idxs}


def cohort_tickets(cohort: Cohort, signing_key: bytes) -> tuple[bytes, bytes]:
    """Signed DoGet tickets for the cohort's values and its ASV chunks."""
    scope = cohort_scope(cohort)
    source = cohort.source
    values = sign_ticket(table=source.values_table, filter=scope, secret=signing_key)
    sequences = sign_ticket(table=source.sequences_table, filter=scope, secret=signing_key)
    return values, sequences


def fetch_runs(
    *, data_plane_url: str, signing_key: bytes, cohort: Cohort, values: bool = True
) -> tuple[pa.Table | None, pa.Table]:
    """Read the cohort's values (unless `values` is False) and the ASV chunks they
    name. Blocking (Flight is synchronous); callers run it off the event loop."""
    values_ticket, sequences_ticket = cohort_tickets(cohort, signing_key)
    with flight.FlightClient(data_plane_url) as client:
        observed = client.do_get(flight.Ticket(values_ticket)).read_all() if values else None
        chunks = client.do_get(flight.Ticket(sequences_ticket)).read_all()
    return observed, chunks


# The relations `stage_values` creates on the caller's connection.
FEATURE_RELATION = "run_feature"
VALUE_RELATION = "run_value"


def stage_features(con, *, chunks: str) -> None:
    """Create `FEATURE_RELATION` (feature_idx, feature, sequence, length) from the
    ASV chunk relation `chunks`: each ASV's bytes reassembled, and its feature id,
    the md5 of its sequence (the QIIME 2 / Greengenes2 convention)."""
    con.execute(
        f"CREATE TEMP TABLE {FEATURE_RELATION} AS"
        " SELECT feature_idx, md5(seq) AS feature, seq AS sequence,"
        "        length(seq)::INTEGER AS length"
        " FROM (SELECT feature_idx, string_agg(chunk_data, '' ORDER BY chunk_index) AS seq"
        f"       FROM {chunks} GROUP BY feature_idx)"
    )


def _refuse_unnamed(con, *, names: str, values: str) -> None:
    unnamed = con.execute(
        f"SELECT count(*) FROM {values} ANTI JOIN {names} USING (processing_idx, prep_sample_idx)"
    ).fetchone()[0]
    if unnamed:
        raise RuntimeError(f"{unnamed} value(s) of a processed sample with no QM")


def stage_values(con, *, source: Source, names: str, values: str, chunks: str) -> None:
    """Create `FEATURE_RELATION` (feature, sequence, length) and `VALUE_RELATION`
    (sample, feature, value): the values in `values` relabelled to public names,
    one row per nonzero cell. `names` maps (processing_idx, prep_sample_idx) to a
    column name; `chunks` is the ASV chunk relation. Refuses rather than guesses: a
    value of a processed sample `names` lacks, or of a feature with no sequence in
    the lake, would need an internal idx as its name."""
    _refuse_unnamed(con, names=names, values=values)
    stage_features(con, chunks=chunks)
    missing = con.execute(
        f"SELECT count(DISTINCT feature_idx) FROM {values}"
        f" ANTI JOIN {FEATURE_RELATION} USING (feature_idx)"
    ).fetchone()[0]
    if missing:
        raise RuntimeError(f"{missing} observed ASV(s) have no sequence in the lake")
    con.execute(
        f"CREATE TEMP TABLE {VALUE_RELATION} AS"
        f" SELECT n.sample, f.feature, sum(m.{source.value_column})::DOUBLE AS value"
        f" FROM {values} m"
        f" JOIN {names} n USING (processing_idx, prep_sample_idx)"
        f" JOIN {FEATURE_RELATION} f USING (feature_idx)"
        " GROUP BY n.sample, f.feature"
    )


def names_table(cohort: Cohort) -> pa.Table:
    """(processing_idx, prep_sample_idx, sample): each processed sample's column name."""
    return pa.table(
        {
            "processing_idx": pa.array([s.processing_idx for s in cohort.samples], pa.int64()),
            "prep_sample_idx": pa.array([s.prep_sample_idx for s in cohort.samples], pa.int64()),
            "sample": pa.array([s.column.sample for s in cohort.samples], pa.string()),
        }
    )


def build_values(cohort: Cohort, values: pa.Table, chunks: pa.Table) -> pa.Table:
    """The values `(sample, feature, value)` as Arrow, for the REST answer."""
    with duckdb.connect() as duck:
        duck.register("run_values", values)
        duck.register("names", names_table(cohort))
        duck.register("chunks", chunks)
        stage_values(
            duck, source=cohort.source, names="names", values="run_values", chunks="chunks"
        )
        table = duck.sql(
            f"SELECT sample, feature, value FROM {VALUE_RELATION} ORDER BY sample, feature"
        ).to_arrow_table()
    return table.cast(VALUES_SCHEMA)


def build_features(cohort: Cohort, chunks: pa.Table) -> pa.Table:
    """The features `(feature, sequence, length)` as Arrow, for the REST answer. The
    ticket already scopes the chunks to the ASVs the cohort observed."""
    with duckdb.connect() as duck:
        duck.register("chunks", chunks)
        stage_features(duck, chunks="chunks")
        table = duck.sql(
            f"SELECT f.feature, f.sequence, f.length FROM {FEATURE_RELATION} f ORDER BY f.feature"
        ).to_arrow_table()
    return table.cast(FEATURES_SCHEMA)


def arrow_stream_bytes(table: pa.Table) -> bytes:
    """`table` as an Arrow IPC stream: what the values and features routes send."""
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()
