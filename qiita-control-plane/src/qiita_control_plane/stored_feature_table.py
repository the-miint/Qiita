"""A feature table of stored values: a selection over a cohort, answered as sparse COO.

Unlike `feature-table build`, which computes its table from the whole cohort, this one
selects values the lake already stores, one per (processed sample, feature), for one
or more runs of a method in `SOURCES`. Features share one `feature_idx` space, so a
feature two runs found is one row, named by its exported-feature handle; the
sequences sidecar maps that handle to its sequence and md5.

Merging runs is that selection, provided they are runs whose features compare
(`Source.merge_keys`). Three steps, each testable alone:
  * `resolve_cohort` picks the processed samples the request names, each a column
    under its QM;
  * `fetch_table` reads their values, or their features' sequences, under one signed
    scope;
  * `mint_feature_labels` names each feature, and `stage_values` / `stage_features`
    relabel the reads on `ft.FEATURE_AXIS` into public handles only.

Nothing is made dense. Internal idxs are join keys and never reach the output.
"""

from __future__ import annotations

import io
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import asyncpg
import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from qiita_common import analytic as ft
from qiita_common.amplicon_constants import (
    AMPLICON_MEMBERSHIP_VISIBLE_TABLE,
    AMPLICON_SEQUENCE_TABLE,
    ORIENT_PRIMER_BINDING,
    TRIM_BINDING,
)
from qiita_common.flight_stream import read_all_counted
from qiita_common.models import MAX_COHORT_PREP_SAMPLE_IDX
from qiita_common.models.feature_table import (
    FeatureTableRequest,
    FeatureTableSample,
    FeatureTableValueKind,
)

from .auth.tickets import sign_ticket
from .repositories.exported_feature import mint_observed_features
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
    # The table holding each feature's sequence, keyed by feature_idx.
    sequences_table: str
    # The run params whose difference makes two runs' features incomparable.
    merge_keys: tuple[str, ...]
    # What a feature is, for refusals ("these runs' ASVs ...").
    noun: str
    # What a feature id is, for the bundle manifest.
    feature_id_note: str
    # The exported-feature mint that names this method's features
    # (`repositories.exported_feature`), returning `feature_idx -> handle`.
    mint: Callable[..., Awaitable[dict[int, str]]]


# The registry of methods this endpoint can answer. A method belongs here when its
# values are a persisted per-cell COO — one stored value per (sample, feature); the
# metagenomic OGU table is cohort-pooled compute (served separately by
# `feature-table build`), so it is not one here. Keyed on the `processing.workflow`
# value `_source_of` reads off each run, so a key is a workflow name.
SOURCES: dict[str, Source] = {
    # The trim length is the ASV's length, and orientation decides which strand it
    # is read on: ASVs from runs differing in these are different features, so
    # merging them would yield disjoint blocks under one table's name. Other params
    # (the SortMeRNA reference, the workflow version) change which reads survive,
    # not what an ASV is.
    "amplicon": Source(
        method="amplicon",
        value_kind="count",
        values_table=AMPLICON_MEMBERSHIP_VISIBLE_TABLE,
        value_column="count",
        sequences_table=AMPLICON_SEQUENCE_TABLE,
        merge_keys=(TRIM_BINDING, ORIENT_PRIMER_BINDING),
        noun="ASVs",
        feature_id_note="exported-feature handle (QF); the sequence companion maps it to md5",
        mint=mint_observed_features,
    ),
}

# Dictionary-encoded names: each sample and feature id is sent once, and each row
# carries two small integer codes and its value. Plain strings would repeat the
# handle per observation, making the stream larger than a dense TSV.
_NAME = pa.dictionary(pa.int32(), pa.string())
VALUES_SCHEMA = pa.schema([("sample", _NAME), ("feature", _NAME), ("value", pa.float64())])
# Each feature's handle, sequence, length and md5 (`ft.FEATURE_SEQUENCE_TABLE`).
FEATURES_SCHEMA = pa.schema(
    [
        ("feature", pa.string()),
        ("sequence", pa.string()),
        ("length", pa.int32()),
        ("md5", pa.string()),
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


def _source_of(runs: list[int], found: dict[int, asyncpg.Record]) -> Source:
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
            f"a feature table of stored values serves {', '.join(sorted(SOURCES))} runs only: "
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
    return source


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
    source = _source_of(runs, {r["processing_idx"]: r for r in rows})

    processed = await pool.fetch(
        "SELECT ei.processing_idx, ei.prep_sample_idx, ei.export_id,"
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
        raise CohortError(404, "these runs processed none of the cohort's prep_samples")
    if any(r["biosample"] is None for r in processed):
        raise RuntimeError("a processed sample's biosample has no accession and no QB")

    samples = [
        ProcessedSample(
            processing_idx=r["processing_idx"],
            prep_sample_idx=r["prep_sample_idx"],
            column=FeatureTableSample(export_id=r["export_id"], biosample=r["biosample"]),
        )
        for r in processed
    ]
    cohort = Cohort(source=source, runs=runs, samples=samples)
    # A named prep_sample request is already bounded (`PrepSampleCohort` caps it at
    # MAX_COHORT_PREP_SAMPLE_IDX); a study cohort has no such bound, and the routes
    # read the whole Flight stream into one response, so refuse one too large to sign
    # as a single ticket rather than buffer an unbounded table. Narrowing is the
    # caller's to do (by prep_sample or a smaller study), since the table's meaning is
    # the whole cohort.
    if len(cohort.prep_sample_idxs) > MAX_COHORT_PREP_SAMPLE_IDX:
        raise CohortError(
            413,
            f"this cohort spans {len(cohort.prep_sample_idxs)} prep_samples, over the"
            f" {MAX_COHORT_PREP_SAMPLE_IDX} a single request serves; narrow it by"
            " prep_sample or a smaller study.",
        )
    return cohort


def fetch_table(*, data_plane_url: str, signing_key: bytes, cohort: Cohort, table: str) -> pa.Table:
    """Read `table` over the cohort's runs and prep_samples, the two keys the data
    plane scopes these tables by. Blocking (Flight is synchronous); callers run it
    off the event loop."""
    scope = {"processing_idx": cohort.runs, "prep_sample_idx": cohort.prep_sample_idxs}
    ticket = sign_ticket(table=table, filter=scope, secret=signing_key)
    with flight.FlightClient(data_plane_url) as client:
        return read_all_counted(client, ticket, name=table)


# The relation `stage_values` creates on the caller's connection (`stage_features`
# creates `ft.FEATURE_SEQUENCE_TABLE`).
VALUE_RELATION = "stored_value"


async def mint_feature_labels(
    pool: asyncpg.Pool, *, source: Source, created_by_idx: int, features: pa.Table
) -> pa.Table:
    """`source`'s mint response as Arrow `(feature_idx, export_feature_id)`, minting any
    handle that is absent. `features` is any cohort relation carrying a `feature_idx`
    column; the relabel reads the result as the feature label."""
    feature_idx = sorted({int(f) for f in features.column("feature_idx").to_pylist()})
    handles = await source.mint(pool, feature_idx=feature_idx, created_by_idx=created_by_idx)
    ordered = sorted(handles)
    return pa.table(
        {
            "feature_idx": pa.array(ordered, pa.int64()),
            "export_feature_id": pa.array([handles[f] for f in ordered], pa.string()),
        }
    )


def stage_features(con, *, labels: str, sequences: str) -> None:
    """Create `ft.FEATURE_SEQUENCE_TABLE` (feature_idx, feature, sequence, length, md5): each
    feature under its minted handle (`labels`), with the sequence and its md5 — the
    companion that maps the published handle back to a reproducible, sequence-derived id."""
    con.execute(ft.feature_label_table_sql(labels, ft.FEATURE_AXIS))
    con.execute(
        f"CREATE TEMP TABLE {ft.FEATURE_SEQUENCE_TABLE} AS"
        " SELECT s.feature_idx, l.feature_id AS feature, s.sequence,"
        "        length(s.sequence)::INTEGER AS length, md5(s.sequence) AS md5"
        f" FROM {sequences} s JOIN {ft.FEATURE_LABEL_TABLE} l USING (feature_idx)"
    )


def _clear_relabel(con) -> ft.LabelClearance:
    """The shared relabel diagnostics, returning the clearance `labelled_relation_sql`
    requires — the check that refuses a value whose sample has no QM or feature no handle."""
    cur = con.execute(ft.relabel_diagnostics_sql(ft.FEATURE_AXIS))
    row = dict(zip((column[0] for column in cur.description), cur.fetchone(), strict=True))
    return ft.check_relabel_diagnostics(**row, axis=ft.FEATURE_AXIS)


def stage_values(con, *, source: Source, names: str, values: str, labels: str) -> None:
    """Relabel `values` to public names into `VALUE_RELATION` (sample, feature, value)
    through the shared `analytic/label.py` relabel.

    The feature labels (`labels`, the mint response), the sample labels (`names`, each
    processed sample's QM) and the values feed `labelled_relation_sql` on
    `ft.FEATURE_AXIS`, held to the same `check_relabel_diagnostics` invariants as the OGU
    table. The trailing sum is a wire concern (a sparse cell is one `(sample, feature)`
    pair), not part of the relabel."""
    con.execute(ft.feature_label_table_sql(labels, ft.FEATURE_AXIS))
    con.execute(ft.sample_label_table_sql(names, ft.FEATURE_AXIS))
    con.execute(
        f"CREATE TABLE {ft.FEATURE_OUTPUT_TABLE} AS "
        f"SELECT processing_idx, prep_sample_idx, feature_idx, {source.value_column} AS value "
        f"FROM {values}"
    )
    con.execute(ft.labelled_relation_sql(clearance=_clear_relabel(con), axis=ft.FEATURE_AXIS))
    con.execute(
        f"CREATE TEMP TABLE {VALUE_RELATION} AS"
        " SELECT sample_id AS sample, feature_id AS feature, sum(value)::DOUBLE AS value"
        f" FROM {ft.LABELLED_RELATION} GROUP BY sample_id, feature_id"
    )


def names_table(cohort: Cohort) -> pa.Table:
    """(processing_idx, prep_sample_idx, export_id): each processed sample's QM, in the
    shape of the exported-identifier mint's response."""
    return pa.table(
        {
            "processing_idx": pa.array([s.processing_idx for s in cohort.samples], pa.int64()),
            "prep_sample_idx": pa.array([s.prep_sample_idx for s in cohort.samples], pa.int64()),
            "export_id": pa.array([s.column.export_id for s in cohort.samples], pa.string()),
        }
    )


def build_values(cohort: Cohort, values: pa.Table, labels: pa.Table) -> pa.Table:
    """The values `(sample, feature, value)` as Arrow, for the REST answer. `labels` is
    the exported-feature mint response that names each feature."""
    with duckdb.connect() as duck:
        duck.register("stored_values", values)
        duck.register("names", names_table(cohort))
        duck.register("labels", labels)
        stage_values(
            duck, source=cohort.source, names="names", values="stored_values", labels="labels"
        )
        # Ordered so the dictionary-encoded `sample`/`feature` codes are assigned in a
        # stable order, making the stream reproducible.
        table = duck.sql(
            f"SELECT sample, feature, value FROM {VALUE_RELATION} ORDER BY sample, feature"
        ).to_arrow_table()
    return table.cast(VALUES_SCHEMA)


def build_features(cohort: Cohort, sequences: pa.Table, labels: pa.Table) -> pa.Table:
    """The features `(feature, sequence, length, md5)` as Arrow, for the REST answer. The
    ticket scopes the sequences to the cohort's features, and `labels` is the mint
    response that names them."""
    with duckdb.connect() as duck:
        duck.register("sequences", sequences)
        duck.register("labels", labels)
        stage_features(duck, labels="labels", sequences="sequences")
        table = duck.sql(
            f"SELECT f.feature, f.sequence, f.length, f.md5 FROM {ft.FEATURE_SEQUENCE_TABLE} f"
        ).to_arrow_table()
    return table.cast(FEATURES_SCHEMA)


def arrow_stream_bytes(table: pa.Table) -> bytes:
    """`table` as an Arrow IPC stream: what the values and features routes send."""
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()
