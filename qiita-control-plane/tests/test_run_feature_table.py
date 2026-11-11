"""The run feature table's COO: public names only, nothing densified, for an
amplicon run's ASV counts."""

import hashlib

import pyarrow as pa
import pytest
from qiita_common.actions import AMPLICON_ACTION_ID
from qiita_common.models.run_feature_table import FeatureTableSample

from qiita_control_plane.run_feature_table import (
    SOURCES,
    Cohort,
    ProcessedSample,
    arrow_stream_bytes,
    build_features,
    build_taxonomy,
    build_values,
)

_RANKS = ("domain", "phylum", "class", "order", "family", "genus", "species", "strain")


def build_coo(cohort, values, sequences):
    return build_values(cohort, values, sequences), build_features(cohort, sequences)


def _cohort(*names):
    """Processed samples (run 7, prep_sample i) named `names` in order."""
    return Cohort(
        source=SOURCES[AMPLICON_ACTION_ID],
        runs=[7],
        samples=[
            ProcessedSample(
                processing_idx=7,
                prep_sample_idx=i,
                column=FeatureTableSample(
                    sample=n, export_id=n, biosample=f"QB{i}", processing_idx=7
                ),
            )
            for i, n in enumerate(names, start=1)
        ],
    )


def _membership(rows):
    return pa.table(
        {
            "prep_sample_idx": pa.array([r[0] for r in rows], pa.int64()),
            "processing_idx": pa.array([7] * len(rows), pa.int64()),
            "feature_idx": pa.array([r[1] for r in rows], pa.int64()),
            "count": pa.array([r[2] for r in rows], pa.int64()),
        }
    )


def _sequences(rows):
    """amplicon_sequence: one row per feature_idx, its bytes stored inline."""
    return pa.table(
        {
            "feature_idx": pa.array([r[0] for r in rows], pa.int64()),
            "sequence_hash": pa.array([md5(r[1]) for r in rows], pa.string()),
            "sequence_length_bp": pa.array([len(r[1]) for r in rows], pa.int64()),
            "sequence": pa.array([r[1] for r in rows], pa.string()),
        }
    )


def md5(s):
    return hashlib.md5(s.encode()).hexdigest()


def test_counts_are_sparse_and_named_by_column_and_md5():
    values, features = build_coo(
        _cohort("QM1", "QM2"),
        _membership([(1, 10, 5), (2, 20, 3)]),
        _sequences([(10, "ACGTACGT"), (20, "TTTT")]),
    )
    # One row per observation: no zero for QM1 x TTTT or QM2 x ACGTACGT.
    assert sorted(values.to_pylist(), key=lambda r: r["sample"]) == [
        {"sample": "QM1", "feature": md5("ACGTACGT"), "value": 5.0},
        {"sample": "QM2", "feature": md5("TTTT"), "value": 3.0},
    ]
    assert sorted(features.to_pylist(), key=lambda r: r["sequence"]) == [
        {"feature": md5("ACGTACGT"), "sequence": "ACGTACGT", "length": 8},
        {"feature": md5("TTTT"), "sequence": "TTTT", "length": 4},
    ]


def test_no_internal_identifier_reaches_the_output():
    table, features = build_coo(
        _cohort("QM1"), _membership([(1, 10, 5)]), _sequences([(10, "ACGT")])
    )
    assert table.column_names == ["sample", "feature", "value"]
    assert features.column_names == ["feature", "sequence", "length"]
    # Names travel once, as dictionaries; rows carry codes.
    assert all(pa.types.is_dictionary(table.schema.field(c).type) for c in ("sample", "feature"))


def test_a_value_of_an_unnamed_sample_is_refused():
    with pytest.raises(RuntimeError, match="no QM"):
        build_coo(_cohort("QM1"), _membership([(2, 10, 5)]), _sequences([(10, "ACGT")]))


def test_an_asv_without_a_sequence_is_refused():
    with pytest.raises(RuntimeError, match="no sequence"):
        build_coo(_cohort("QM1"), _membership([(1, 99, 5)]), _sequences([(10, "ACGT")]))


def test_the_values_round_trip_through_an_arrow_stream():
    values, _ = build_coo(_cohort("QM1"), _membership([(1, 10, 5)]), _sequences([(10, "ACGT")]))
    assert pa.ipc.open_stream(arrow_stream_bytes(values)).read_all().equals(values)


def _taxonomy(rows):
    """A reference_taxonomy answer: (feature_idx, <unprefixed ranks>)."""
    cols = {"feature_idx": pa.array([r[0] for r in rows], pa.int64())}
    for i, rank in enumerate(_RANKS, start=1):
        cols[rank] = pa.array([r[i] for r in rows], pa.string())
    return pa.table(cols)


def test_taxonomy_keys_each_asv_by_md5_and_restores_rank_prefixes():
    table = build_taxonomy(
        _sequences([(10, "ACGTACGT")]),
        _taxonomy([(10, "Bacteria", "Bacillota", None, None, None, None, None, None)]),
    )
    assert table.column_names == ["feature", *_RANKS]
    row = table.to_pylist()[0]
    assert row["feature"] == md5("ACGTACGT")
    # Prefixes restored; an unresolved rank stays NULL, not a bare "c__".
    assert row["domain"] == "d__Bacteria"
    assert row["phylum"] == "p__Bacillota"
    assert row["class"] is None


def test_taxonomy_keeps_an_asv_the_reference_does_not_place():
    # An observed ASV (20) with no taxonomy row survives with all-NULL ranks — the
    # query is a lookup, not a filter.
    table = build_taxonomy(
        _sequences([(10, "ACGT"), (20, "TTTT")]),
        _taxonomy([(10, "Bacteria", None, None, None, None, None, None, None)]),
    )
    rows = {r["feature"]: r for r in table.to_pylist()}
    assert rows[md5("TTTT")]["domain"] is None
    assert rows[md5("ACGT")]["domain"] == "d__Bacteria"
