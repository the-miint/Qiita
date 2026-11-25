"""The stored-value feature table's COO: public names only, nothing densified. The
fixture runs are amplicon."""

import hashlib
import io
import zipfile

import pyarrow as pa
import pytest
from qiita_common.models.feature_table import FeatureTableSample

from qiita_control_plane.stored_feature_table import (
    SOURCES,
    Cohort,
    ProcessedSample,
    build_features,
    build_values,
    pack_feature_table,
    unpack_feature_table,
)


def build_coo(cohort, values, sequences, labels):
    return build_values(cohort, values, labels), build_features(cohort, sequences, labels)


def _cohort(*names):
    """Processed samples (run 7, prep_sample i) named `names` in order."""
    return Cohort(
        source=SOURCES["amplicon"],
        runs=[7],
        parameters={"trim": 150, "orient_primer": False},
        samples=[
            ProcessedSample(
                processing_idx=7,
                prep_sample_idx=i,
                column=FeatureTableSample(export_id=n, biosample=f"QB{i}"),
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


def _labels(rows):
    """exported-feature mint response: (feature_idx, export_feature_id)."""
    return pa.table(
        {
            "feature_idx": pa.array([r[0] for r in rows], pa.int64()),
            "export_feature_id": pa.array([r[1] for r in rows], pa.string()),
        }
    )


def md5(s):
    return hashlib.md5(s.encode()).hexdigest()


def test_counts_are_sparse_and_named_by_column_and_handle():
    values, features = build_coo(
        _cohort("QM1", "QM2"),
        _membership([(1, 10, 5), (2, 20, 3)]),
        _sequences([(10, "ACGTACGT"), (20, "TTTT")]),
        _labels([(10, "QF10"), (20, "QF20")]),
    )
    # One row per observation: no zero for QM1 x TTTT or QM2 x ACGTACGT.
    assert sorted(values.to_pylist(), key=lambda r: r["sample"]) == [
        {"sample": "QM1", "feature": "QF10", "value": 5.0},
        {"sample": "QM2", "feature": "QF20", "value": 3.0},
    ]
    # The feature companion maps the handle to its sequence and reproducible md5.
    assert sorted(features.to_pylist(), key=lambda r: r["sequence"]) == [
        {"feature": "QF10", "sequence": "ACGTACGT", "length": 8, "md5": md5("ACGTACGT")},
        {"feature": "QF20", "sequence": "TTTT", "length": 4, "md5": md5("TTTT")},
    ]


def test_no_internal_identifier_reaches_the_output():
    table, features = build_coo(
        _cohort("QM1"),
        _membership([(1, 10, 5)]),
        _sequences([(10, "ACGT")]),
        _labels([(10, "QF10")]),
    )
    assert table.column_names == ["sample", "feature", "value"]
    assert features.column_names == ["feature", "sequence", "length", "md5"]
    # Names travel once, as dictionaries; rows carry codes.
    assert all(pa.types.is_dictionary(table.schema.field(c).type) for c in ("sample", "feature"))


def test_a_value_of_an_unnamed_sample_is_refused():
    # The shared relabel refuses it: the value's sample has no label, so it would carry a
    # NULL sample_id — the same check the OGU table runs, not a second one.
    with pytest.raises(ValueError, match="processed sample with no public handle"):
        build_coo(
            _cohort("QM1"),
            _membership([(2, 10, 5)]),
            _sequences([(10, "ACGT")]),
            _labels([(10, "QF10")]),
        )


def test_a_feature_without_a_handle_is_refused():
    # A counted feature the mint did not name carries a NULL feature_id — the same relabel
    # check as the OGU table, now that the handle is minted rather than the sequence md5.
    with pytest.raises(ValueError, match="NULL feature_id"):
        build_coo(
            _cohort("QM1"),
            _membership([(1, 99, 5)]),
            _sequences([(99, "ACGT")]),
            _labels([(10, "QF10")]),
        )


def test_the_table_round_trips_through_one_body():
    cohort = _cohort("QM1")
    values, features = build_coo(
        cohort,
        _membership([(1, 10, 5)]),
        _sequences([(10, "ACGT")]),
        _labels([(10, "QF10")]),
    )
    samples, values_back, features_back = unpack_feature_table(
        pack_feature_table(cohort, values, features)
    )
    assert samples.method == "amplicon"
    assert samples.parameters == {"trim": 150, "orient_primer": False}
    assert [s.export_id for s in samples.samples] == ["QM1"]
    assert values_back.equals(values)
    assert features_back.equals(features)


def test_a_body_without_every_part_is_refused():
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w") as zf:
        zf.writestr("samples.json", "{}")
    with pytest.raises(ValueError, match="values.arrows"):
        unpack_feature_table(sink.getvalue())


async def test_features_are_named_by_the_methods_own_mint():
    """Which kind of handle names a method's features is the method's: the relabel mints
    through `Source.mint`, not a mint of its own choosing."""
    import dataclasses

    from qiita_control_plane.stored_feature_table import mint_feature_labels

    asked = []

    async def mint(pool, *, feature_idx, created_by_idx):
        asked.append((feature_idx, created_by_idx))
        return {f: f"QF{f}" for f in feature_idx}

    source = dataclasses.replace(SOURCES["amplicon"], mint=mint)
    labels = await mint_feature_labels(
        None, source=source, created_by_idx=5, features=pa.table({"feature_idx": [9, 3, 9]})
    )
    assert asked == [([3, 9], 5)]
    assert labels.to_pylist() == [
        {"feature_idx": 3, "export_feature_id": "QF3"},
        {"feature_idx": 9, "export_feature_id": "QF9"},
    ]
