"""The amplicon feature table's rendering: ASV sequences by labelled prep_samples."""

import pyarrow as pa
import pytest

from qiita_control_plane.amplicon_feature_table import render_feature_table_tsv


def _membership(rows):
    return pa.table(
        {
            "prep_sample_idx": pa.array([r[0] for r in rows], pa.int64()),
            "processing_idx": pa.array([7] * len(rows), pa.int64()),
            "feature_idx": pa.array([r[1] for r in rows], pa.int64()),
            "count": pa.array([r[2] for r in rows], pa.int64()),
        }
    )


def _chunks(rows):
    return pa.table(
        {
            "feature_idx": pa.array([r[0] for r in rows], pa.int64()),
            "chunk_index": pa.array([r[1] for r in rows], pa.int32()),
            "chunk_data": pa.array([r[2] for r in rows], pa.string()),
        }
    )


def test_rows_are_asv_sequences_and_columns_are_labels():
    membership = _membership([(1, 100, 5), (1, 200, 2), (2, 200, 9)])
    # feature 200's sequence spans two chunks, out of order on the wire.
    chunks = _chunks([(100, 0, "TTTT"), (200, 1, "GG"), (200, 0, "AACC")])
    tsv = render_feature_table_tsv(membership, chunks, {1: "SAMEA10", 2: "SAMEA11"})
    assert tsv.splitlines() == [
        "#OTU ID\tSAMEA10\tSAMEA11",
        "AACCGG\t2\t9",
        "TTTT\t5\t0",
    ]


def test_no_internal_identifier_reaches_the_table():
    membership = _membership([(41, 9001, 3)])
    chunks = _chunks([(9001, 0, "ACGT")])
    tsv = render_feature_table_tsv(membership, chunks, {41: "SAMEA5"})
    assert "41" not in tsv
    assert "9001" not in tsv


def test_an_asv_without_a_sequence_is_refused():
    with pytest.raises(RuntimeError, match="no sequence"):
        render_feature_table_tsv(_membership([(1, 100, 5)]), _chunks([]), {1: "SAMEA1"})


def test_a_counted_prep_sample_without_a_label_is_refused():
    """A row whose prep_sample has no public label would need an internal id to print."""
    with pytest.raises(RuntimeError, match="no label"):
        render_feature_table_tsv(
            _membership([(1, 100, 5), (3, 100, 1)]), _chunks([(100, 0, "A")]), {1: "SAMEA1"}
        )
