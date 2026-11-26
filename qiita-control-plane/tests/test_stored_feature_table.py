"""The stored-value feature table's COO: public names only, nothing densified. The
fixture runs are amplicon."""

import hashlib
import io
import types
import zipfile

import pyarrow as pa
import pytest
from qiita_common.auth_constants import MAX_NAME_LENGTH, MAX_VERSION_LENGTH
from qiita_common.models.feature_table import FeatureTableSample
from qiita_common.models.reference import ReferenceHandle
from qiita_common.taxonomy import RANK_COLUMNS

from qiita_control_plane.stored_feature_table import (
    SOURCES,
    Cohort,
    ProcessedSample,
    build_features,
    build_taxonomy,
    build_values,
    fetch_taxonomy,
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
    answer = unpack_feature_table(pack_feature_table(cohort, values, features))
    assert answer.samples.method == "amplicon"
    assert answer.samples.parameters == {"trim": 150, "orient_primer": False}
    assert [s.export_id for s in answer.samples.samples] == ["QM1"]
    assert answer.values.equals(values)
    assert answer.features.equals(features)
    # No taxonomy was asked for, so the answer carries none and names no reference.
    assert answer.taxonomy is None
    assert answer.samples.taxonomy is None


def test_a_taxonomy_travels_with_the_reference_that_classified_it():
    cohort = _cohort("QM1")
    values, features = build_coo(
        cohort,
        _membership([(1, 10, 5)]),
        _sequences([(10, "ACGT")]),
        _labels([(10, "QF10")]),
    )
    taxonomy = build_taxonomy(
        _labels([(10, "QF10")]),
        _taxonomy([(10, "Bacteria", None, None, None, None, None, None, None)]),
    )
    reference = ReferenceHandle(name="a-reference", version="1.0")
    answer = unpack_feature_table(
        pack_feature_table(cohort, values, features, taxonomy=(reference, taxonomy))
    )
    assert answer.samples.taxonomy == reference
    assert answer.taxonomy.equals(taxonomy)


def test_a_body_without_every_part_is_refused():
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w") as zf:
        zf.writestr("samples.json", "{}")
    with pytest.raises(ValueError, match="values.arrows"):
        unpack_feature_table(sink.getvalue())


def _taxonomy(rows):
    """A reference_taxonomy answer: (feature_idx, <unprefixed ranks>)."""
    cols = {"feature_idx": pa.array([r[0] for r in rows], pa.int64())}
    for i, rank in enumerate(RANK_COLUMNS, start=1):
        cols[rank] = pa.array([r[i] for r in rows], pa.string())
    return pa.table(cols)


def test_taxonomy_keys_each_feature_by_handle_and_restores_rank_prefixes():
    table = build_taxonomy(
        _labels([(10, "QF10")]),
        _taxonomy([(10, "Bacteria", "Bacillota", None, None, None, None, None, None)]),
    )
    assert table.column_names == ["feature", *RANK_COLUMNS]
    row = table.to_pylist()[0]
    assert row["feature"] == "QF10"
    # Prefixes restored; an unresolved rank stays NULL, not a bare "c__".
    assert row["domain"] == "d__Bacteria"
    assert row["phylum"] == "p__Bacillota"
    assert row["class"] is None


def test_taxonomy_keeps_a_feature_the_reference_does_not_classify():
    # A feature (20) with no taxonomy row survives with all-NULL ranks: the query is a
    # lookup, not a filter.
    table = build_taxonomy(
        _labels([(10, "QF10"), (20, "QF20")]),
        _taxonomy([(10, "Bacteria", None, None, None, None, None, None, None)]),
    )
    rows = {r["feature"]: r for r in table.to_pylist()}
    assert rows["QF20"]["domain"] is None
    assert rows["QF10"]["domain"] == "d__Bacteria"


def test_a_reference_with_two_rows_for_one_feature_is_refused():
    # With no representative reduction on this axis, a duplicate row would fan the
    # feature into two answer rows with disagreeing lineages, so it fails loud as the
    # genome axis does.
    with pytest.raises(ValueError, match="more than one row"):
        build_taxonomy(
            _labels([(10, "QF10")]),
            _taxonomy(
                [
                    (10, "Bacteria", None, None, None, None, None, None, None),
                    (10, "Archaea", None, None, None, None, None, None, None),
                ]
            ),
        )


# --- fetch_taxonomy: the two-ticket readiness logic, over a fake Flight stream ---


class _FakeStream:
    """A FlightStreamReader stand-in: `read_all()` for the roster read, and iteration
    (chunks with `.data.num_rows`) for the whole-reference readiness probe."""

    def __init__(self, table):
        self._table = table

    def read_all(self):
        return self._table

    def __iter__(self):
        for batch in self._table.to_batches():
            yield types.SimpleNamespace(data=batch)


class _FakeFlightClient:
    """Answers `do_get` from a queue of tables, in call order (roster, then, only if the
    roster was empty, the whole-reference probe)."""

    def __init__(self, responses):
        self._responses = list(responses)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def do_get(self, _ticket):
        return _FakeStream(self._responses.pop(0))


def _tax(n):
    """A reference_taxonomy answer with `n` rows (0 = empty)."""
    return pa.table(
        {
            "feature_idx": pa.array(list(range(n)), pa.int64()),
            "domain": pa.array(["d__X"] * n, pa.string()),
        }
    )


def _fetch(monkeypatch, responses):
    monkeypatch.setattr(
        "qiita_control_plane.stored_feature_table.flight.FlightClient",
        lambda _url: _FakeFlightClient(responses),
    )
    # The row-count check is `read_all_counted`'s own, tested in qiita-common.
    monkeypatch.setattr(
        "qiita_control_plane.stored_feature_table.read_all_counted",
        lambda client, ticket, *, name: client.do_get(ticket).read_all(),
    )
    return fetch_taxonomy(
        data_plane_url="grpc://x", signing_key=b"x" * 32, reference_idx=1, feature_idxs=[10]
    )


def test_fetch_taxonomy_returns_the_roster_rows_when_they_match(monkeypatch):
    out = _fetch(monkeypatch, [_tax(2)])
    assert out is not None and out.num_rows == 2


def test_fetch_taxonomy_returns_empty_when_loaded_but_nothing_classified(monkeypatch):
    # Roster empty, but the reference carries taxonomy: a real (empty) answer, not None.
    out = _fetch(monkeypatch, [_tax(0), _tax(3)])
    assert out is not None and out.num_rows == 0


def test_fetch_taxonomy_returns_none_when_no_taxonomy_is_loaded(monkeypatch):
    # Roster empty and the reference has no taxonomy at all: not loaded yet, so None.
    assert _fetch(monkeypatch, [_tax(0), _tax(0)]) is None


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


@pytest.mark.parametrize(
    ("field", "cap"), [("name", MAX_NAME_LENGTH), ("version", MAX_VERSION_LENGTH)]
)
def test_a_reference_handle_is_bounded_like_the_reference_it_names(field, cap):
    from pydantic import ValidationError

    handle = {"name": "a-reference", "version": "1"}
    assert getattr(ReferenceHandle(**{**handle, field: "x" * cap}), field) == "x" * cap
    with pytest.raises(ValidationError):
        ReferenceHandle(**{**handle, field: "x" * (cap + 1)})
