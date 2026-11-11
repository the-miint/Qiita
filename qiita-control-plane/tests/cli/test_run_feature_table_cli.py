"""`qiita feature-table runs`: the bundle written from the `/feature-table` routes'
answers — an amplicon table's values, columns, each feature's sequence and md5, and
(on `--taxonomy`) its lineage."""

import hashlib

import duckdb
import pyarrow as pa
from qiita_common.api_paths import (
    PATH_FEATURE_TABLE_FEATURE,
    PATH_FEATURE_TABLE_PREFIX,
    PATH_FEATURE_TABLE_TAXONOMY,
    PATH_FEATURE_TABLE_VALUE,
)

from qiita_control_plane.run_feature_table import (
    ARROW_STREAM_MEDIA_TYPE,
    FEATURES_SCHEMA,
    TAXONOMY_SCHEMA,
    VALUES_SCHEMA,
    arrow_stream_bytes,
)

_RANKS = ("domain", "phylum", "class", "order", "family", "genus", "species", "strain")

_SAMPLES = [
    {"sample": "QM1", "export_id": "QM1", "biosample": "QB1"},
    {"sample": "QM2", "export_id": "QM2", "biosample": "QB2"},
]


def _answers(method, values, features, taxonomy=None):
    def call(method_, base_url, token, path, json=None, params=None):
        assert path.endswith("/sample")
        return {"method": method, "value_kind": "count", "samples": _SAMPLES}

    bodies = {
        f"{PATH_FEATURE_TABLE_PREFIX}{PATH_FEATURE_TABLE_VALUE}": values,
        f"{PATH_FEATURE_TABLE_PREFIX}{PATH_FEATURE_TABLE_FEATURE}": features,
    }
    if taxonomy is not None:
        bodies[f"{PATH_FEATURE_TABLE_PREFIX}{PATH_FEATURE_TABLE_TAXONOMY}"] = taxonomy

    def fetch_binary(base_url, token, path, *, accept, json=None, **_):
        return arrow_stream_bytes(bodies[path]), ARROW_STREAM_MEDIA_TYPE

    return call, fetch_binary


def _run(monkeypatch, tmp_path, method, values, features, extra=(), taxonomy=None):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user._parser import _build_parser

    call, fetch_binary = _answers(method, values, features, taxonomy)
    monkeypatch.setattr(_common, "read_token", lambda *a, **k: "qk_tok")
    monkeypatch.setattr(_common, "call", call)
    monkeypatch.setattr(_common, "fetch_binary", fetch_binary)
    out = tmp_path / "table.parquet"
    parser = _build_parser()
    args = parser.parse_args(
        [
            "--base-url",
            "http://cp",
            "feature-table",
            "runs",
            "--processing-idx",
            "7",
            "--study-idx",
            "3",
            "--format",
            "parquet",
            "--output",
            str(out),
            *extra,
        ]
    )
    assert args.handler(args, parser) == 0
    return out


def _values(rows):
    return pa.table(
        {
            "sample": [r[0] for r in rows],
            "feature": [r[1] for r in rows],
            "value": pa.array([r[2] for r in rows], pa.float64()),
        }
    ).cast(VALUES_SCHEMA)


def _features(rows):
    def _md5(s):
        return hashlib.md5(s.encode()).hexdigest() if s else None

    return pa.table(
        {
            "feature": [r[0] for r in rows],
            "sequence": pa.array([r[1] for r in rows], pa.string()),
            "length": pa.array([len(r[1]) if r[1] else None for r in rows], pa.int32()),
            "md5": pa.array([_md5(r[1]) for r in rows], pa.string()),
        }
    ).cast(FEATURES_SCHEMA)


def test_an_amplicon_bundle_maps_each_handle_to_sequence_and_md5(monkeypatch, tmp_path):
    out = _run(
        monkeypatch,
        tmp_path,
        "amplicon",
        _values([("QM1", "QF1", 5.0)]),
        _features([("QF1", "ACGT"), ("QF2", "TTTT")]),
    )
    with duckdb.connect() as con:
        mapping = con.execute(
            "SELECT feature_id, sequence, md5 FROM read_parquet(?)",
            [str(out.with_name("table.sequences.parquet"))],
        ).fetchall()
    # Only the features the table names, handle -> (sequence, md5).
    assert mapping == [("QF1", "ACGT", hashlib.md5(b"ACGT").hexdigest())]
    assert not out.with_name("table.taxonomy.parquet").exists()


def _taxonomy(rows):
    """A `/feature-table/taxonomy` answer: (feature, <prefix-restored ranks>)."""
    cols = {"feature": [r[0] for r in rows]}
    for i, rank in enumerate(_RANKS, start=1):
        cols[rank] = pa.array([r[i] for r in rows], pa.string())
    return pa.table(cols).cast(TAXONOMY_SCHEMA)


def test_taxonomy_flag_writes_each_asvs_lineage_beside_the_bundle(monkeypatch, tmp_path):
    out = _run(
        monkeypatch,
        tmp_path,
        "amplicon",
        _values([("QM1", "md5a", 5.0)]),
        _features([("md5a", "ACGT"), ("md5b", "TTTT")]),
        extra=(
            "--taxonomy",
            "--taxonomy-reference-name",
            "greengenes2",
            "--taxonomy-reference-version",
            "2024.09",
        ),
        taxonomy=_taxonomy(
            [
                ("md5a", "d__Bacteria", "p__Bacillota", None, None, None, None, None, None),
                ("md5b", "d__Archaea", None, None, None, None, None, None, None),
            ]
        ),
    )
    with duckdb.connect() as con:
        lineage = con.execute(
            "SELECT feature_id, domain, phylum FROM read_parquet(?) ORDER BY feature_id",
            [str(out.with_name("table.taxonomy.parquet"))],
        ).fetchall()
    # Only the feature the table names (md5a); the unused md5b is not written.
    assert lineage == [("md5a", "d__Bacteria", "p__Bacillota")]
