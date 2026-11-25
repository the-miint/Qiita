"""`qiita feature-table select`: the bundle written from the `/feature-table` answer —
an amplicon table's values, columns, and each feature's sequence and md5."""

import hashlib
import json

import duckdb
import httpx
import pyarrow as pa
from qiita_common.api_paths import PATH_FEATURE_TABLE_PREFIX, PATH_FEATURE_TABLE_ROOT
from qiita_common.auth_constants import STALE_TOKEN_SCOPE_HEADER
from qiita_common.models.feature_table import FeatureTableSample

from qiita_control_plane.cli.user.stored_feature_table import MAP_NOTE
from qiita_control_plane.stored_feature_table import (
    FEATURE_TABLE_MEDIA_TYPE,
    FEATURES_SCHEMA,
    SOURCES,
    VALUES_SCHEMA,
    Cohort,
    ProcessedSample,
    pack_feature_table,
)

_SAMPLES = [
    {"export_id": "QM1", "biosample": "QB1"},
    {"export_id": "QM2", "biosample": "QB2"},
]
_PARAMETERS = {"trim": 150, "orient_primer": False}


def _body(values, features):
    cohort = Cohort(
        source=SOURCES["amplicon"],
        runs=[7],
        parameters=_PARAMETERS,
        samples=[
            ProcessedSample(processing_idx=7, prep_sample_idx=i, column=FeatureTableSample(**s))
            for i, s in enumerate(_SAMPLES, start=1)
        ],
    )
    return pack_feature_table(cohort, values, features)


def _run(monkeypatch, tmp_path, values, features, extra=(), *, fetch_binary=None, rc=0):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user._parser import _build_parser

    sent = []

    def answer(base_url, token, path, *, accept, json=None, **_):
        assert path == f"{PATH_FEATURE_TABLE_PREFIX}{PATH_FEATURE_TABLE_ROOT}"
        assert accept == FEATURE_TABLE_MEDIA_TYPE
        sent.append(json)
        return _body(values, features), FEATURE_TABLE_MEDIA_TYPE

    monkeypatch.setattr(_common, "read_token", lambda *a, **k: "qk_tok")
    monkeypatch.setattr(_common, "fetch_binary", fetch_binary or answer)
    out = tmp_path / "table.parquet"
    parser = _build_parser()
    args = parser.parse_args(
        [
            "--base-url",
            "http://cp",
            "feature-table",
            "select",
            "--method",
            "amplicon",
            "--study-idx",
            "3",
            "--format",
            "parquet",
            "--output",
            str(out),
            *extra,
        ]
    )
    assert args.handler(args, parser) == rc
    return out, sent


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
    out, _ = _run(
        monkeypatch,
        tmp_path,
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


def test_parameters_are_sent_typed_and_recorded(monkeypatch, tmp_path):
    """`--parameter KEY=VALUE` reads VALUE as JSON where it parses (150, false) and as a
    string otherwise; the manifest records the settings the table's runs share."""
    out, sent = _run(
        monkeypatch,
        tmp_path,
        _values([("QM1", "QF1", 5.0)]),
        _features([("QF1", "ACGT")]),
        extra=["--parameter", "trim=150", "--parameter", "orient_primer=false"],
    )
    assert sent == [
        {"method": "amplicon", "parameters": {"trim": 150, "orient_primer": False}, "study_idx": 3}
    ]
    manifest = json.loads(out.with_name("table.manifest.json").read_text())
    assert manifest["kind"] == "amplicon"
    assert manifest["parameters"] == _PARAMETERS


def test_a_parameter_without_a_value_is_refused_before_anything_is_read(
    monkeypatch, tmp_path, capsys
):
    def fetch_binary(*_, **__):
        raise AssertionError("nothing should be read for a malformed parameter")

    _run(
        monkeypatch, tmp_path, None, None, ["--parameter", "trim"], fetch_binary=fetch_binary, rc=1
    )
    assert "KEY=VALUE" in capsys.readouterr().err


def test_the_identifier_map_says_what_it_carries(monkeypatch, tmp_path):
    """The map holds each column's QM and biosample name, no internal idx, so its note
    describes those rather than warning of one."""
    out, _ = _run(
        monkeypatch,
        tmp_path,
        _values([("QM1", "QF1", 5.0)]),
        _features([("QF1", "ACGT")]),
    )
    payload = json.loads(out.with_name("table.exported-identifier.json").read_text())
    assert payload["identifiers"] == _SAMPLES
    assert payload["note"] == MAP_NOTE


def test_a_stale_scope_403_asks_for_a_fresh_login(monkeypatch, tmp_path, capsys):
    def fetch_binary(base_url, token, path, **_):
        request = httpx.Request("POST", f"http://cp{path}")
        response = httpx.Response(403, headers={STALE_TOKEN_SCOPE_HEADER: "1"}, request=request)
        raise httpx.HTTPStatusError("forbidden", request=request, response=response)

    _run(monkeypatch, tmp_path, None, None, fetch_binary=fetch_binary, rc=1)
    assert "qiita login" in capsys.readouterr().err


def test_an_occupied_sequences_file_is_refused_before_anything_is_read(
    monkeypatch, tmp_path, capsys
):
    """The sequences file is part of every bundle, so its name is checked with the rest
    before the route is asked for anything (and before the mint names a feature)."""
    (tmp_path / "table.sequences.parquet").write_bytes(b"theirs")

    def fetch_binary(*_, **__):
        raise AssertionError("nothing should be read for a bundle that cannot be written")

    _run(monkeypatch, tmp_path, None, None, fetch_binary=fetch_binary, rc=1)
    assert "table.sequences.parquet" in capsys.readouterr().err
