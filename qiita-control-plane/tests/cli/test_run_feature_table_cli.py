"""`qiita feature-table runs`: the bundle written from the `/feature-table` routes'
answers — an amplicon table's values, columns, and each feature's sequence and md5."""

import hashlib
import json

import duckdb
import httpx
import pyarrow as pa
from qiita_common.api_paths import (
    PATH_FEATURE_TABLE_FEATURE,
    PATH_FEATURE_TABLE_PREFIX,
    PATH_FEATURE_TABLE_VALUE,
)
from qiita_common.auth_constants import STALE_TOKEN_SCOPE_HEADER

from qiita_control_plane.run_feature_table import (
    ARROW_STREAM_MEDIA_TYPE,
    FEATURES_SCHEMA,
    VALUES_SCHEMA,
    arrow_stream_bytes,
)

_SAMPLES = [
    {"sample": "QM1", "export_id": "QM1", "biosample": "QB1"},
    {"sample": "QM2", "export_id": "QM2", "biosample": "QB2"},
]


def _answers(method, values, features):
    def call(method_, base_url, token, path, json=None, params=None):
        assert path.endswith("/sample")
        return {"method": method, "value_kind": "count", "samples": _SAMPLES}

    def fetch_binary(base_url, token, path, *, accept, json=None, **_):
        body = {
            f"{PATH_FEATURE_TABLE_PREFIX}{PATH_FEATURE_TABLE_VALUE}": values,
            f"{PATH_FEATURE_TABLE_PREFIX}{PATH_FEATURE_TABLE_FEATURE}": features,
        }[path]
        return arrow_stream_bytes(body), ARROW_STREAM_MEDIA_TYPE

    return call, fetch_binary


def _run(monkeypatch, tmp_path, method, values, features, extra=(), *, call=None, rc=0):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user._parser import _build_parser

    answer, fetch_binary = _answers(method, values, features)
    call = call or answer
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
    assert args.handler(args, parser) == rc
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


def test_the_identifier_map_says_what_it_carries(monkeypatch, tmp_path, capsys):
    """The map holds each column's QM and biosample name, no internal idx, so its note
    must not warn of one."""
    out = _run(
        monkeypatch,
        tmp_path,
        "amplicon",
        _values([("QM1", "QF1", 5.0)]),
        _features([("QF1", "ACGT")]),
    )
    payload = json.loads(out.with_name("table.exported-identifier.json").read_text())
    assert payload["identifiers"] == _SAMPLES
    assert "_idx" not in payload["note"]
    assert "_idx" not in capsys.readouterr().out


def test_a_stale_scope_403_asks_for_a_fresh_login(monkeypatch, tmp_path, capsys):
    def call(method_, base_url, token, path, json=None, params=None):
        request = httpx.Request("POST", f"http://cp{path}")
        response = httpx.Response(403, headers={STALE_TOKEN_SCOPE_HEADER: "1"}, request=request)
        raise httpx.HTTPStatusError("forbidden", request=request, response=response)

    _run(monkeypatch, tmp_path, "amplicon", None, None, call=call, rc=1)
    assert "qiita login" in capsys.readouterr().err
