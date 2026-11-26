"""`qiita feature-table select`: the bundle written from the `/feature-table` answer —
an amplicon table's values, columns, and each feature's sequence and md5."""

import hashlib
import json

import duckdb
import httpx
import pyarrow as pa
import pytest
from qiita_common.api_paths import PATH_FEATURE_TABLE_PREFIX, PATH_FEATURE_TABLE_ROOT
from qiita_common.auth_constants import STALE_TOKEN_SCOPE_HEADER
from qiita_common.models.feature_table import FeatureTableSample
from qiita_common.models.reference import ReferenceHandle
from qiita_common.taxonomy import RANK_COLUMNS

from qiita_control_plane.cli.user.stored_feature_table import MAP_NOTE
from qiita_control_plane.stored_feature_table import (
    FEATURE_TABLE_MEDIA_TYPE,
    FEATURES_SCHEMA,
    SOURCES,
    TAXONOMY_SCHEMA,
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


def _body(values, features, taxonomy=None):
    cohort = Cohort(
        source=SOURCES["amplicon"],
        runs=[7],
        parameters=_PARAMETERS,
        samples=[
            ProcessedSample(processing_idx=7, prep_sample_idx=i, column=FeatureTableSample(**s))
            for i, s in enumerate(_SAMPLES, start=1)
        ],
    )
    return pack_feature_table(cohort, values, features, taxonomy=taxonomy)


def _run(
    monkeypatch,
    tmp_path,
    values,
    features,
    extra=(),
    *,
    cohort=None,
    taxonomy=None,
    fetch_binary=None,
    rc=0,
):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user._parser import _build_parser

    sent = []

    def answer(base_url, token, path, *, accept, json=None, **_):
        assert path == f"{PATH_FEATURE_TABLE_PREFIX}{PATH_FEATURE_TABLE_ROOT}"
        assert accept == FEATURE_TABLE_MEDIA_TYPE
        sent.append(json)
        return _body(values, features, taxonomy), FEATURE_TABLE_MEDIA_TYPE

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
            *(cohort or ["--study", "QS3"]),
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
        {
            "method": "amplicon",
            "parameters": {"trim": 150, "orient_primer": False},
            "study": {"value": "QS3"},
        }
    ]
    manifest = json.loads(out.with_name("table.manifest.json").read_text())
    assert manifest["kind"] == "amplicon"
    assert manifest["parameters"] == _PARAMETERS


def test_a_key_of_the_other_cohort_form_is_refused_before_anything_is_read(
    monkeypatch, tmp_path, capsys
):
    def fetch_binary(*_, **__):
        raise AssertionError("nothing should be read for a key the study cannot take")

    _run(
        monkeypatch,
        tmp_path,
        None,
        None,
        cohort=["--study", "QS3", "--key", "matrix_tube_id"],
        fetch_binary=fetch_binary,
        rc=1,
    )
    assert "study.key" in capsys.readouterr().err


def test_biosamples_are_named_by_the_key_the_lookups_take(monkeypatch, tmp_path):
    """`--key` names the identifier kind, as `qiita biosample bulk-link` does, with a
    biosample's idx spelled biosample_idx."""
    sent = []
    for key, sent_key in (("biosample_accession", "biosample_accession"), ("biosample_idx", "idx")):
        (tmp_path / key).mkdir()
        _, request = _run(
            monkeypatch,
            tmp_path / key,
            _values([("QM1", "QF1", 5.0)]),
            _features([("QF1", "ACGT")]),
            cohort=["--biosample", "1", "2", "--key", key],
        )
        sent += request
        assert request[0]["biosample"] == {"key": sent_key, "values": ["1", "2"]}
    assert len(sent) == 2


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


def test_a_named_reference_writes_each_features_lineage_beside_the_table(monkeypatch, tmp_path):
    """`--taxonomy NAME VERSION` asks the one request to classify the features, writes
    the lineage as `<stem>.taxonomy.parquet`, and records the reference in the manifest."""
    reference = ReferenceHandle(name="silva", version="2024.09")
    lineage = pa.table(
        {
            "feature": ["QF1"],
            **{r: pa.array([None], pa.string()) for r in TAXONOMY_SCHEMA.names[1:]},
        }
    ).cast(TAXONOMY_SCHEMA)
    out, sent = _run(
        monkeypatch,
        tmp_path,
        _values([("QM1", "QF1", 5.0)]),
        _features([("QF1", "ACGT")]),
        ["--taxonomy", "silva", "2024.09"],
        taxonomy=(reference, lineage),
    )
    assert sent[0]["taxonomy"] == {"name": "silva", "version": "2024.09"}
    with duckdb.connect() as con:
        rows = con.execute(
            "SELECT feature_id, domain FROM read_parquet(?)",
            [str(out.with_name("table.taxonomy.parquet"))],
        ).fetchall()
    assert rows == [("QF1", None)]
    # The same columns `feature-table build --taxonomy` writes.
    with duckdb.connect() as con:
        described = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(out.with_name("table.taxonomy.parquet"))],
        ).fetchall()
    assert [r[0] for r in described] == ["feature_id", *RANK_COLUMNS]
    manifest = json.loads(out.with_name("table.manifest.json").read_text())
    assert manifest["taxonomy"] == {"name": "silva", "version": "2024.09"}
    assert manifest["classified_only"] is None


def test_an_occupied_taxonomy_file_is_refused_before_anything_is_read(
    monkeypatch, tmp_path, capsys
):
    (tmp_path / "table.taxonomy.parquet").write_bytes(b"theirs")

    def fetch_binary(*_, **__):
        raise AssertionError("nothing should be read for a bundle that cannot be written")

    _run(
        monkeypatch,
        tmp_path,
        None,
        None,
        ["--taxonomy", "silva", "2024.09"],
        fetch_binary=fetch_binary,
        rc=1,
    )
    assert "table.taxonomy.parquet" in capsys.readouterr().err


def _lineage(rows):
    """A taxonomy part: (feature, domain), every other rank NULL."""
    return pa.table(
        {
            "feature": [r[0] for r in rows],
            "domain": pa.array([r[1] and f"d__{r[1]}" for r in rows], pa.string()),
            **{r: pa.array([None] * len(rows), pa.string()) for r in TAXONOMY_SCHEMA.names[2:]},
        }
    ).cast(TAXONOMY_SCHEMA)


def _read(path, columns):
    with duckdb.connect() as con:
        return con.execute(
            f"SELECT {columns} FROM read_parquet(?) ORDER BY 1", [str(path)]
        ).fetchall()


_SILVA = ReferenceHandle(name="silva", version="2024.09")


def test_classified_only_keeps_the_features_the_reference_classifies(monkeypatch, tmp_path):
    """Narrowed on this machine, over the answer: the table, its sequences and its lineage
    lose the unclassified feature; every column stays, and the manifest says how many
    features went."""
    out, _ = _run(
        monkeypatch,
        tmp_path,
        _values([("QM1", "QF1", 5.0), ("QM1", "QF2", 1.0), ("QM2", "QF2", 3.0)]),
        _features([("QF1", "ACGT"), ("QF2", "TTTT")]),
        ["--taxonomy", "silva", "2024.09", "--classified-only"],
        taxonomy=(_SILVA, _lineage([("QF1", "Bacteria"), ("QF2", None)])),
    )
    assert _read(out, "feature_id, sample_id, value") == [("QF1", "QM1", 5.0)]
    assert _read(out.with_name("table.sequences.parquet"), "feature_id") == [("QF1",)]
    assert _read(out.with_name("table.taxonomy.parquet"), "feature_id, domain") == [
        ("QF1", "d__Bacteria")
    ]
    manifest = json.loads(out.with_name("table.manifest.json").read_text())
    assert manifest["classified_only"] == {"dropped_features": 1}
    identifiers = json.loads(out.with_name("table.exported-identifier.json").read_text())
    assert identifiers["identifiers"] == _SAMPLES


def test_classified_only_without_a_reference_is_refused_before_anything_is_read(
    monkeypatch, tmp_path, capsys
):
    def fetch_binary(*_, **__):
        raise AssertionError("nothing should be read for a request that cannot be filtered")

    _run(monkeypatch, tmp_path, None, None, ["--classified-only"], fetch_binary=fetch_binary, rc=1)
    assert "--taxonomy" in capsys.readouterr().err


@pytest.mark.parametrize(
    "rows",
    [[], [("QF1", "Bacteria"), ("QF1", "Archaea")]],
    ids=["short", "repeated"],
)
def test_a_lineage_not_one_row_per_feature_is_refused(monkeypatch, tmp_path, capsys, rows):
    """A short lineage would read as unclassified features, and a repeated one as two
    lineages for one feature; either is refused and nothing is written."""
    out, _ = _run(
        monkeypatch,
        tmp_path,
        _values([("QM1", "QF1", 5.0)]),
        _features([("QF1", "ACGT")]),
        ["--taxonomy", "silva", "2024.09"],
        taxonomy=(_SILVA, _lineage(rows)),
        rc=1,
    )
    assert "error:" in capsys.readouterr().err
    assert not out.exists()
