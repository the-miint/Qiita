"""`qiita mask syndna-read-count` with the HTTP read faked — real miint, real BIOM and
Parquet writers. The route's refusals are `tests/routes/test_syndna_read_count.py`."""

import json

import pyarrow as pa
import pytest
from qiita_common.models import AS_OF_SCOPE, SyndnaReadCountResponse

from qiita_control_plane.cli import user as cli
from qiita_control_plane.cli.user import mask as mx
from qiita_control_plane.miint import connect_with_miint

_H1, _H2 = "synDNA_16SrRNA_seq_1_gc=0.26", "synDNA_16SrRNA_seq_2_gc=0.36"


_DECISIONS = {
    "decision_set": "sha256:" + "1" * 64,
    "newest_event_at": "2026-11-16T10:00:00Z",
    "as_of": True,
    "as_of_scope": AS_OF_SCOPE,
    "pairs_used": 1,
    "pairs_used_set": "sha256:" + "2" * 64,
}


def _response(
    samples, inserts=((11, _H1), (12, _H2)), left_out=(), left_out_accession=None
) -> SyndnaReadCountResponse:
    left_out = list(left_out)
    if left_out_accession is None:
        left_out_accession = {r["prep_sample_idx"]: None for r in left_out}
    return SyndnaReadCountResponse.model_validate(
        {
            "mask_idx": 3,
            "reference_idx": 17,
            "inserts": [{"feature_idx": f, "accession": a} for f, a in inserts],
            "samples": [
                {
                    "prep_sample_idx": ps,
                    "mask_idx": 3,
                    "mask_export_processing_id": "QP31",
                    "biosample_accession": acc,
                    "sequenced_pool_idx": pool,
                    "read_counts": counts,
                }
                for ps, acc, pool, counts in samples
            ],
            "left_out": left_out,
            "left_out_biosample_accession": left_out_accession,
            "decisions": _DECISIONS,
        }
    )


_TWO = _response([(1, "SAMEA1", 40, [5, 0]), (2, "SAMEA2", 41, [1, 9])])


def test_samples_are_named_by_accession_or_prefixed_by_pool():
    assert mx.syndna_sample_names(_TWO, prefix_pool=False) == ["SAMEA1", "SAMEA2"]
    assert mx.syndna_sample_names(_TWO, prefix_pool=True) == ["40_SAMEA1", "41_SAMEA2"]


def test_one_biosample_twice_is_refused_unless_the_pools_tell_them_apart():
    same_pool = _response([(1, "SAMEA1", 40, [1, 0]), (2, "SAMEA1", 40, [0, 1])])
    two_pools = _response([(1, "SAMEA1", 40, [1, 0]), (2, "SAMEA1", 41, [0, 1])])

    with pytest.raises(ValueError, match="--prefix-pool"):
        mx.syndna_sample_names(two_pools, prefix_pool=False)
    assert mx.syndna_sample_names(two_pools, prefix_pool=True) == ["40_SAMEA1", "41_SAMEA1"]
    with pytest.raises(ValueError, match="'40_SAMEA1'"):
        mx.syndna_sample_names(same_pool, prefix_pool=True)


def test_a_sample_without_an_accession_or_pool_is_refused():
    with pytest.raises(ValueError, match="no biosample accession"):
        mx.syndna_sample_names(_response([(1, None, 40, [1, 0])]), prefix_pool=False)
    with pytest.raises(ValueError, match="no sequenced_pool"):
        mx.syndna_sample_names(_response([(1, "SAMEA1", None, [1, 0])]), prefix_pool=True)


def test_inserts_are_named_by_accession_or_species():
    assert mx.syndna_feature_names(_TWO, None) == [_H1, _H2]
    assert mx.syndna_feature_names(_TWO, {11: "s1", 12: "s2"}) == ["s1", "s2"]
    with pytest.raises(ValueError, match="no taxonomy species"):
        mx.syndna_feature_names(_TWO, {11: "s1"})
    with pytest.raises(ValueError, match="share the name"):
        mx.syndna_feature_names(_TWO, {11: "s", 12: "s"})
    with pytest.raises(ValueError, match="no accession"):
        mx.syndna_feature_names(_response([], inserts=((11, _H1), (12, None))), None)


def _cells(path, fmt):
    with connect_with_miint() as con:
        source = f"read_biom('{path}')" if fmt == "biom" else f"read_parquet('{path}')"
        return sorted(con.execute(f"SELECT sample_id, feature_id, value FROM {source}").fetchall())


@pytest.mark.parametrize("fmt", ["biom", "parquet"])
def test_both_formats_carry_the_same_values(tmp_path, fmt):
    out = tmp_path / f"t.{fmt}"
    with connect_with_miint() as con:
        mx.write_syndna_table(
            con,
            _TWO,
            sample_names=["SAMEA1", "SAMEA2"],
            feature_names=[_H1, _H2],
            output=out,
            fmt=fmt,
        )
    nonzero = [
        ("SAMEA1", _H1, 5.0),
        ("SAMEA2", _H1, 1.0),
        ("SAMEA2", _H2, 9.0),
    ]
    # BIOM is sparse; Parquet keeps the zero cell.
    expected = nonzero if fmt == "biom" else sorted([*nonzero, ("SAMEA1", _H2, 0.0)])
    assert _cells(out, fmt) == sorted(expected)
    assert not out.with_name(out.name + ".partial").exists()


def test_an_existing_output_is_refused(tmp_path):
    out = tmp_path / "t.biom"
    out.write_text("mine")
    with connect_with_miint() as con, pytest.raises(ValueError, match="already exists"):
        mx.write_syndna_table(
            con, _TWO, sample_names=["a", "b"], feature_names=["x", "y"], output=out, fmt="biom"
        )
    assert out.read_text() == "mine"


def _run(monkeypatch, tmp_path, *extra, response=_TWO):
    monkeypatch.setenv("QIITA_TOKEN", "t")
    monkeypatch.setattr(mx, "_get_syndna_read_count", lambda *a, **k: response)
    return cli.main(
        ["--base-url", "https://cp", "mask", "syndna-read-count", "--mask-idx", "3", *extra]
    )


def _fake_species(monkeypatch, names=("s1", "s2")):
    taxonomy = pa.table({"feature_idx": [11, 12], "species": list(names)})

    def fake_species(base_url, token, con, data_plane_url, reference_idx):
        assert reference_idx == 17
        assert data_plane_url == "grpc://dp:50051"
        con.register("t", taxonomy)
        return dict(con.execute("SELECT feature_idx, species FROM t").fetchall())

    monkeypatch.setattr(mx, "_fetch_species", fake_species)


_DP = ("--data-plane-url", "grpc://dp:50051")


def test_main_writes_a_biom_named_by_species_by_default(monkeypatch, tmp_path, capsys):
    _fake_species(monkeypatch)
    out = tmp_path / "syndna.biom"
    assert _run(monkeypatch, tmp_path, "--study-idx", "1", "--output", str(out), *_DP) == 0
    cells = _cells(out, "biom")
    assert {r[0] for r in cells} == {"SAMEA1", "SAMEA2"}
    assert {r[1] for r in cells} == {"s1", "s2"}
    assert "2 sample(s) x 2 insert(s)" in capsys.readouterr().out


def test_main_lists_the_samples_the_table_leaves_out(monkeypatch, tmp_path, capsys):
    """The route leaves out a sample that is not processed in the mask's group; the
    table is still written, and each left-out sample is one stderr line with its
    reason."""
    _fake_species(monkeypatch)
    out = tmp_path / "syndna.biom"
    partial = _response(
        [(1, "SAMEA1", 40, [5, 0])],
        left_out=[
            {
                "prep_sample_idx": 2,
                "state": "incomplete",
                "definition_kind": "mask",
                "definition_idx": 9,
                "reason": "in_progress",
                "work_ticket_idx": 77,
            },
            {
                "prep_sample_idx": 4,
                "state": "not_processed",
                "definition_kind": "mask",
                "reason": "no_run",
            },
            {
                "prep_sample_idx": 6,
                "state": "not_processed",
                "definition_kind": "mask",
                "definition_idx": 9,
                "reason": "no_syndna_counts",
            },
        ],
        left_out_accession={2: "SAMEA2", 4: None, 6: "SAMEA6"},
    )
    rc = _run(
        monkeypatch, tmp_path, "--study-idx", "1", "--output", str(out), *_DP, response=partial
    )
    assert rc == 0
    assert {r[0] for r in _cells(out, "biom")} == {"SAMEA1"}
    err = capsys.readouterr().err
    assert "left out 3 prep_sample(s)" in err
    assert "prep_sample 2: incomplete (in_progress), mask 9, work_ticket 77" in err
    assert "prep_sample 4: not_processed (no_run)" in err
    assert "prep_sample 6: not_processed (no_syndna_counts), mask 9" in err
    sidecar = json.loads(mx.syndna_sidecar_path(out).read_text())
    assert sidecar["left_out"] == [
        {"biosample_accession": "SAMEA2", "state": "incomplete", "reason": "in_progress"},
        {"biosample_accession": "SAMEA6", "state": "not_processed", "reason": "no_syndna_counts"},
        {"biosample_accession": None, "state": "not_processed", "reason": "no_run"},
    ]
    assert sidecar["samples"] == {
        "SAMEA1": {"biosample_accession": "SAMEA1", "mask_export_processing_id": "QP31"}
    }


def test_accession_names_need_no_data_plane(monkeypatch, tmp_path):
    out = tmp_path / "syndna.parquet"
    rc = _run(
        monkeypatch,
        tmp_path,
        "--prep-sample-idx",
        "1",
        "--output",
        str(out),
        "--format",
        "parquet",
        "--feature-names",
        "accession",
    )
    assert rc == 0
    assert {r[1] for r in _cells(out, "parquet")} == {_H1, _H2}


def test_a_response_the_cli_does_not_recognise_is_one_error_line(monkeypatch, tmp_path, capsys):
    """A syndna-read-count body that fails SyndnaReadCountResponse (e.g. from a
    control plane at a different version) is one stderr line, exit 1, no file."""
    monkeypatch.setenv("QIITA_TOKEN", "t")
    monkeypatch.setattr(mx._common, "call", lambda *a, **k: {"mask_idx": 3})
    out = tmp_path / "syndna.biom"
    rc = cli.main(
        [
            "--base-url",
            "https://cp",
            "mask",
            "syndna-read-count",
            "--mask-idx",
            "3",
            "--study-idx",
            "1",
            "--output",
            str(out),
            "--feature-names",
            "accession",
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert len(err.strip().splitlines()) == 1, err
    assert "invalid SyndnaReadCountResponse" in err
    assert not out.exists()


def test_main_refuses_a_collision_and_writes_nothing(monkeypatch, tmp_path, capsys):
    _fake_species(monkeypatch)
    out = tmp_path / "syndna.biom"
    collide = _response([(1, "SAMEA1", 40, [1, 0]), (2, "SAMEA1", 41, [0, 1])])
    assert _run(
        monkeypatch, tmp_path, "--study-idx", "1", "--output", str(out), *_DP, response=collide
    )
    assert "--prefix-pool" in capsys.readouterr().err
    assert not out.exists()


def test_one_species_shared_by_every_insert_is_refused(monkeypatch, tmp_path, capsys):
    """A plasmid reference whose taxonomy names every member `synthetic construct`
    must not merge them into one row."""
    _fake_species(monkeypatch, names=("synthetic construct", "synthetic construct"))
    out = tmp_path / "syndna.biom"
    assert _run(monkeypatch, tmp_path, "--study-idx", "1", "--output", str(out), *_DP)
    assert "share the name" in capsys.readouterr().err
    assert not out.exists()


def test_species_without_a_data_plane_url_and_no_selector_are_usage_errors(monkeypatch, tmp_path):
    out = str(tmp_path / "x.biom")
    with pytest.raises(SystemExit):
        _run(monkeypatch, tmp_path, "--study-idx", "1", "--output", out)
    with pytest.raises(SystemExit):
        _run(monkeypatch, tmp_path, "--output", out, *_DP)


def test_include_deprecated_reaches_the_route_and_the_read_is_marked(monkeypatch, tmp_path, capsys):
    """--include-deprecated is sent as `include_deprecated=true`; a response marked
    deprecated is said so on stderr."""
    seen: list[dict] = []
    body = _TWO.model_dump(mode="json") | {"deprecated": True, "deprecation_reason": "bad host"}

    def fake_call(method, base_url, token, path, **kw):
        seen.append(kw.get("params"))
        return body

    monkeypatch.setenv("QIITA_TOKEN", "t")
    monkeypatch.setattr(mx._common, "call", fake_call)
    out = tmp_path / "syndna.parquet"
    argv = ["--base-url", "https://cp", "mask", "syndna-read-count", "--mask-idx", "3"]
    argv += ["--study-idx", "1", "--output", str(out), "--format", "parquet"]
    argv += ["--feature-names", "accession", "--include-deprecated"]
    assert cli.main(argv) == 0
    assert seen == [{"study_idx": "1", "include_deprecated": "true"}]
    err = capsys.readouterr().err
    assert "mask 3 is deprecated (bad host)" in err
    assert "--include-deprecated" in err


def test_as_of_event_reaches_the_route_and_the_sidecar_records_the_decisions(monkeypatch, tmp_path):
    seen: list[dict] = []

    def fake_call(method, base_url, token, path, **kw):
        seen.append(kw.get("params"))
        return _TWO.model_dump(mode="json")

    monkeypatch.setenv("QIITA_TOKEN", "t")
    monkeypatch.setattr(mx._common, "call", fake_call)
    out = tmp_path / "syndna.parquet"
    argv = ["--base-url", "https://cp", "mask", "syndna-read-count", "--mask-idx", "3"]
    argv += ["--study-idx", "1", "--output", str(out), "--format", "parquet"]
    argv += ["--feature-names", "accession", "--as-of-event", _DECISIONS["decision_set"]]
    assert cli.main(argv) == 0
    assert seen == [{"study_idx": "1", "as_of_event": _DECISIONS["decision_set"]}]
    sidecar = json.loads(mx.syndna_sidecar_path(out).read_text())
    assert sidecar["decisions"]["decision_set"] == _DECISIONS["decision_set"]
    assert sidecar["decisions"]["as_of_scope"] == AS_OF_SCOPE


def _idx_free(value) -> None:
    """No key or string anywhere in `value` names one of our `_idx` identifiers."""
    if isinstance(value, dict):
        for key, inner in value.items():
            assert "_idx" not in key, key
            _idx_free(inner)
    elif isinstance(value, list):
        for inner in value:
            _idx_free(inner)
    elif isinstance(value, str):
        assert "_idx" not in value, value


@pytest.mark.parametrize("fmt", ["biom", "parquet"])
def test_the_table_and_its_sidecar_cite_the_mask_handle_and_carry_no_idx(tmp_path, fmt):
    """Each sample's counts cite the mask they came from by its public handle (the
    sidecar for both formats; a column too in Parquet), the sidecar records the
    deprecated read and the left-out prep_samples, and no output names an `_idx`."""
    out = tmp_path / f"t.{fmt}"
    body = _response(
        [(1, "SAMEA1", 40, [5, 0]), (2, "SAMEA2", 41, [1, 9])],
        left_out=[
            {
                "prep_sample_idx": 5,
                "state": "not_processed",
                "definition_kind": "mask",
                "definition_idx": 3,
                "reason": "withdrawn",
            }
        ],
        left_out_accession={5: "SAMEA5"},
    ).model_dump(mode="json")
    body["samples"][1]["mask_idx"] = 4
    body["samples"][1]["mask_export_processing_id"] = "QP40"
    body |= {"deprecated": True, "deprecation_reason": "host reference was wrong"}
    response = SyndnaReadCountResponse.model_validate(body)
    with connect_with_miint() as con:
        mx.write_syndna_table(
            con,
            response,
            sample_names=["SAMEA1", "SAMEA2"],
            feature_names=[_H1, _H2],
            output=out,
            fmt=fmt,
        )
        if fmt == "parquet":
            columns = [
                r[0]
                for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{out}')").fetchall()
            ]
            handles = dict(
                con.execute(
                    f"SELECT DISTINCT sample_id, mask_export_processing_id"
                    f" FROM read_parquet('{out}')"
                ).fetchall()
            )
            cells = con.execute(f"SELECT * FROM read_parquet('{out}')").fetchall()
        else:
            columns = [
                r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_biom('{out}')").fetchall()
            ]
            cells = con.execute(f"SELECT * FROM read_biom('{out}')").fetchall()
    sidecar = json.loads(mx.syndna_sidecar_path(out).read_text())

    assert sidecar == {
        "schema_version": 1,
        "samples": {
            "SAMEA1": {"biosample_accession": "SAMEA1", "mask_export_processing_id": "QP31"},
            "SAMEA2": {"biosample_accession": "SAMEA2", "mask_export_processing_id": "QP40"},
        },
        "left_out": [
            {"biosample_accession": "SAMEA5", "state": "not_processed", "reason": "withdrawn"}
        ],
        "deprecated": True,
        "deprecation_reason": "host reference was wrong",
        "decisions": {**_DECISIONS, "newest_event_at": "2026-11-16T10:00:00Z"},
    }
    if fmt == "parquet":
        assert handles == {"SAMEA1": "QP31", "SAMEA2": "QP40"}
    _idx_free(sidecar)
    _idx_free(columns)
    _idx_free([str(v) for row in cells for v in row])
    assert not any(p.name.endswith(".partial") for p in tmp_path.iterdir())


def test_an_existing_sidecar_is_refused_and_nothing_is_written(tmp_path):
    out = tmp_path / "t.biom"
    mx.syndna_sidecar_path(out).write_text("mine")
    with connect_with_miint() as con, pytest.raises(ValueError, match="already exists"):
        mx.write_syndna_table(
            con, _TWO, sample_names=["a", "b"], feature_names=["x", "y"], output=out, fmt="biom"
        )
    assert not out.exists()
    assert mx.syndna_sidecar_path(out).read_text() == "mine"
