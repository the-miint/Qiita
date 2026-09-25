"""`qiita mask syndna-read-count` with the HTTP read faked — real miint, real BIOM and
Parquet writers. The route's refusals are `tests/routes/test_syndna_read_count.py`."""

import pyarrow as pa
import pytest
from qiita_common.models import SyndnaReadCountResponse

from qiita_control_plane.cli import user as cli
from qiita_control_plane.cli.user import mask as mx
from qiita_control_plane.miint import connect_with_miint

_H1, _H2 = "synDNA_16SrRNA_seq_1_gc=0.26", "synDNA_16SrRNA_seq_2_gc=0.36"


def _response(samples, inserts=((11, _H1), (12, _H2))) -> SyndnaReadCountResponse:
    return SyndnaReadCountResponse.model_validate(
        {
            "mask_idx": 3,
            "reference_idx": 17,
            "inserts": [{"feature_idx": f, "accession": a} for f, a in inserts],
            "samples": [
                {
                    "prep_sample_idx": ps,
                    "biosample_accession": acc,
                    "sequenced_pool_idx": pool,
                    "read_counts": counts,
                }
                for ps, acc, pool, counts in samples
            ],
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


def test_main_writes_a_biom_by_default(monkeypatch, tmp_path, capsys):
    out = tmp_path / "syndna.biom"
    assert _run(monkeypatch, tmp_path, "--study-idx", "1", "--output", str(out)) == 0
    assert {r[0] for r in _cells(out, "biom")} == {"SAMEA1", "SAMEA2"}
    assert "2 sample(s) x 2 insert(s)" in capsys.readouterr().out


def test_main_refuses_a_collision_and_writes_nothing(monkeypatch, tmp_path, capsys):
    out = tmp_path / "syndna.biom"
    collide = _response([(1, "SAMEA1", 40, [1, 0]), (2, "SAMEA1", 41, [0, 1])])
    assert _run(monkeypatch, tmp_path, "--study-idx", "1", "--output", str(out), response=collide)
    assert "--prefix-pool" in capsys.readouterr().err
    assert not out.exists()


def test_species_names_are_read_from_the_reference_taxonomy(monkeypatch, tmp_path):
    taxonomy = pa.table({"feature_idx": [11, 12], "species": ["s1", "s2"]})

    def fake_species(base_url, token, con, data_plane_url, reference_idx):
        assert reference_idx == 17
        con.register("t", taxonomy)
        return dict(con.execute("SELECT feature_idx, species FROM t").fetchall())

    monkeypatch.setattr(mx, "_fetch_species", fake_species)
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
        "species",
        "--data-plane-url",
        "grpc://dp:50051",
    )
    assert rc == 0
    assert {r[1] for r in _cells(out, "parquet")} == {"s1", "s2"}


def test_species_without_a_data_plane_url_and_no_selector_are_usage_errors(monkeypatch, tmp_path):
    out = str(tmp_path / "x.biom")
    with pytest.raises(SystemExit):
        _run(
            monkeypatch, tmp_path, "--study-idx", "1", "--output", out, "--feature-names", "species"
        )
    with pytest.raises(SystemExit):
        _run(monkeypatch, tmp_path, "--output", out)
