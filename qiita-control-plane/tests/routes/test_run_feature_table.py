"""Route tests for POST /feature-table/{sample,value,feature}.

One cohort request — amplicon runs, and a study or a named
prep_sample list — answered as the table's columns (JSON) and its values and
features (Arrow IPC).
A column is a processed sample (a live QM of one of the runs); it is named by
its biosample's deposited accession when no other column shares it, else by its
QM. Access is all-or-nothing over the cohort, checked before the data plane is
read. The data-plane read is stubbed; its scope is pinned by the data plane's
own tests.
"""

import hashlib
import uuid

import pyarrow as pa
import pyarrow.flight as flight
import pytest
from qiita_common.api_paths import (
    URL_FEATURE_TABLE_FEATURE,
    URL_FEATURE_TABLE_SAMPLE,
    URL_FEATURE_TABLE_VALUE,
)
from qiita_common.models.run_feature_table import MAX_FEATURE_TABLE_RUNS

from qiita_control_plane.repositories.processing import mint_processing

pytestmark = pytest.mark.db

ASV = "ACGTACGT"
PARAMS = {"workflow": "amplicon", "version": "1.1.0", "trim": 150, "orient_primer": False}


async def _mint_run(db, owner_idx, prep_sample_idxs, **params):
    async with db.acquire() as conn:
        run = await mint_processing(
            conn,
            workflow="amplicon",
            version="1.1.0",
            params={**PARAMS, **params, "probe": str(uuid.uuid4())},
        )
    await db.execute(
        "INSERT INTO qiita.exported_identifier (processing_idx, prep_sample_idx, created_by_idx)"
        " SELECT $1, unnest($2::bigint[]), $3",
        run["processing_idx"],
        sorted(prep_sample_idxs),
        owner_idx,
    )
    return run["processing_idx"]


@pytest.fixture
async def amplicon_run(role_keyed_clients, pool_alignment_seed, monkeypatch):
    """An amplicon run that counted ps_a + ps_c (study_1) and ps_b (study_2), and
    the data-plane fetch stubbed to one ASV seen in every requested sample."""
    seed, db = pool_alignment_seed, role_keyed_clients["pool"]
    cohort = [seed["ps_a"], seed["ps_b"], seed["ps_c"]]
    processing_idx = await _mint_run(db, seed["owner_idx"], cohort)
    runs = [processing_idx]
    fetched: list[tuple[list[int], list[int]]] = []

    def _fetch(*, data_plane_url, signing_key, cohort, values=True):
        processing_idxs, prep_sample_idxs = cohort.runs, cohort.prep_sample_idxs
        fetched.append((list(processing_idxs), list(prep_sample_idxs)))
        rows = [(r, p) for r in processing_idxs for p in prep_sample_idxs]
        observed = pa.table(
            {
                "prep_sample_idx": pa.array([p for _, p in rows], pa.int64()),
                "processing_idx": pa.array([r for r, _ in rows], pa.int64()),
                "feature_idx": pa.array([77] * len(rows), pa.int64()),
                "count": pa.array(range(1, len(rows) + 1), pa.int64()),
            }
        )
        sequences = pa.table(
            {
                "feature_idx": pa.array([77], pa.int64()),
                "sequence_hash": pa.array([hashlib.md5(ASV.encode()).hexdigest()], pa.string()),
                "sequence_length_bp": pa.array([len(ASV)], pa.int64()),
                "sequence": pa.array([ASV], pa.string()),
            }
        )
        return (observed if values else None), sequences

    monkeypatch.setattr("qiita_control_plane.routes.run_feature_table.fetch_runs", _fetch)
    yield {"processing_idx": processing_idx, "runs": runs, "fetched": fetched, **seed}
    await db.execute(
        "DELETE FROM qiita.exported_identifier WHERE processing_idx = ANY($1::bigint[])", runs
    )
    await db.execute("DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])", runs)


async def _qm(db, processing_idx, prep_sample_idx):
    return await db.fetchval(
        "SELECT export_id FROM qiita.exported_identifier"
        " WHERE processing_idx = $1 AND prep_sample_idx = $2",
        processing_idx,
        prep_sample_idx,
    )


async def _qb(db, prep_sample_idx):
    return await db.fetchval(
        "SELECT ee.export_entity_id FROM qiita.prep_sample ps"
        " JOIN qiita.exported_entity ee ON ee.biosample_idx = ps.biosample_idx"
        " WHERE ps.idx = $1",
        prep_sample_idx,
    )


async def _set_accession(db, prep_sample_idx, accession):
    await db.execute(
        "UPDATE qiita.biosample SET ena_sample_accession = $1"
        " WHERE idx = (SELECT biosample_idx FROM qiita.prep_sample WHERE idx = $2)",
        accession,
        prep_sample_idx,
    )


def _arrow(resp) -> pa.Table:
    assert resp.headers["content-type"] == "application/vnd.apache.arrow.stream"
    return pa.ipc.open_stream(resp.content).read_all()


def _body(runs, *, study_idx=None, prep_sample_idx=None):
    body = {"processing_idx": runs}
    if study_idx is not None:
        body["study_idx"] = study_idx
    if prep_sample_idx is not None:
        body["prep_sample_idx"] = prep_sample_idx
    return body


async def test_counts_are_coo_named_by_qm_and_md5(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    cohort = sorted([run["ps_a"], run["ps_b"], run["ps_c"]])
    resp = await role_keyed_clients["wet"].post(
        URL_FEATURE_TABLE_VALUE,
        json=_body([run["processing_idx"]], prep_sample_idx=cohort),
    )
    assert resp.status_code == 200, resp.text
    table = _arrow(resp)
    assert table.column_names == ["sample", "feature", "value"]
    qms = [await _qm(db, run["processing_idx"], p) for p in cohort]
    feature = hashlib.md5(ASV.encode()).hexdigest()
    assert sorted(zip(*table.to_pydict().values(), strict=True)) == sorted(
        (qm, feature, float(i + 1)) for i, qm in enumerate(qms)
    )
    assert run["fetched"] == [([run["processing_idx"]], cohort)]


async def test_features_carry_each_md5s_sequence(role_keyed_clients, amplicon_run):
    run = amplicon_run
    resp = await role_keyed_clients["wet"].post(
        URL_FEATURE_TABLE_FEATURE,
        json=_body([run["processing_idx"]], prep_sample_idx=[run["ps_a"]]),
    )
    assert resp.status_code == 200, resp.text
    assert _arrow(resp).to_pylist() == [
        {
            "feature": hashlib.md5(ASV.encode()).hexdigest(),
            "sequence": ASV,
            "length": len(ASV),
        }
    ]


async def test_samples_join_each_column_to_its_biosample(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    resp = await role_keyed_clients["wet"].post(
        URL_FEATURE_TABLE_SAMPLE,
        json=_body([run["processing_idx"]], prep_sample_idx=[run["ps_a"]]),
    )
    assert resp.status_code == 200, resp.text
    qm = await _qm(db, run["processing_idx"], run["ps_a"])
    assert resp.json()["samples"] == [
        {
            "sample": qm,
            "export_id": qm,
            "biosample": await _qb(db, run["ps_a"]),
            "processing_idx": run["processing_idx"],
        }
    ]


async def test_a_deposited_accession_names_its_column(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    accession = f"SAMEA{uuid.uuid4().int % 10**9}"
    await _set_accession(db, run["ps_a"], accession)
    try:
        resp = await role_keyed_clients["wet"].post(
            URL_FEATURE_TABLE_SAMPLE,
            json=_body([run["processing_idx"]], prep_sample_idx=[run["ps_a"]]),
        )
        assert resp.status_code == 200, resp.text
        (column,) = resp.json()["samples"]
        assert column["sample"] == accession
        assert column["biosample"] == accession
        assert column["export_id"].startswith("QM")
    finally:
        await _set_accession(db, run["ps_a"], None)


async def test_one_biosample_in_two_runs_is_two_columns_by_qm(role_keyed_clients, amplicon_run):
    """Merging two comparable runs that both counted ps_a: its accession would
    name two columns, so each falls back to its own QM."""
    run, db = amplicon_run, role_keyed_clients["pool"]
    second = await _mint_run(db, run["owner_idx"], [run["ps_a"]])
    run["runs"].append(second)
    await _set_accession(db, run["ps_a"], f"SAMEA{uuid.uuid4().int % 10**9}")
    try:
        resp = await role_keyed_clients["wet"].post(
            URL_FEATURE_TABLE_VALUE,
            json=_body([run["processing_idx"], second], prep_sample_idx=[run["ps_a"]]),
        )
        assert resp.status_code == 200, resp.text
        assert set(_arrow(resp).column("sample").to_pylist()) == {
            await _qm(db, run["processing_idx"], run["ps_a"]),
            await _qm(db, second, run["ps_a"]),
        }
        assert run["fetched"] == [(sorted([run["processing_idx"], second]), [run["ps_a"]])]
    finally:
        await _set_accession(db, run["ps_a"], None)


async def test_refuses_runs_whose_asvs_are_not_comparable(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    other = await _mint_run(db, run["owner_idx"], [run["ps_a"]], trim=100)
    run["runs"].append(other)
    resp = await role_keyed_clients["wet"].post(
        URL_FEATURE_TABLE_VALUE,
        json=_body([run["processing_idx"], other], prep_sample_idx=[run["ps_a"]]),
    )
    assert resp.status_code == 409, resp.text
    assert "trim" in resp.json()["detail"]
    assert run["fetched"] == []


async def test_404s_a_run_that_is_not_a_feature_table_run(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    async with db.acquire() as conn:
        other = await mint_processing(
            conn,
            workflow="long-read-assembly",
            version="1.0.0",
            params={"workflow": "long-read-assembly", "probe": str(uuid.uuid4())},
        )
    run["runs"].append(other["processing_idx"])
    # A run of another method names the method it is; a missing idx says so instead.
    for runs, fragment in (
        ([other["processing_idx"]], "is long-read-assembly"),
        ([2**62], "no run at"),
    ):
        resp = await role_keyed_clients["wet"].post(
            URL_FEATURE_TABLE_SAMPLE, json=_body(runs, prep_sample_idx=[run["ps_a"]])
        )
        assert resp.status_code == 404, resp.text
        assert fragment in resp.json()["detail"], resp.text


async def test_a_named_sample_the_runs_did_not_count_404s(role_keyed_clients, amplicon_run):
    run = amplicon_run
    resp = await role_keyed_clients["wet"].post(
        URL_FEATURE_TABLE_VALUE,
        json=_body([run["processing_idx"]], prep_sample_idx=[run["ps_a"], run["ps_e"]]),
    )
    assert resp.status_code == 404, resp.text
    assert run["fetched"] == []


async def test_refuses_a_partially_readable_cohort_whole(role_keyed_clients, amplicon_run):
    """The reader sees study_1 only; ps_b is study_2's. Nothing is fetched."""
    run = amplicon_run
    resp = await role_keyed_clients["user"].post(
        URL_FEATURE_TABLE_VALUE,
        json=_body([run["processing_idx"]], prep_sample_idx=[run["ps_a"], run["ps_b"]]),
    )
    assert resp.status_code == 403, resp.text
    assert run["fetched"] == []


async def test_study_idx_names_that_studys_samples(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    resp = await role_keyed_clients["user"].post(
        URL_FEATURE_TABLE_SAMPLE,
        json=_body([run["processing_idx"]], study_idx=run["study_1"]),
    )
    assert resp.status_code == 200, resp.text
    assert [c["export_id"] for c in resp.json()["samples"]] == [
        await _qm(db, run["processing_idx"], p) for p in sorted([run["ps_a"], run["ps_c"]])
    ]


async def test_study_idx_with_none_of_the_runs_404s(role_keyed_clients, amplicon_run):
    resp = await role_keyed_clients["wet"].post(
        URL_FEATURE_TABLE_SAMPLE,
        json=_body([amplicon_run["processing_idx"]], study_idx=2**62),
    )
    assert resp.status_code == 404, resp.text


@pytest.mark.parametrize(
    "body",
    [
        {"processing_idx": [1]},
        {"processing_idx": [1], "study_idx": 1, "prep_sample_idx": [1]},
        {"processing_idx": [], "study_idx": 1},
        {"processing_idx": list(range(1, MAX_FEATURE_TABLE_RUNS + 2)), "study_idx": 1},
    ],
    ids=["no samples", "both cohorts", "no runs", "too many runs"],
)
async def test_rejects_a_malformed_request(role_keyed_clients, amplicon_run, body):
    resp = await role_keyed_clients["wet"].post(URL_FEATURE_TABLE_SAMPLE, json=body)
    assert resp.status_code == 422, resp.text


async def test_a_data_plane_failure_is_a_502(role_keyed_clients, amplicon_run, monkeypatch):
    def _fail(**_):
        raise flight.FlightUnavailableError("data plane down")

    monkeypatch.setattr("qiita_control_plane.routes.run_feature_table.fetch_runs", _fail)
    resp = await role_keyed_clients["wet"].post(
        URL_FEATURE_TABLE_VALUE,
        json=_body([amplicon_run["processing_idx"]], prep_sample_idx=[amplicon_run["ps_a"]]),
    )
    assert resp.status_code == 502, resp.text


async def test_requires_auth(role_keyed_clients, amplicon_run):
    from httpx import ASGITransport, AsyncClient

    from qiita_control_plane.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(
            URL_FEATURE_TABLE_VALUE,
            json=_body([amplicon_run["processing_idx"]], study_idx=1),
        )
    assert resp.status_code == 401, resp.text


async def test_features_read_no_observations(role_keyed_clients, amplicon_run, monkeypatch):
    calls = []

    def _fetch(*, data_plane_url, signing_key, cohort, values=True):
        calls.append(values)
        sequences = pa.table(
            {
                "feature_idx": pa.array([77], pa.int64()),
                "sequence_hash": pa.array([hashlib.md5(ASV.encode()).hexdigest()], pa.string()),
                "sequence_length_bp": pa.array([len(ASV)], pa.int64()),
                "sequence": pa.array([ASV], pa.string()),
            }
        )
        return None, sequences

    monkeypatch.setattr("qiita_control_plane.routes.run_feature_table.fetch_runs", _fetch)
    resp = await role_keyed_clients["wet"].post(
        URL_FEATURE_TABLE_FEATURE,
        json=_body([amplicon_run["processing_idx"]], prep_sample_idx=[amplicon_run["ps_a"]]),
    )
    assert resp.status_code == 200, resp.text
    assert calls == [False]
