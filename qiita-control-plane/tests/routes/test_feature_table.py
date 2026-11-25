"""Route tests for POST /feature-table.

One request — a method, its parameters, and a study or a list of biosamples, each
named by any of its identifiers —
answered as one body carrying the table's columns, values and features. The fixture
runs are amplicon. A column is a processed sample (a live QM of a matching run), named
by its QM. Access is all-or-nothing over the cohort, checked before the data plane is
read. The data-plane read is stubbed; its scope is pinned by the data plane's own
tests.
"""

import hashlib
import uuid

import pyarrow as pa
import pyarrow.flight as flight
import pytest
from qiita_common.amplicon_constants import (
    AMPLICON_MEMBERSHIP_VISIBLE_TABLE,
    AMPLICON_SEQUENCE_TABLE,
)
from qiita_common.api_paths import URL_FEATURE_TABLE
from qiita_common.flight_stream import StreamRowCountMismatch

from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.stored_feature_table import (
    FEATURE_TABLE_MEDIA_TYPE,
    unpack_feature_table,
)
from qiita_control_plane.testing.db_seeds import (
    seed_prep_sample_to_study_link,
    seed_sequenced_prep_sample,
)
from qiita_control_plane.testing.db_teardown import teardown_entity_graph

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
    fetched: list[tuple[str, list[int], list[int]]] = []

    def _fetch(*, data_plane_url, signing_key, cohort, table):
        processing_idxs, prep_sample_idxs = cohort.runs, cohort.prep_sample_idxs
        fetched.append((table, list(processing_idxs), list(prep_sample_idxs)))
        if table == AMPLICON_SEQUENCE_TABLE:
            return _sequences()
        # One row per processed sample, as the lake stores them: a run's counts cover
        # only the prep_samples it processed.
        rows = [(s.processing_idx, s.prep_sample_idx) for s in cohort.samples]
        return pa.table(
            {
                "prep_sample_idx": pa.array([p for _, p in rows], pa.int64()),
                "processing_idx": pa.array([r for r, _ in rows], pa.int64()),
                "feature_idx": pa.array([77] * len(rows), pa.int64()),
                "count": pa.array(range(1, len(rows) + 1), pa.int64()),
            }
        )

    async def _labels(pool, *, source, created_by_idx, features):
        # Stand in for the exported-feature mint (tested separately): a deterministic
        # QF per feature_idx, so the route tests assert handles, not minted idxs.
        fids = sorted({int(f) for f in features.column("feature_idx").to_pylist()})
        return pa.table(
            {
                "feature_idx": pa.array(fids, pa.int64()),
                "export_feature_id": pa.array([f"QF{f}" for f in fids], pa.string()),
            }
        )

    monkeypatch.setattr("qiita_control_plane.routes.feature_table.fetch_table", _fetch)
    monkeypatch.setattr("qiita_control_plane.routes.feature_table.mint_feature_labels", _labels)
    # The public handles a caller names the cohort by: each prep_sample's biosample's QB,
    # and each study's QS.
    qb = {seed[ps]: await _qb(db, seed[ps]) for ps in ("ps_a", "ps_b", "ps_c", "ps_d", "ps_e")}
    qs = {seed[s]: await _qs(db, seed[s]) for s in ("study_1", "study_2")}
    yield {
        "processing_idx": processing_idx,
        "runs": runs,
        "fetched": fetched,
        "qb": qb,
        "qs": qs,
        **seed,
    }
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


async def _qs(db, study_idx):
    return await db.fetchval(
        "SELECT export_entity_id FROM qiita.exported_entity WHERE study_idx = $1", study_idx
    )


async def _set_accession(db, prep_sample_idx, accession):
    await db.execute(
        "UPDATE qiita.biosample SET ena_sample_accession = $1"
        " WHERE idx = (SELECT biosample_idx FROM qiita.prep_sample WHERE idx = $2)",
        accession,
        prep_sample_idx,
    )


def _sequences() -> pa.Table:
    return pa.table(
        {
            "feature_idx": pa.array([77], pa.int64()),
            "sequence_hash": pa.array([hashlib.md5(ASV.encode()).hexdigest()], pa.string()),
            "sequence_length_bp": pa.array([len(ASV)], pa.int64()),
            "sequence": pa.array([ASV], pa.string()),
        }
    )


def _answer(resp):
    """The body's columns (as JSON), values and features."""
    assert resp.headers["content-type"] == FEATURE_TABLE_MEDIA_TYPE
    samples, values, features = unpack_feature_table(resp.content)
    return samples.model_dump(), values, features


def _body(*, study=None, biosamples=None, key=None, method="amplicon", **parameters):
    """A request naming the cohort by a study's QS or by biosamples' QBs, or by `key`."""
    body = {"method": method}
    if parameters:
        body["parameters"] = parameters
    if study is not None:
        body["study"] = {"value": study} if key is None else {"key": key, "value": study}
    if biosamples is not None:
        body["biosample"] = (
            {"values": biosamples} if key is None else {"key": key, "values": biosamples}
        )
    return body


def _qbs(run, *prep_sample_keys):
    return [run["qb"][run[k]] for k in prep_sample_keys]


async def _post(client, body):
    return await client.post(URL_FEATURE_TABLE, json=body)


async def test_one_answer_carries_the_columns_values_and_features(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    cohort = sorted([run["ps_a"], run["ps_b"], run["ps_c"]])
    resp = await _post(role_keyed_clients["wet"], _body(biosamples=[run["qb"][p] for p in cohort]))
    assert resp.status_code == 200, resp.text
    samples, values, features = _answer(resp)
    qms = [await _qm(db, run["processing_idx"], p) for p in cohort]

    assert samples["method"] == "amplicon"
    assert samples["value_kind"] == "count"
    # The settings the table's runs share, whether or not the request named them.
    assert samples["parameters"] == {"trim": 150, "orient_primer": False}
    assert samples["samples"] == [
        {"export_id": qm, "biosample": await _qb(db, p)} for qm, p in zip(qms, cohort, strict=True)
    ]
    assert values.column_names == ["sample", "feature", "value"]
    assert sorted(zip(*values.to_pydict().values(), strict=True)) == sorted(
        (qm, "QF77", float(i + 1)) for i, qm in enumerate(qms)
    )
    assert features.to_pylist() == [
        {
            "feature": "QF77",
            "sequence": ASV,
            "length": len(ASV),
            "md5": hashlib.md5(ASV.encode()).hexdigest(),
        }
    ]
    # Each table read once, under the one scope.
    assert sorted(run["fetched"]) == sorted(
        (t, [run["processing_idx"]], cohort)
        for t in (AMPLICON_MEMBERSHIP_VISIBLE_TABLE, AMPLICON_SEQUENCE_TABLE)
    )


async def test_a_column_is_its_qm_and_the_accession_names_its_biosample(
    role_keyed_clients, amplicon_run
):
    """Every column is named by its QM, as `feature-table build` names them; a deposited
    accession is the biosample's public name, the key for joining metadata."""
    run, db = amplicon_run, role_keyed_clients["pool"]
    accession = f"SAMEA{uuid.uuid4().int % 10**9}"
    await _set_accession(db, run["ps_a"], accession)
    try:
        resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(run, "ps_a")))
        assert resp.status_code == 200, resp.text
        samples, values, _ = _answer(resp)
        qm = await _qm(db, run["processing_idx"], run["ps_a"])
        assert samples["samples"] == [{"export_id": qm, "biosample": accession}]
        assert set(values.column("sample").to_pylist()) == {qm}
    finally:
        await _set_accession(db, run["ps_a"], None)


async def test_one_prep_sample_in_two_matching_runs_is_two_columns(
    role_keyed_clients, amplicon_run
):
    """Two comparable runs that both processed ps_a: two processed samples, so two
    columns, each under its own QM."""
    run, db = amplicon_run, role_keyed_clients["pool"]
    second = await _mint_run(db, run["owner_idx"], [run["ps_a"]])
    run["runs"].append(second)
    resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(run, "ps_a")))
    assert resp.status_code == 200, resp.text
    assert set(_answer(resp)[1].column("sample").to_pylist()) == {
        await _qm(db, run["processing_idx"], run["ps_a"]),
        await _qm(db, second, run["ps_a"]),
    }
    assert {(t, tuple(r)) for t, r, _ in run["fetched"]} == {
        (t, tuple(sorted([run["processing_idx"], second])))
        for t in (AMPLICON_MEMBERSHIP_VISIBLE_TABLE, AMPLICON_SEQUENCE_TABLE)
    }


async def test_runs_that_disagree_are_refused_naming_what_to_choose(
    role_keyed_clients, amplicon_run
):
    """The cohort's runs differ in trim, so their features do not compare: the refusal
    lists each setting found, and naming one in `parameters` selects its runs."""
    run, db = amplicon_run, role_keyed_clients["pool"]
    other = await _mint_run(db, run["owner_idx"], [run["ps_a"]], trim=100)
    run["runs"].append(other)

    resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(run, "ps_a")))
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert "trim=100" in detail and "trim=150" in detail, detail
    assert run["fetched"] == []

    resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(run, "ps_a"), trim=100))
    assert resp.status_code == 200, resp.text
    samples, values, _ = _answer(resp)
    assert samples["parameters"]["trim"] == 100
    assert set(values.column("sample").to_pylist()) == {await _qm(db, other, run["ps_a"])}


async def test_a_parameter_matches_as_json_compares_it(role_keyed_clients, amplicon_run):
    """A parameter matches a run's stored setting as JSON compares them: 150 matches the
    stored 150, and "150" does not."""
    run = amplicon_run
    body = _body(biosamples=_qbs(run, "ps_a"), trim=150)
    assert (await _post(role_keyed_clients["wet"], body)).status_code == 200
    resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(run, "ps_a"), trim="150"))
    assert resp.status_code == 404, resp.text


async def test_a_deprecated_run_is_not_selected(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    other = await _mint_run(db, run["owner_idx"], [run["ps_a"]], trim=100)
    run["runs"].append(other)
    await db.execute(
        "UPDATE qiita.processing SET status = 'deprecated', deprecated_at = now(),"
        " deprecated_by_idx = $2, deprecation_reason = 'test'"
        " WHERE processing_idx = $1",
        other,
        run["owner_idx"],
    )
    resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(run, "ps_a")))
    assert resp.status_code == 200, resp.text
    assert set(_answer(resp)[1].column("sample").to_pylist()) == {
        await _qm(db, run["processing_idx"], run["ps_a"])
    }


async def test_more_matching_runs_than_one_table_merges_413s(
    role_keyed_clients, amplicon_run, monkeypatch
):
    monkeypatch.setattr("qiita_control_plane.stored_feature_table.MAX_FEATURE_TABLE_RUNS", 1)
    run, db = amplicon_run, role_keyed_clients["pool"]
    run["runs"].append(await _mint_run(db, run["owner_idx"], [run["ps_a"]]))
    resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(run, "ps_a")))
    assert resp.status_code == 413, resp.text
    assert run["fetched"] == []


@pytest.mark.parametrize(
    ("body", "fragment"),
    [
        ({"method": "long-read-assembly"}, "serves amplicon"),
        ({"reference": "gg2"}, "takes the parameters"),
    ],
    ids=["unserved method", "unknown parameter"],
)
async def test_refuses_what_the_method_does_not_take(
    role_keyed_clients, amplicon_run, body, fragment
):
    """Refused before any lookup: the study is one no value names, which would be a 404
    if it were looked up first."""
    run = amplicon_run
    request = _body(study="QS999999999999")
    if "method" in body:
        request["method"] = body["method"]
    else:
        request["parameters"] = body
    resp = await _post(role_keyed_clients["wet"], request)
    assert resp.status_code == 422, resp.text
    assert fragment in resp.json()["detail"], resp.text
    assert run["fetched"] == []


async def test_a_named_biosample_no_matching_run_processed_404s(role_keyed_clients, amplicon_run):
    run = amplicon_run
    resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(run, "ps_a", "ps_e")))
    assert resp.status_code == 404, resp.text
    assert run["fetched"] == []


async def test_refuses_a_partially_readable_cohort_whole(role_keyed_clients, amplicon_run):
    """The reader sees study_1 only; ps_b is study_2's. Nothing is fetched."""
    run = amplicon_run
    resp = await _post(role_keyed_clients["user"], _body(biosamples=_qbs(run, "ps_a", "ps_b")))
    assert resp.status_code == 403, resp.text
    assert run["fetched"] == []


async def test_a_study_names_its_processed_samples(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    resp = await _post(role_keyed_clients["user"], _body(study=run["qs"][run["study_1"]]))
    assert resp.status_code == 200, resp.text
    assert [c["export_id"] for c in _answer(resp)[0]["samples"]] == [
        await _qm(db, run["processing_idx"], p) for p in sorted([run["ps_a"], run["ps_c"]])
    ]


async def test_a_value_that_names_no_study_404s(role_keyed_clients, amplicon_run):
    resp = await _post(role_keyed_clients["wet"], _body(study="QS999999999999"))
    assert resp.status_code == 404, resp.text


async def test_an_unreadable_named_biosample_403s_before_the_unprocessed_404(
    role_keyed_clients, amplicon_run
):
    """The named biosamples' prep_samples are authorized before the cohort is resolved,
    so a caller who may not read one is refused 403, never handed the 404 that lists
    which named biosamples no run processed. ps_b is study_2's (unreadable to user);
    ps_e's biosample is unprocessed."""
    run = amplicon_run
    resp = await _post(role_keyed_clients["user"], _body(biosamples=_qbs(run, "ps_b", "ps_e")))
    assert resp.status_code == 403, resp.text
    assert run["fetched"] == []


async def test_a_study_cohort_over_the_cap_413s(role_keyed_clients, amplicon_run, monkeypatch):
    """A study cohort has no request-model bound, so it is capped at resolve: wider than
    MAX_COHORT_PREP_SAMPLE_IDX is refused (413) rather than buffered into one response."""
    monkeypatch.setattr("qiita_control_plane.stored_feature_table.MAX_COHORT_PREP_SAMPLE_IDX", 1)
    run = amplicon_run
    resp = await _post(role_keyed_clients["user"], _body(study=run["qs"][run["study_1"]]))
    assert resp.status_code == 413, resp.text
    assert run["fetched"] == []


async def test_biosamples_spanning_more_prep_samples_than_the_cap_413(
    role_keyed_clients, amplicon_run, monkeypatch
):
    """A biosample cohort is bounded by its values, not its prep_samples, so it is capped
    at resolve too."""
    monkeypatch.setattr("qiita_control_plane.stored_feature_table.MAX_COHORT_PREP_SAMPLE_IDX", 1)
    run = amplicon_run
    resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(run, "ps_a", "ps_c")))
    assert resp.status_code == 413, resp.text
    assert run["fetched"] == []


async def test_an_unreadable_study_403s_before_it_is_resolved(
    role_keyed_clients, amplicon_run, monkeypatch
):
    """A study cohort is authorized before it is resolved, so a caller who may not
    read the study learns nothing of its cohort — not its size (the 413) and not
    whether any run processed it (the 404)."""
    monkeypatch.setattr("qiita_control_plane.stored_feature_table.MAX_COHORT_PREP_SAMPLE_IDX", 0)
    run = amplicon_run
    resp = await _post(role_keyed_clients["user"], _body(study=run["qs"][run["study_2"]]))
    assert resp.status_code == 403, resp.text
    assert run["fetched"] == []


async def test_a_readable_study_with_a_prep_sample_linked_elsewhere_403s(
    role_keyed_clients, amplicon_run
):
    """Passing the study's own gate is not enough: ps_e is linked to study_1 (readable)
    and study_2 (not), so the resolved cohort is refused whole, before any read."""
    run, db = amplicon_run, role_keyed_clients["pool"]
    run["runs"].append(await _mint_run(db, run["owner_idx"], [run["ps_e"]]))
    resp = await _post(role_keyed_clients["user"], _body(study=run["qs"][run["study_1"]]))
    assert resp.status_code == 403, resp.text
    assert run["fetched"] == []


async def test_biosamples_name_their_processed_samples_by_any_identifier(
    role_keyed_clients, amplicon_run
):
    """A biosample is named by any identifier kind the biosample lookup takes: here by
    its idx rather than its QB. The columns are the same."""
    run, db = amplicon_run, role_keyed_clients["pool"]
    biosample_idx = await db.fetchval(
        "SELECT biosample_idx FROM qiita.prep_sample WHERE idx = $1", run["ps_a"]
    )
    resp = await _post(role_keyed_clients["wet"], _body(biosamples=[str(biosample_idx)], key="idx"))
    assert resp.status_code == 200, resp.text
    assert [c["export_id"] for c in _answer(resp)[0]["samples"]] == [
        await _qm(db, run["processing_idx"], run["ps_a"])
    ]


async def test_each_processed_prep_sample_of_a_biosample_is_a_column(
    role_keyed_clients, amplicon_run
):
    """A biosample with two prep_samples a matching run processed (two library
    preps) is two columns, each under its own QM."""
    run, db = amplicon_run, role_keyed_clients["pool"]
    biosample_idx = await db.fetchval(
        "SELECT biosample_idx FROM qiita.prep_sample WHERE idx = $1", run["ps_a"]
    )
    second = await seed_sequenced_prep_sample(
        db, biosample_idx=biosample_idx, owner_idx=run["owner_idx"]
    )
    await seed_prep_sample_to_study_link(
        db, prep_sample_idx=second, study_idx=run["study_1"], created_by_idx=run["owner_idx"]
    )
    rerun = await _mint_run(db, run["owner_idx"], [second])
    run["runs"].append(rerun)
    try:
        resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(run, "ps_a")))
        assert resp.status_code == 200, resp.text
        assert {c["export_id"] for c in _answer(resp)[0]["samples"]} == {
            await _qm(db, run["processing_idx"], run["ps_a"]),
            await _qm(db, rerun, second),
        }
    finally:
        await db.execute("DELETE FROM qiita.exported_identifier WHERE processing_idx = $1", rerun)
        await teardown_entity_graph(db, study_idxs=[], biosample_idxs=[], prep_sample_idxs=[second])


async def test_a_value_that_names_no_biosample_is_refused_with_every_such_value(
    role_keyed_clients, amplicon_run
):
    """As the bulk biosample link refuses: a 422 naming every value that resolves to no
    live biosample, before anything is read."""
    run = amplicon_run
    resp = await _post(
        role_keyed_clients["wet"],
        _body(biosamples=[*_qbs(run, "ps_a"), "QB999999999998", "QB999999999999"]),
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert "QB999999999998" in detail and "QB999999999999" in detail, detail
    assert run["fetched"] == []


async def test_a_study_named_by_an_accession(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    accession = f"PRJEB{uuid.uuid4().int % 10**6}"
    await db.execute(
        "UPDATE qiita.study SET bioproject_accession = $1 WHERE idx = $2",
        accession,
        run["study_1"],
    )
    try:
        resp = await _post(
            role_keyed_clients["user"], _body(study=accession, key="bioproject_accession")
        )
        assert resp.status_code == 200, resp.text
        assert len(_answer(resp)[0]["samples"]) == 2
    finally:
        await db.execute(
            "UPDATE qiita.study SET bioproject_accession = NULL WHERE idx = $1", run["study_1"]
        )


@pytest.mark.parametrize(
    "body",
    [
        {"method": "amplicon"},
        {"method": "amplicon", "study": {"value": "QS1"}, "biosample": {"values": ["QB1"]}},
        {"study": {"value": "QS1"}},
        {"method": "amplicon", "study": {"value": "QS1"}, "parameters": {"trim": [150]}},
        {"method": "amplicon", "study": {"value": "QS1"}, "processing_idx": [1]},
        {"method": "amplicon", "study_idx": 1},
        {"method": "amplicon", "study": {"key": "idx", "value": "1"}},
        {"method": "amplicon", "biosample": {"values": ["QB1", "QB1"]}},
        {"method": "amplicon", "biosample": {"key": "idx", "values": ["one"]}},
        {"method": "amplicon", "biosample": {"values": []}},
    ],
    ids=[
        "no samples",
        "both cohorts",
        "no method",
        "non-scalar parameter",
        "runs named",
        "study by internal idx",
        "study key not a study identifier",
        "repeated biosample",
        "malformed biosample idx",
        "no biosamples",
    ],
)
async def test_rejects_a_malformed_request(role_keyed_clients, amplicon_run, body):
    resp = await _post(role_keyed_clients["wet"], body)
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize(
    "failure",
    [
        flight.FlightUnavailableError("data plane down"),
        StreamRowCountMismatch("the stream ended early"),
    ],
    ids=["unavailable", "short stream"],
)
async def test_a_data_plane_failure_is_a_502(
    role_keyed_clients, amplicon_run, monkeypatch, failure
):
    def _fail(**_):
        raise failure

    monkeypatch.setattr("qiita_control_plane.routes.feature_table.fetch_table", _fail)
    resp = await _post(role_keyed_clients["wet"], _body(biosamples=_qbs(amplicon_run, "ps_a")))
    assert resp.status_code == 502, resp.text


async def test_requires_auth(role_keyed_clients, amplicon_run):
    from httpx import ASGITransport, AsyncClient

    from qiita_control_plane.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await _post(anon, _body(study="QS1"))
    assert resp.status_code == 401, resp.text
