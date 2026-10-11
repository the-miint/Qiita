"""Route tests for GET /amplicon/{processing_idx}/feature-table.

The cohort is the named pool's population of prep_samples, narrowed by `study_idx` on request.
Access is all-or-nothing over that cohort, in the order the other cohort routes
use: the run exists → access → content. Columns are the counted prep_samples, named by
biosample accession. The data-plane read is stubbed; its scope is pinned by the
data plane's own tests.
"""

import secrets
import uuid

import pyarrow as pa
import pyarrow.flight as flight
import pytest
from qiita_common.actions import AMPLICON_ACTION_ID, LONG_READ_ASSEMBLY_ACTION_ID
from qiita_common.api_paths import URL_AMPLICON_FEATURE_TABLE
from qiita_common.models.reference import Tier

from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
    seed_sequenced_sample_subtype,
    seed_study,
)
from qiita_control_plane.testing.db_teardown import teardown_entity_graph

pytestmark = pytest.mark.db


@pytest.fixture
async def amplicon_run(role_keyed_clients, monkeypatch):
    """One pool: ps_a + ps_c in study_1, ps_b in study_2, each biosample with a
    BioSample accession; the regular user reads study_1 only. An amplicon run, and
    the data-plane fetch stubbed to count every prep_sample it is asked for except
    those listed in `uncounted`."""
    db = role_keyed_clients["pool"]
    owner = role_keyed_clients["wet_session"]["principal_idx"]
    reader = role_keyed_clients["user_session"]["principal_idx"]
    studies = [
        await seed_study(db, owner_idx=owner, title=f"amplicon-ft-{i}-{secrets.token_hex(4)}")
        for i in (1, 2)
    ]
    await db.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, $3::qiita.tier, $4)",
        studies[0],
        reader,
        Tier.VIEWER,
        owner,
    )
    run_idx = pool_idx = None
    samples = []  # (biosample_idx, prep_sample_idx)
    for i, study_idx in enumerate([studies[0], studies[1], studies[0]]):
        bs, ps = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
        run_idx, pool_idx, _ = await seed_sequenced_sample_subtype(
            db,
            prep_sample_idx=ps,
            owner_idx=owner,
            sequenced_pool_item_id=f"amp-{i}",
            sequencing_run_idx=run_idx,
            sequenced_pool_idx=pool_idx,
        )
        await seed_biosample_to_study_link(
            db, biosample_idx=bs, study_idx=study_idx, created_by_idx=owner
        )
        await seed_prep_sample_to_study_link(
            db, prep_sample_idx=ps, study_idx=study_idx, created_by_idx=owner
        )
        await db.execute(
            "UPDATE qiita.biosample SET biosample_accession = $2 WHERE idx = $1",
            bs,
            f"SAMEA{secrets.randbelow(10**9)}",
        )
        samples.append((bs, ps))
    async with db.acquire() as conn:
        run = await mint_processing(
            conn,
            workflow=AMPLICON_ACTION_ID,
            version="1.0.0",
            params={"workflow": AMPLICON_ACTION_ID, "version": "1.0.0", "probe": str(uuid.uuid4())},
        )
    fetched: list[list[int]] = []
    uncounted: set[int] = set()

    async def _fetch(*, data_plane_url, signing_key, processing_idx, prep_sample_idxs):
        fetched.append(list(prep_sample_idxs))
        counted = [idx for idx in prep_sample_idxs if idx not in uncounted]
        membership = pa.table(
            {
                "prep_sample_idx": pa.array(counted, pa.int64()),
                "processing_idx": pa.array([processing_idx] * len(counted), pa.int64()),
                "feature_idx": pa.array([77] * len(counted), pa.int64()),
                "count": pa.array(range(1, len(counted) + 1), pa.int64()),
            }
        )
        chunks = pa.table(
            {
                "feature_idx": pa.array([77], pa.int64()),
                "chunk_index": pa.array([0], pa.int32()),
                "chunk_data": pa.array(["ACGTACGT"], pa.string()),
            }
        )
        return membership, chunks

    monkeypatch.setattr("qiita_control_plane.routes.amplicon.fetch_amplicon_run", _fetch)
    (bs_a, ps_a), (bs_b, ps_b), (bs_c, ps_c) = samples
    yield {
        "processing_idx": run["processing_idx"],
        "pool_idx": pool_idx,
        "study_1": studies[0],
        "ps_a": ps_a,
        "ps_b": ps_b,
        "ps_c": ps_c,
        "bs_a": bs_a,
        "bs_b": bs_b,
        "bs_c": bs_c,
        "fetched": fetched,
        "uncounted": uncounted,
    }
    await db.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = $1", run["processing_idx"]
    )
    await teardown_entity_graph(
        db,
        study_idxs=studies,
        biosample_idxs=[bs for bs, _ in samples],
        prep_sample_idxs=[ps for _, ps in samples],
    )
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)


def _url(run):
    return URL_AMPLICON_FEATURE_TABLE.format(processing_idx=run["processing_idx"])


async def _accessions(db, biosample_idxs):
    return [
        await db.fetchval("SELECT biosample_accession FROM qiita.biosample WHERE idx = $1", bs)
        for bs in biosample_idxs
    ]


async def test_serves_the_pool_as_tsv_named_by_accession(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    resp = await role_keyed_clients["wet"].get(
        _url(run), params={"sequenced_pool_idx": run["pool_idx"]}
    )
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("text/tab-separated-values")
    labels = await _accessions(db, [run["bs_a"], run["bs_b"], run["bs_c"]])
    assert resp.text.splitlines() == ["#OTU ID\t" + "\t".join(labels), "ACGTACGT\t1\t2\t3"]
    assert run["fetched"] == [sorted([run["ps_a"], run["ps_b"], run["ps_c"]])]


async def test_falls_back_to_the_ena_sample_accession(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    ena = f"ERS{secrets.randbelow(10**9)}"
    await db.execute(
        "UPDATE qiita.biosample SET biosample_accession = NULL, ena_sample_accession = $2"
        " WHERE idx = $1",
        run["bs_b"],
        ena,
    )
    resp = await role_keyed_clients["wet"].get(
        _url(run), params={"sequenced_pool_idx": run["pool_idx"]}
    )
    assert resp.status_code == 200, resp.text
    assert resp.text.splitlines()[0].split("\t")[2] == ena


async def test_only_counted_prep_samples_are_columns(role_keyed_clients, amplicon_run):
    """An uncounted prep_sample needs no name: it has no column, so its missing
    accession does not refuse the table."""
    run, db = amplicon_run, role_keyed_clients["pool"]
    run["uncounted"].add(run["ps_b"])
    await db.execute(
        "UPDATE qiita.biosample SET biosample_accession = NULL WHERE idx = $1", run["bs_b"]
    )
    resp = await role_keyed_clients["wet"].get(
        _url(run), params={"sequenced_pool_idx": run["pool_idx"]}
    )
    assert resp.status_code == 200, resp.text
    labels = await _accessions(db, [run["bs_a"], run["bs_c"]])
    assert resp.text.splitlines() == ["#OTU ID\t" + "\t".join(labels), "ACGTACGT\t1\t2"]


async def test_a_counted_prep_sample_without_an_accession_is_refused(
    role_keyed_clients, amplicon_run
):
    run, db = amplicon_run, role_keyed_clients["pool"]
    await db.execute(
        "UPDATE qiita.biosample SET biosample_accession = NULL WHERE idx = $1", run["bs_b"]
    )
    resp = await role_keyed_clients["wet"].get(
        _url(run), params={"sequenced_pool_idx": run["pool_idx"]}
    )
    assert resp.status_code == 409, resp.text
    assert "no biosample accession" in resp.text


async def test_one_biosample_sequenced_twice_is_refused(role_keyed_clients, amplicon_run):
    """Two columns with one name could not say which prep_sample a count came from."""
    run, db = amplicon_run, role_keyed_clients["pool"]
    await db.execute(
        "UPDATE qiita.prep_sample SET biosample_idx = $2 WHERE idx = $1",
        run["ps_c"],
        run["bs_a"],
    )
    try:
        resp = await role_keyed_clients["wet"].get(
            _url(run), params={"sequenced_pool_idx": run["pool_idx"]}
        )
        assert resp.status_code == 409, resp.text
        assert "more than one column" in resp.text
    finally:
        await db.execute(
            "UPDATE qiita.prep_sample SET biosample_idx = $2 WHERE idx = $1",
            run["ps_c"],
            run["bs_c"],
        )


async def test_an_accession_unsafe_as_a_column_name_is_refused(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    await db.execute(
        "UPDATE qiita.biosample SET biosample_accession = $2 WHERE idx = $1",
        run["bs_b"],
        f"SAMEA{secrets.randbelow(10**9)}\tx",
    )
    resp = await role_keyed_clients["wet"].get(
        _url(run), params={"sequenced_pool_idx": run["pool_idx"]}
    )
    assert resp.status_code == 409, resp.text
    assert "outside" in resp.text


async def test_a_prep_sample_flagged_at_ena_is_not_in_the_cohort(role_keyed_clients, amplicon_run):
    """The same population the pool's other rollups count."""
    run, db = amplicon_run, role_keyed_clients["pool"]
    await db.execute(
        "UPDATE qiita.sequenced_sample SET ena_status = 'suppressed' WHERE prep_sample_idx = $1",
        run["ps_b"],
    )
    resp = await role_keyed_clients["wet"].get(
        _url(run), params={"sequenced_pool_idx": run["pool_idx"]}
    )
    assert resp.status_code == 200, resp.text
    assert run["fetched"] == [sorted([run["ps_a"], run["ps_c"]])]


async def test_a_data_plane_failure_is_a_502(role_keyed_clients, amplicon_run, monkeypatch):
    async def _down(**_):
        raise flight.FlightUnavailableError("connection refused")

    monkeypatch.setattr("qiita_control_plane.routes.amplicon.fetch_amplicon_run", _down)
    resp = await role_keyed_clients["wet"].get(
        _url(amplicon_run), params={"sequenced_pool_idx": amplicon_run["pool_idx"]}
    )
    assert resp.status_code == 502, resp.text


async def test_404s_an_unknown_run(role_keyed_clients, amplicon_run):
    resp = await role_keyed_clients["wet"].get(
        URL_AMPLICON_FEATURE_TABLE.format(processing_idx=2**62),
        params={"sequenced_pool_idx": amplicon_run["pool_idx"]},
    )
    assert resp.status_code == 404, resp.text


async def test_404s_a_run_that_is_not_amplicon(role_keyed_clients, amplicon_run):
    db = role_keyed_clients["pool"]
    async with db.acquire() as conn:
        other = await mint_processing(
            conn,
            workflow=LONG_READ_ASSEMBLY_ACTION_ID,
            version="1.0.0",
            params={"workflow": LONG_READ_ASSEMBLY_ACTION_ID, "probe": str(uuid.uuid4())},
        )
    try:
        resp = await role_keyed_clients["wet"].get(
            URL_AMPLICON_FEATURE_TABLE.format(processing_idx=other["processing_idx"]),
            params={"sequenced_pool_idx": amplicon_run["pool_idx"]},
        )
        assert resp.status_code == 404, resp.text
    finally:
        await db.execute(
            "DELETE FROM qiita.processing WHERE processing_idx = $1", other["processing_idx"]
        )


async def test_404s_a_pool_the_run_counted_nothing_in(role_keyed_clients, amplicon_run):
    run = amplicon_run
    run["uncounted"].update({run["ps_a"], run["ps_b"], run["ps_c"]})
    resp = await role_keyed_clients["wet"].get(
        _url(run), params={"sequenced_pool_idx": run["pool_idx"]}
    )
    assert resp.status_code == 404, resp.text


async def test_requires_a_pool(role_keyed_clients, amplicon_run):
    resp = await role_keyed_clients["wet"].get(_url(amplicon_run))
    assert resp.status_code == 422, resp.text


async def test_refuses_a_partially_readable_pool_whole(role_keyed_clients, amplicon_run):
    """The reader sees study_1 only; ps_b is study_2's. Nothing is fetched."""
    run = amplicon_run
    resp = await role_keyed_clients["user"].get(
        _url(run), params={"sequenced_pool_idx": run["pool_idx"]}
    )
    assert resp.status_code == 403, resp.text
    assert run["fetched"] == []


async def test_study_idx_narrows_to_that_studys_prep_samples(role_keyed_clients, amplicon_run):
    run, db = amplicon_run, role_keyed_clients["pool"]
    resp = await role_keyed_clients["user"].get(
        _url(run), params={"sequenced_pool_idx": run["pool_idx"], "study_idx": run["study_1"]}
    )
    assert resp.status_code == 200, resp.text
    labels = await _accessions(db, [run["bs_a"], run["bs_c"]])
    assert resp.text.splitlines()[0] == "#OTU ID\t" + "\t".join(labels)
    assert run["fetched"] == [sorted([run["ps_a"], run["ps_c"]])]


async def test_study_idx_with_none_of_the_pool_404s(role_keyed_clients, amplicon_run):
    run = amplicon_run
    resp = await role_keyed_clients["wet"].get(
        _url(run), params={"sequenced_pool_idx": run["pool_idx"], "study_idx": 2**62}
    )
    assert resp.status_code == 404, resp.text


async def test_requires_auth(amplicon_run):
    from httpx import ASGITransport, AsyncClient

    from qiita_control_plane.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(
            _url(amplicon_run), params={"sequenced_pool_idx": amplicon_run["pool_idx"]}
        )
    assert resp.status_code == 401, resp.text
