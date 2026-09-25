"""Route tests for the reads `qiita assembly export` composes, as a VIEWER:

* ``GET /assembly/{prep_sample_idx}/{processing_idx}/membership[/parquet]`` — one
  run's membership rows, every kind, with the assembler's per-contig report;
* ``GET /assembly/{processing_idx}/prep-sample`` — the samples under a run the
  caller may read;
* ``POST /assembly/{prep_sample_idx}/{processing_idx}/ticket/doget`` for
  ``bin_quality``.

The caller is a plain user holding `Tier.VIEWER` on one study, which is the tier the
export is for and the one the /processing roster does not serve.
"""

import base64
import json
import secrets
import struct
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_ASSEMBLY_MEMBERSHIP,
    URL_ASSEMBLY_MEMBERSHIP_PARQUET,
    URL_ASSEMBLY_PREP_SAMPLE,
    URL_ASSEMBLY_RUN_DOGET,
    URL_PROCESSING_PREP_SAMPLE,
)
from qiita_common.assembly_constants import BIN_QUALITY_TABLE, KIND_LCG, KIND_MAG, KIND_UNBINNED

from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_seeds import (
    seed_bare_feature,
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)

pytestmark = pytest.mark.db

_TEST_SEED = b"\x00" * 32


@pytest_asyncio.fixture
async def ctx(postgres_pool, regular_user_session, human_admin_session):
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused", flight_signing_key=_TEST_SEED, data_plane_url="unused"
    )
    transport = ASGITransport(app=app)
    async with (
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {regular_user_session['token']}"},
        ) as viewer,
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {human_admin_session['token']}"},
        ) as admin,
    ):
        yield {"viewer": viewer, "admin": admin}


@pytest_asyncio.fixture
async def export(postgres_pool, regular_user_session):
    """Two studies owned by someone else. The viewer holds `Tier.VIEWER` on the first
    only.

    * ``a`` — in the viewed study, with an accession and a sequenced_pool, completed
      under run ``p`` and under a second run ``q``.
    * ``b`` — in the viewed study, pending under ``p``.
    * ``c`` — in the other study, completed under ``p``.

    ``shared`` is one contig in both of ``a``'s runs: a MAG member under ``p`` and an
    UNBINNED contig under ``q``, which is the shape the export must not double. No row
    carries a ``genome_idx``, so every read here would 422 on the genome map.
    """
    db = postgres_pool
    viewer_idx = regular_user_session["principal_idx"]
    suffix = secrets.token_hex(4)
    owner = await seed_user_principal(db, prefix="assembly-export", suffix=suffix)

    async def _study(title: str) -> int:
        return await db.fetchval(
            "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
            " VALUES ($1, $2, $1) RETURNING idx",
            owner,
            f"{title}-{suffix}",
        )

    viewed, other = await _study("viewed"), await _study("other")
    await db.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, 'viewer', $3)",
        viewed,
        viewer_idx,
        owner,
    )

    samples, biosamples = {}, {}
    for name, study in (("a", viewed), ("b", viewed), ("c", other)):
        biosamples[name], samples[name] = await seed_biosample_with_sequenced_prep_sample(
            db, owner_idx=owner
        )
        await seed_biosample_to_study_link(
            db, biosample_idx=biosamples[name], study_idx=study, created_by_idx=owner
        )
        await seed_prep_sample_to_study_link(
            db, prep_sample_idx=samples[name], study_idx=study, created_by_idx=owner
        )
    accession = f"SAMEA{suffix}"
    await db.execute(
        "UPDATE qiita.biosample SET biosample_accession = $1 WHERE idx = $2",
        accession,
        biosamples["a"],
    )
    run_idx, pool_idx, _ = await seed_sequenced_sample_subtype(
        db, prep_sample_idx=samples["a"], owner_idx=owner, sequenced_pool_item_id="a"
    )

    async def _processing(tag: str) -> int:
        version = f"v-{uuid.uuid4()}"
        async with db.acquire() as conn:
            row = await mint_processing(
                conn,
                workflow="long-read-assembly",
                version=version,
                params={"workflow": "long-read-assembly", "version": version, "tag": tag},
            )
        return row["processing_idx"]

    p, q = await _processing(f"{suffix}-p"), await _processing(f"{suffix}-q")
    await db.executemany(
        "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, $3)",
        [
            (p, samples["a"], "completed"),
            (p, samples["b"], "pending"),
            (p, samples["c"], "completed"),
            (q, samples["a"], "completed"),
        ],
    )
    features = {n: await seed_bare_feature(db) for n in ("lcg", "mag", "shared", "unb", "c")}
    rows = [
        (samples["a"], p, KIND_LCG, "u7ctg", features["lcg"], "u7ctg_circular-yes", "yes", 30.5),
        (samples["a"], p, KIND_MAG, "bin.1", features["mag"], "u1ctg", "no", 12.0),
        (samples["a"], p, KIND_MAG, "bin.1", features["shared"], "u2ctg", "no", 11.0),
        (samples["a"], p, KIND_UNBINNED, "u9ctg", features["unb"], None, None, None),
        (samples["a"], q, KIND_UNBINNED, "u3ctg", features["shared"], "u3ctg", "no", 4.0),
        (samples["c"], p, KIND_MAG, "bin.1", features["c"], None, None, None),
    ]
    await db.executemany(
        "INSERT INTO qiita.assembly_membership"
        " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx,"
        "  raw_name, circularity, depth)"
        " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        rows,
    )

    yield {
        "samples": samples,
        "accession": accession,
        "p": p,
        "q": q,
        "features": features,
        "viewed": viewed,
        "other": other,
        "pool_idx": pool_idx,
    }

    runs = [p, q]
    await db.execute(
        "DELETE FROM qiita.assembly_membership WHERE processing_idx = ANY($1::bigint[])", runs
    )
    await db.execute(
        "DELETE FROM qiita.assembly_sample WHERE processing_idx = ANY($1::bigint[])", runs
    )
    await db.execute("DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])", runs)
    await db.execute("DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = $1", samples["a"])
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await db.execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE study_idx = ANY($1::bigint[])",
        [viewed, other],
    )
    await db.execute(
        "DELETE FROM qiita.biosample_to_study WHERE study_idx = ANY($1::bigint[])",
        [viewed, other],
    )
    await db.execute("DELETE FROM qiita.study_access WHERE study_idx = $1", viewed)
    await db.execute("DELETE FROM qiita.study WHERE idx = ANY($1::bigint[])", [viewed, other])
    await db.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", list(samples.values())
    )
    await db.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", list(biosamples.values())
    )
    await db.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", list(features.values())
    )
    await db.execute("DELETE FROM qiita.user WHERE principal_idx = $1", owner)
    await db.execute("DELETE FROM qiita.principal WHERE idx = $1", owner)


def _membership_url(prep_sample_idx: int, processing_idx: int, *, parquet: bool = False) -> str:
    url = URL_ASSEMBLY_MEMBERSHIP_PARQUET if parquet else URL_ASSEMBLY_MEMBERSHIP
    return url.format(prep_sample_idx=prep_sample_idx, processing_idx=processing_idx)


def _roster_url(processing_idx: int) -> str:
    return URL_ASSEMBLY_PREP_SAMPLE.format(processing_idx=processing_idx)


def _rows(entries) -> list[tuple]:
    return [(e["feature_idx"], e["kind"], e["bin_id"]) for e in entries]


# ---------------------------------------------------------------------------
# Membership
# ---------------------------------------------------------------------------


async def test_membership_serves_every_kind_of_one_run_to_a_viewer(ctx, export):
    """Every kind, UNBINNED included, with the attributes, in (kind, bin_id,
    feature_idx) order — and only run ``p``'s rows: the shared contig is a MAG member
    here and nothing else, though run ``q`` holds it as UNBINNED. None of the rows is
    genome-minted, which the genome map would refuse and this read does not look at."""
    f, a = export["features"], export["samples"]["a"]
    resp = await ctx["viewer"].get(_membership_url(a, export["p"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert _rows(body["entries"]) == [
        (f["lcg"], KIND_LCG, "u7ctg"),
        *sorted([(f["mag"], KIND_MAG, "bin.1"), (f["shared"], KIND_MAG, "bin.1")]),
        (f["unb"], KIND_UNBINNED, "u9ctg"),
    ]
    assert body["count"] == 4
    lcg = body["entries"][0]
    assert (lcg["raw_name"], lcg["circularity"], lcg["depth"], lcg["mult"]) == (
        "u7ctg_circular-yes",
        "yes",
        30.5,
        None,
    )
    assert "genome_idx" not in lcg


async def test_membership_parquet_serves_the_same_rows_uncapped(ctx, export, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    monkeypatch.setattr("qiita_control_plane.routes.assembly.ASSEMBLY_MEMBERSHIP_HARD_CAP", 1)
    a, p = export["samples"]["a"], export["p"]
    refused = await ctx["viewer"].get(_membership_url(a, p))
    assert refused.status_code == 413, refused.text
    assert "4 rows" in refused.json()["detail"]

    served = await ctx["viewer"].get(_membership_url(a, p, parquet=True))
    assert served.status_code == 200, served.text
    table = pq.read_table(pa.BufferReader(pa.py_buffer(served.content)))
    monkeypatch.undo()
    expected = (await ctx["viewer"].get(_membership_url(a, p))).json()["entries"]
    assert table.to_pylist() == expected


async def test_membership_refuses_a_sample_the_caller_cannot_read_before_existence(ctx, export):
    """403 for a sample in a study the caller holds nothing on, including for a run
    that does not exist — a 404 there would answer whether that sample assembled."""
    c = export["samples"]["c"]
    for processing_idx in (export["p"], 10**9):
        resp = await ctx["viewer"].get(_membership_url(c, processing_idx))
        assert resp.status_code == 403, resp.text


async def test_membership_404s_a_run_that_never_assembled(ctx, export):
    resp = await ctx["viewer"].get(_membership_url(export["samples"]["a"], 10**9))
    assert resp.status_code == 404, resp.text


async def test_membership_refuses_a_run_that_is_not_completed(ctx, export):
    resp = await ctx["viewer"].get(_membership_url(export["samples"]["b"], export["p"]))
    assert resp.status_code == 409, resp.text
    assert "pending" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# bin_quality on the human run mint
# ---------------------------------------------------------------------------


def _ticket_payload(ticket_b64: str) -> dict:
    raw = base64.b64decode(ticket_b64)
    (length,) = struct.unpack(">I", raw[1:5])
    return json.loads(raw[5 : 5 + length])


async def test_run_mint_signs_bin_quality_for_one_run_to_a_viewer(ctx, export):
    """The filter is the run and nothing else — the shape `build_bin_quality_query`
    accepts with a one-sample cohort."""
    a, p = export["samples"]["a"], export["p"]
    resp = await ctx["viewer"].post(
        URL_ASSEMBLY_RUN_DOGET.format(prep_sample_idx=a, processing_idx=p),
        json={"table": BIN_QUALITY_TABLE},
    )
    assert resp.status_code == 201, resp.text
    payload = _ticket_payload(resp.json()["ticket"])
    assert payload["table"] == BIN_QUALITY_TABLE
    assert payload["filter"] == {"prep_sample_idx": [a], "processing_idx": [p]}


async def test_run_mint_refuses_bin_quality_for_a_sample_the_caller_cannot_read(ctx, export):
    resp = await ctx["viewer"].post(
        URL_ASSEMBLY_RUN_DOGET.format(
            prep_sample_idx=export["samples"]["c"], processing_idx=export["p"]
        ),
        json={"table": BIN_QUALITY_TABLE},
    )
    assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# The export roster
# ---------------------------------------------------------------------------


async def test_roster_lists_the_samples_a_viewer_may_read(ctx, export):
    """``a`` and ``b`` with their states and accessions, not ``c``. The /processing
    roster, which narrows at the submit tier, gives this viewer none of them — the
    difference this route exists for."""
    s = export["samples"]
    resp = await ctx["viewer"].get(_roster_url(export["p"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["samples"] == [
        {
            "prep_sample_idx": s["a"],
            "biosample_accession": export["accession"],
            "assembly_state": "completed",
        },
        {"prep_sample_idx": s["b"], "biosample_accession": None, "assembly_state": "pending"},
    ]
    assert body["count"] == 2

    submit_tier = await ctx["viewer"].get(
        URL_PROCESSING_PREP_SAMPLE.format(processing_idx=export["p"])
    )
    assert submit_tier.status_code == 200, submit_tier.text
    assert not {x["prep_sample_idx"] for x in submit_tier.json()["samples"]} & set(s.values())


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        pytest.param(lambda e: {"study_idx": e["viewed"]}, ["a", "b"], id="viewed-study"),
        pytest.param(lambda e: {"study_idx": e["other"]}, [], id="unreadable-study"),
        pytest.param(lambda e: {"sequenced_pool_idx": e["pool_idx"]}, ["a"], id="pool"),
        pytest.param(lambda e: {"prep_sample_idx": e["samples"]["b"]}, ["b"], id="one-sample"),
    ],
)
async def test_roster_filters(ctx, export, params, expected):
    resp = await ctx["viewer"].get(_roster_url(export["p"]), params=params(export))
    assert resp.status_code == 200, resp.text
    assert [x["prep_sample_idx"] for x in resp.json()["samples"]] == [
        export["samples"][n] for n in expected
    ]


async def test_roster_filters_by_study_for_an_admin(ctx, export):
    """The bypass role reads ``c`` too; the study filter is what narrows it."""
    resp = await ctx["admin"].get(_roster_url(export["p"]), params={"study_idx": export["other"]})
    assert [x["prep_sample_idx"] for x in resp.json()["samples"]] == [export["samples"]["c"]]


async def test_roster_refuses_a_named_sample_the_caller_cannot_read(ctx, export):
    """403 rather than an empty list, and before the run's existence is looked at."""
    for processing_idx in (export["p"], 10**9):
        resp = await ctx["viewer"].get(
            _roster_url(processing_idx), params={"prep_sample_idx": export["samples"]["c"]}
        )
        assert resp.status_code == 403, resp.text


async def test_roster_404s_an_unknown_run(ctx, export):
    resp = await ctx["viewer"].get(_roster_url(10**9))
    assert resp.status_code == 404, resp.text


async def test_roster_refuses_over_its_cap_rather_than_truncating(ctx, export, monkeypatch):
    monkeypatch.setattr("qiita_control_plane.routes.assembly.GATE_ROSTER_HARD_CAP", 1)
    resp = await ctx["admin"].get(_roster_url(export["p"]))
    assert resp.status_code == 413, resp.text
