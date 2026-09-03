"""Route tests for the two HUMAN-facing assembly reads over one run:

* ``POST /assembly/{prep_sample_idx}/{processing_idx}/ticket/doget`` — the
  scientist-facing DoGet mint, the counterpart of the service-account one pinned in
  `test_assembly_doget.py`. Same signed filter, same surfaces; what differs is who
  may ask and how the run is authorized.
* ``GET /assembly/{prep_sample_idx}/{processing_idx}/genome-map`` — that run's
  contig -> genome lookup, the de novo arm's twin of the reference genome map.

The two are together because they share an authorization ladder that runs in the
opposite order to the alignment routes' — access first, existence second — and that
ordering is a disclosure decision worth pinning in one place.
"""

import base64
import json
import secrets
import struct
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_ASSEMBLY_GENOME_MAP, URL_ASSEMBLY_RUN_DOGET
from qiita_common.assembly_constants import (
    ASSEMBLED_SEQUENCE_CHUNKS_TABLE,
    ASSEMBLED_SEQUENCE_TABLE,
    KIND_LCG,
    KIND_MAG,
)
from qiita_common.auth_constants import Scope

from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_seeds import (
    seed_bare_feature,
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db

_TEST_SEED = b"\x00" * 32


def _decode_ticket_payload(ticket_b64: str) -> dict:
    """Parse the JSON payload out of a base64 signed Flight ticket.

    Wire format: <1B version><4B payload_len><payload><64B Ed25519 signature><8B expiry>.
    """
    raw = base64.b64decode(ticket_b64)
    payload_len = struct.unpack(">I", raw[1:5])[0]
    return json.loads(raw[5 : 5 + payload_len])


@pytest_asyncio.fixture
async def ctx(postgres_pool, regular_user_session, human_admin_session):
    """Anonymous, plain-user and admin clients.

    The admin is the one that gets past `authorize_prep_sample_cohort` here: the
    seeded samples carry no study link the plain user could hold a tier on, so that
    user is this module's 403 case rather than a second happy path.
    """
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=_TEST_SEED,
        data_plane_url="unused",
    )
    transport = ASGITransport(app=app)

    async with (
        AsyncClient(transport=transport, base_url="http://test") as anon,
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {regular_user_session['token']}"},
        ) as user,
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {human_admin_session['token']}"},
        ) as admin,
    ):
        yield {"anon": anon, "user": user, "admin": admin}


@pytest_asyncio.fixture
async def run(postgres_pool):
    """One assembly run of two contigs in two bins, both genome-minted, plus a
    second run of the same sample that is deliberately left UNMINTED.

    The second run is what discriminates the map's completeness refusal: it is a
    real run with real contigs whose `genome_idx` is NULL, which is exactly the
    pre-backfill state the 422 exists for — and it is a different run of the same
    sample, so a check that keyed on the sample alone would refuse both.
    """
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="assembly-run-read", suffix=suffix
    )
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )

    async def _processing(tag: str) -> int:
        version = f"v-{uuid.uuid4()}"
        async with postgres_pool.acquire() as conn:
            row = await mint_processing(
                conn,
                workflow="long-read-assembly",
                version=version,
                params={
                    "workflow": "long-read-assembly",
                    "version": version,
                    "assembler": "flye",
                    "tag": f"{suffix}-{tag}",
                },
            )
        return row["processing_idx"]

    minted_run, unminted_run = await _processing("minted"), await _processing("unminted")

    features = {name: await seed_bare_feature(postgres_pool) for name in ("lcg", "mag", "orphan")}
    genomes = {}
    for name in ("lcg", "mag"):
        genomes[name] = await postgres_pool.fetchval(
            "INSERT INTO qiita.genome (source, source_id, prep_sample_idx)"
            " VALUES ('qiita', $1, $2) RETURNING genome_idx",
            f"{suffix}-{name}",
            prep_sample_idx,
        )

    # The gate both human routes read. Completion is this value, never the presence
    # of membership rows — `20260819000001_assembly_sample.sql` says so on the column.
    await postgres_pool.executemany(
        "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed')",
        [(minted_run, prep_sample_idx), (unminted_run, prep_sample_idx)],
    )
    await postgres_pool.executemany(
        "INSERT INTO qiita.assembly_membership"
        " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx, genome_idx)"
        " VALUES ($1, $2, $3, $4, $5, $6)",
        [
            (prep_sample_idx, minted_run, KIND_LCG, "contig_1", features["lcg"], genomes["lcg"]),
            (prep_sample_idx, minted_run, KIND_MAG, "bin.1", features["mag"], genomes["mag"]),
            (prep_sample_idx, unminted_run, KIND_MAG, "bin.1", features["orphan"], None),
        ],
    )

    yield {
        "prep_sample_idx": prep_sample_idx,
        "minted_run": minted_run,
        "unminted_run": unminted_run,
        "pairs": {(features["lcg"], genomes["lcg"]), (features["mag"], genomes["mag"])},
    }

    # FK-reverse: assembly_membership references genome, and `genome.prep_sample_idx`
    # is ON DELETE RESTRICT — so the genomes go before the sample that minted them.
    await postgres_pool.execute(
        "DELETE FROM qiita.assembly_sample WHERE processing_idx = ANY($1::bigint[])",
        [minted_run, unminted_run],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.assembly_membership WHERE processing_idx = ANY($1::bigint[])",
        [minted_run, unminted_run],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])", sorted(genomes.values())
    )
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
        sorted(features.values()),
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])",
        [minted_run, unminted_run],
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


def _map_url(prep_sample_idx: int, processing_idx: int) -> str:
    return URL_ASSEMBLY_GENOME_MAP.format(
        prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    )


def _doget_url(prep_sample_idx: int, processing_idx: int) -> str:
    return URL_ASSEMBLY_RUN_DOGET.format(
        prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    )


# ---------------------------------------------------------------------------
# The scope, and who holds it
# ---------------------------------------------------------------------------


def test_assembly_doget_is_human_only_and_on_every_human_ceiling():
    """The mirror of `ticket:doget`'s placement test, and the inverse result. This
    scope is on every human role ceiling and on NO service ceiling — so a worker
    keeps the work-ticket route and one data-plane surface never grows two
    validation paths, which is the whole argument `alignment:doget` makes and this
    one inherits."""
    from qiita_control_plane.auth.scopes import (
        ROLE_IMPLIED_SCOPES,
        SERVICE_ACCOUNT_SCOPE_CEILING,
    )

    assert Scope.ASSEMBLY_DOGET not in SERVICE_ACCOUNT_SCOPE_CEILING
    for role, ceiling in ROLE_IMPLIED_SCOPES.items():
        assert Scope.ASSEMBLY_DOGET in ceiling, f"{role} must imply assembly:doget"


# ---------------------------------------------------------------------------
# Auth matrix, and the order the ladder runs in
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("route", ["map", "doget"])
async def test_anonymous_401(ctx, run, route):
    url = (
        _map_url(run["prep_sample_idx"], run["minted_run"])
        if route == "map"
        else _doget_url(run["prep_sample_idx"], run["minted_run"])
    )
    resp = (
        await ctx["anon"].get(url)
        if route == "map"
        else await ctx["anon"].post(url, json={"table": ASSEMBLED_SEQUENCE_TABLE})
    )
    assert resp.status_code == 401, resp.text


async def test_a_user_with_no_access_to_the_sample_is_refused(ctx, run):
    """The sample carries no study link this user holds a tier on, so neither route
    serves it. The scope alone is not the boundary — every human ceiling implies
    `assembly:doget`, and the per-study check is what actually decides."""
    resp = await ctx["user"].post(
        _doget_url(run["prep_sample_idx"], run["minted_run"]),
        json={"table": ASSEMBLED_SEQUENCE_TABLE},
    )
    assert resp.status_code == 403, resp.text


async def test_access_is_checked_before_existence(ctx, run):
    """**The ladder is access-then-existence, inverting the alignment mint's.**

    There the 404 is about an `alignment_definition` — a global object whose
    existence tells a caller nothing about anyone's samples. Here the thing that may
    not exist is `(this sample, this run)`, so a 404 ahead of the access check would
    answer "was this sample assembled?" for a sample the caller may not read.

    Asserted on a run that does NOT exist for a sample the caller cannot read: the
    correct answer is the 403, and a 404 here would be the leak.
    """
    resp = await ctx["user"].get(_map_url(run["prep_sample_idx"], 10**9))
    assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# The genome map
# ---------------------------------------------------------------------------


async def test_genome_map_returns_the_runs_pairs_scoped_to_that_run(ctx, run):
    """The map is one run's, not one sample's. The same sample has a second run in
    the fixture, and its contig is absent — which is the scoping the analytic
    depends on: a contig assembled by two runs is one `feature_idx` under two
    genomes, so an unscoped map would return both and double-count every read on
    it."""
    resp = await ctx["admin"].get(_map_url(run["prep_sample_idx"], run["minted_run"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["prep_sample_idx"] == run["prep_sample_idx"]
    assert body["processing_idx"] == run["minted_run"]
    assert {(e["feature_idx"], e["genome_idx"]) for e in body["entries"]} == run["pairs"]
    assert body["count"] == len(run["pairs"])
    # Ordered, so a caller diffing two pulls of an unchanged run sees no churn.
    assert [e["feature_idx"] for e in body["entries"]] == sorted(
        e["feature_idx"] for e in body["entries"]
    )


async def test_genome_map_carries_the_genome_provenance(ctx, run):
    """`source` and `source_id` ship for the reason the reference map's do:
    `qiita.genome` is unique on the PAIR, so a consumer relabelling `genome_idx` to
    a public handle needs both to assert no collision. An assembled subject's source
    is `qiita`, which is also how a consumer tells the two arms' genomes apart."""
    resp = await ctx["admin"].get(_map_url(run["prep_sample_idx"], run["minted_run"]))
    assert {e["source"] for e in resp.json()["entries"]} == {"qiita"}
    assert all(e["source_id"] for e in resp.json()["entries"])


async def test_genome_map_404s_a_run_that_never_assembled(ctx, run):
    """An unknown processing_idx, an unknown sample, and a real pair that assembled
    nothing are deliberately one answer — there are no contigs to map. The client
    recipe reads this 404 as "no de novo arm for this sample" and degrades it to
    reference-only, which is why it must not be distinguishable from the others."""
    resp = await ctx["admin"].get(_map_url(run["prep_sample_idx"], 10**9))
    assert resp.status_code == 404, resp.text
    assert "no assembled contigs" in resp.json()["detail"]


async def test_genome_map_refuses_a_run_whose_memberships_are_not_all_minted(ctx, run):
    """422, not a short 200. The contigs with no `genome_idx` are simply absent from
    the map, and the absence does not read as missing rows downstream: the genomes
    they belong to keep their other contigs, so their length denominators come back
    short and their breadth of coverage comes back high — a plausible table, not an
    error. The refusal names the backfill that fixes it."""
    resp = await ctx["admin"].get(_map_url(run["prep_sample_idx"], run["unminted_run"]))
    assert resp.status_code == 422, resp.text
    assert "assembly-genome backfill" in resp.json()["detail"]


async def test_genome_map_refuses_over_its_cap_rather_than_truncating(ctx, run, monkeypatch):
    """413 naming the real size, and the response carries no `truncated` — a lookup
    table silently missing rows yields a WRONG feature table rather than a partial
    one. Same posture and the same shared cap as the reference map."""
    monkeypatch.setattr("qiita_control_plane.routes.assembly.GENOME_MAP_HARD_CAP", 1)
    resp = await ctx["admin"].get(_map_url(run["prep_sample_idx"], run["minted_run"]))
    assert resp.status_code == 413, resp.text
    assert str(len(run["pairs"])) in resp.json()["detail"]


# ---------------------------------------------------------------------------
# The human DoGet mint
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", [ASSEMBLED_SEQUENCE_TABLE, ASSEMBLED_SEQUENCE_CHUNKS_TABLE])
async def test_run_doget_signs_the_pair_for_either_surface(ctx, run, table):
    """The signed filter is the run and nothing else, on both surfaces — identical
    to what the service-account route signs, because the two routes differ in
    authorization and in nothing about what a ticket returns."""
    resp = await ctx["admin"].post(
        _doget_url(run["prep_sample_idx"], run["minted_run"]), json={"table": table}
    )
    assert resp.status_code == 201, resp.text
    payload = _decode_ticket_payload(resp.json()["ticket"])
    assert payload["table"] == table
    assert payload["filter"] == {
        "prep_sample_idx": [run["prep_sample_idx"]],
        "processing_idx": [run["minted_run"]],
    }


async def test_run_doget_body_cannot_name_a_second_run(ctx, run):
    """The run is in the PATH, so the body has one field. `extra="forbid"` is what
    keeps a caller from naming a different pair alongside the authorized one."""
    resp = await ctx["admin"].post(
        _doget_url(run["prep_sample_idx"], run["minted_run"]),
        json={"table": ASSEMBLED_SEQUENCE_TABLE, "prep_sample_idx": 1},
    )
    assert resp.status_code == 422, resp.text


async def test_run_doget_refuses_a_table_outside_the_two_surfaces(ctx, run):
    """The closed set is the service route's, shared through `_sign_assembly_ticket`
    — one definition, so a table this route admits and that one refuses is not
    expressible."""
    resp = await ctx["admin"].post(
        _doget_url(run["prep_sample_idx"], run["minted_run"]), json={"table": "read_masked"}
    )
    assert resp.status_code == 422, resp.text


async def test_run_doget_404s_a_run_that_never_assembled(ctx, run):
    resp = await ctx["admin"].post(
        _doget_url(run["prep_sample_idx"], 10**9), json={"table": ASSEMBLED_SEQUENCE_TABLE}
    )
    assert resp.status_code == 404, resp.text


# ---------------------------------------------------------------------------
# The gate: completion is `assembly_sample.state`, not the presence of rows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("route", ["map", "doget"])
@pytest.mark.parametrize("state", ["pending", "invalidated"])
async def test_neither_route_serves_a_run_that_is_not_completed(
    ctx, run, postgres_pool, route, state
):
    """**Membership rows exist for this run and both routes still refuse.** The
    assembly tail writes membership across several workflow entries, so their
    presence never means the run finished — the schema states that on the column
    itself, and these two reads are the ones that most need it: they feed the
    client-side combined table, whose server-side counterpart refuses exactly these
    states at submit. A `pending` run would give a table that changes underneath the
    caller; an `invalidated` one would carry withdrawn contigs into a published
    result.

    409, not 404: the run exists and the caller may read it, and the reason they are
    being refused is a state a person or a running job can change. Collapsing it into
    the 404 would make it indistinguishable from "assembled nothing", which the
    client treats as a reason to proceed reference-only.
    """
    invalidation = (
        ", invalidated_at = now(), invalidated_by_idx ="
        " (SELECT created_by_idx FROM qiita.prep_sample WHERE idx = $2)"
        ", invalidation_reason = 'seeded withdrawal'"
        if state == "invalidated"
        else ""
    )
    await postgres_pool.execute(
        f"UPDATE qiita.assembly_sample SET state = $3{invalidation}"
        " WHERE processing_idx = $1 AND prep_sample_idx = $2",
        run["minted_run"],
        run["prep_sample_idx"],
        state,
    )
    url = (
        _map_url(run["prep_sample_idx"], run["minted_run"])
        if route == "map"
        else _doget_url(run["prep_sample_idx"], run["minted_run"])
    )
    resp = (
        await ctx["admin"].get(url)
        if route == "map"
        else await ctx["admin"].post(url, json={"table": ASSEMBLED_SEQUENCE_TABLE})
    )
    assert resp.status_code == 409, resp.text
    assert state in resp.json()["detail"]


@pytest.mark.parametrize("route", ["map", "doget"])
async def test_a_run_that_assembled_nothing_stays_a_404(ctx, run, postgres_pool, route):
    """`no_data` keeps the 404, so the client's graceful path is unchanged: it reads
    that status as "no de novo arm for this prep_sample" and builds reference-only.
    Were it a 409 the whole combined build would fail on a run that legitimately
    produced nothing."""
    await postgres_pool.execute(
        "UPDATE qiita.assembly_sample SET state = 'no_data'"
        " WHERE processing_idx = $1 AND prep_sample_idx = $2",
        run["minted_run"],
        run["prep_sample_idx"],
    )
    url = (
        _map_url(run["prep_sample_idx"], run["minted_run"])
        if route == "map"
        else _doget_url(run["prep_sample_idx"], run["minted_run"])
    )
    resp = (
        await ctx["admin"].get(url)
        if route == "map"
        else await ctx["admin"].post(url, json={"table": ASSEMBLED_SEQUENCE_TABLE})
    )
    assert resp.status_code == 404, resp.text
