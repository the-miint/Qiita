"""Route tests for POST /assembly/ticket/doget.

Signs an Ed25519 Flight DoGet ticket for the contig sequences ONE assembly run —
a `(prep_sample_idx, processing_idx)` pair — produced, on the data plane's
`assembled_sequence` / `assembled_sequence_chunks` tables. Service-account-only
(Scope.TICKET_DOGET), same as the alignment doget route.

The signed `feature_idx` list IS the authorization boundary here: the data plane
has no `prep_sample_idx` to re-check against, so whatever the route resolves is
what streams. These tests therefore assert the roster VERBATIM against a fixture
seeded with the three neighbours a wrong query would pick up — the same sample's
other run, the same run's other sample, and a contig belonging to no assembly.
"""

import base64
import json
import secrets
import struct
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_ASSEMBLY_DOGET, URL_REFERENCE_DOGET
from qiita_common.assembly_constants import (
    ASSEMBLED_SEQUENCE_CHUNKS_TABLE,
    ASSEMBLED_SEQUENCE_TABLE,
    KIND_LCG,
    KIND_MAG,
)
from qiita_common.auth_constants import Scope

from qiita_control_plane.auth.token import mint_api_token
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_seeds import (
    seed_bare_feature,
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db

# Ed25519 signing seed the test app signs tickets with; the test decodes the
# payload (not the signature) so any 32-byte value works.
_TEST_SEED = b"\x00" * 32


def _decode_ticket_payload(ticket_b64: str) -> dict:
    """Parse the JSON payload out of a base64 signed Flight ticket.

    Wire format: <1B version><4B payload_len><payload><64B Ed25519 signature><8B expiry>.
    """
    raw = base64.b64decode(ticket_b64)
    payload_len = struct.unpack(">I", raw[1:5])[0]
    return json.loads(raw[5 : 5 + payload_len])


@pytest_asyncio.fixture
async def ctx(postgres_pool, regular_user_session, compute_worker_service_account):
    """Route-test context: anon + regular-user + compute-SA AsyncClients."""
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
            headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
        ) as sa,
    ):
        yield {"pool": postgres_pool, "anon": anon, "user": user, "sa": sa}


@pytest_asyncio.fixture
async def sa_no_scope_client(postgres_pool, compute_worker_service_account):
    """An SA token carrying a scope that is NOT ticket:doget, to exercise the
    require_scope 403 path."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=compute_worker_service_account["principal_idx"],
        label=f"sa-no-assembly-doget-{secrets.token_hex(4)}",
        scopes=[Scope.FEATURE_MINT],
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as client:
        yield client


@pytest_asyncio.fixture
async def env(postgres_pool):
    """Two samples x two assembly runs, laid out so a wrong roster is visible.

    `qiita.assembly_membership` FKs prep_sample, processing and feature, so none
    of the three can be invented. The contigs:

    * `run_a` (sample A, processing A) — `lcg` under KIND_LCG, `mag` and
      `shared` under KIND_MAG in one bin, and `mag` AGAIN under a second
      (kind, bin_id) so the DISTINCT has something to collapse;
    * `run_b` (sample A, processing B) — a different run of the SAME sample,
      holding `other_run` plus `shared`. `shared` is the content-dedup case: one
      feature_idx claimed by two runs, so it must appear in both rosters;
    * `run_c` (sample B, processing A) — the SAME run identity on a different
      sample, holding `other_sample`;
    * `unclaimed` — a real feature no assembly_membership row names.

    Yields the ids; the roster of `run_a` is exactly {lcg, mag, shared}.
    """
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="assembly-doget-test", suffix=suffix
    )
    bs_a, ps_a = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    bs_b, ps_b = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )

    features = {
        name: await seed_bare_feature(postgres_pool)
        for name in ("lcg", "mag", "shared", "other_run", "other_sample", "unclaimed")
    }

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

    proc_a, proc_b = await _processing("a"), await _processing("b")

    await postgres_pool.executemany(
        "INSERT INTO qiita.assembly_membership"
        " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx)"
        " VALUES ($1, $2, $3, $4, $5)",
        [
            (ps_a, proc_a, KIND_LCG, "contig_1", features["lcg"]),
            (ps_a, proc_a, KIND_MAG, "bin.1", features["mag"]),
            (ps_a, proc_a, KIND_MAG, "bin.1", features["shared"]),
            # Same feature, second (kind, bin_id) — the DISTINCT's job.
            (ps_a, proc_a, KIND_MAG, "bin.2", features["mag"]),
            (ps_a, proc_b, KIND_MAG, "bin.1", features["other_run"]),
            (ps_a, proc_b, KIND_MAG, "bin.1", features["shared"]),
            (ps_b, proc_a, KIND_MAG, "bin.1", features["other_sample"]),
        ],
    )

    yield {
        "prep_sample_a": ps_a,
        "prep_sample_b": ps_b,
        "processing_a": proc_a,
        "processing_b": proc_b,
        **features,
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.assembly_membership WHERE processing_idx = ANY($1::bigint[])",
        [proc_a, proc_b],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", [ps_a, ps_b]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [bs_a, bs_b]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
        sorted(features.values()),
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])", [proc_a, proc_b]
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


def _body(
    env,
    *,
    table=ASSEMBLED_SEQUENCE_CHUNKS_TABLE,
    sample="prep_sample_a",
    run="processing_a",
):
    return {
        "prep_sample_idx": env[sample],
        "processing_idx": env[run],
        "table": table,
    }


# ---------------------------------------------------------------------------
# Auth matrix
# ---------------------------------------------------------------------------


async def test_assembly_doget_anonymous_401(ctx, env):
    resp = await ctx["anon"].post(URL_ASSEMBLY_DOGET, json=_body(env))
    assert resp.status_code == 401, resp.text


async def test_assembly_doget_human_user_403(ctx, env, postgres_pool, regular_user_session):
    """Humans can't mint even carrying the scope — require_service rejects the
    HumanUser before require_scope runs. `ticket:doget` is on no human role
    ceiling either, so a real PAT could not carry it in the first place."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=regular_user_session["principal_idx"],
        label=f"human-assembly-doget-{secrets.token_hex(4)}",
        scopes=[Scope.SELF_PROFILE, Scope.TICKET_DOGET],
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as human:
        resp = await human.post(URL_ASSEMBLY_DOGET, json=_body(env))
    assert resp.status_code == 403, resp.text
    assert "service accounts" in resp.json()["detail"]


async def test_assembly_doget_sa_without_scope_403(sa_no_scope_client, env):
    resp = await sa_no_scope_client.post(URL_ASSEMBLY_DOGET, json=_body(env))
    assert resp.status_code == 403, resp.text
    assert "ticket:doget" in resp.json()["detail"]


async def test_ticket_doget_is_service_only_and_on_no_human_ceiling():
    """The scope this route reuses is worker-only, which is what bounds who gains
    contig read-back: a human PAT cannot be minted carrying it at all."""
    from qiita_control_plane.auth.scopes import (
        ROLE_IMPLIED_SCOPES,
        SERVICE_ACCOUNT_SCOPE_CEILING,
    )

    assert Scope.TICKET_DOGET in SERVICE_ACCOUNT_SCOPE_CEILING
    for role, ceiling in ROLE_IMPLIED_SCOPES.items():
        assert Scope.TICKET_DOGET not in ceiling, f"{role} must not imply ticket:doget"


# ---------------------------------------------------------------------------
# The signed roster — exactly this run's contigs and no others
# ---------------------------------------------------------------------------


async def test_doget_signs_exactly_this_runs_contigs(ctx, env):
    """The roster is the run's whole assembly, deduplicated, ascending — and
    excludes the same sample's OTHER run, the same run's OTHER sample, and a
    feature no assembly claims. Asserted verbatim: on this surface the signed
    list is the authorization boundary, so "some rows came back" is not the
    property under test."""
    resp = await ctx["sa"].post(URL_ASSEMBLY_DOGET, json=_body(env))
    assert resp.status_code == 201, resp.text
    payload = _decode_ticket_payload(resp.json()["ticket"])

    assert payload["table"] == ASSEMBLED_SEQUENCE_CHUNKS_TABLE
    expected = sorted([env["lcg"], env["mag"], env["shared"]])
    assert payload["filter"] == {"feature_idx": expected}
    for absent in ("other_run", "other_sample", "unclaimed"):
        assert env[absent] not in payload["filter"]["feature_idx"], absent
    # Nothing but feature_idx is signed: the data plane has no prep_sample_idx
    # column to match on for these tables.
    assert set(payload["filter"]) == {"feature_idx"}
    assert "members" not in payload and "columns" not in payload


async def test_doget_second_run_of_the_same_sample_gets_its_own_roster(ctx, env):
    """processing_idx is the run discriminator: the same prep_sample under the
    other run resolves to that run's contigs. `shared` is in BOTH rosters —
    a contig two runs produced is one content-deduped feature_idx, and scoping
    by membership rows (not by "features minted during this run") is what makes
    it stream for each."""
    resp = await ctx["sa"].post(URL_ASSEMBLY_DOGET, json=_body(env, run="processing_b"))
    assert resp.status_code == 201, resp.text
    payload = _decode_ticket_payload(resp.json()["ticket"])
    assert payload["filter"] == {"feature_idx": sorted([env["other_run"], env["shared"]])}


async def test_doget_second_sample_of_the_same_run_gets_its_own_roster(ctx, env):
    """The mirror of the above on the other key: same processing_idx, different
    prep_sample_idx."""
    resp = await ctx["sa"].post(URL_ASSEMBLY_DOGET, json=_body(env, sample="prep_sample_b"))
    assert resp.status_code == 201, resp.text
    payload = _decode_ticket_payload(resp.json()["ticket"])
    assert payload["filter"] == {"feature_idx": [env["other_sample"]]}


async def test_doget_signs_the_same_roster_for_both_surfaces(ctx, env):
    """`assembled_sequence` and `assembled_sequence_chunks` differ only in what a
    row carries; the scope is the same roster."""
    chunks = _decode_ticket_payload(
        (await ctx["sa"].post(URL_ASSEMBLY_DOGET, json=_body(env))).json()["ticket"]
    )
    flat = _decode_ticket_payload(
        (
            await ctx["sa"].post(
                URL_ASSEMBLY_DOGET, json=_body(env, table=ASSEMBLED_SEQUENCE_TABLE)
            )
        ).json()["ticket"]
    )
    assert flat["table"] == ASSEMBLED_SEQUENCE_TABLE
    assert flat["filter"] == chunks["filter"]


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("missing", ["sample", "run"])
async def test_doget_pair_with_no_contigs_404(ctx, env, missing):
    """A pair with no assembly_membership row is a 404, never a ticket with an
    empty (whole-table) filter. An unknown identifier and a real one that simply
    never assembled are the same answer."""
    body = _body(env)
    body["prep_sample_idx" if missing == "sample" else "processing_idx"] = 2_000_000_000
    resp = await ctx["sa"].post(URL_ASSEMBLY_DOGET, json=body)
    assert resp.status_code == 404, resp.text
    assert "no assembled contigs" in resp.json()["detail"]


@pytest.mark.parametrize(
    "table",
    ["assembly_membership", "bin_quality", "reference_sequences", "read_masked", "", "assembled"],
)
async def test_doget_table_outside_the_two_surfaces_422(ctx, env, table):
    """Only the two sequence surfaces. `assembly_membership` is the junction the
    route itself reads from Postgres and is not Flight-readable; the reference /
    read surfaces have their own routes and their own scope rules."""
    resp = await ctx["sa"].post(URL_ASSEMBLY_DOGET, json=_body(env, table=table))
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param({"prep_sample_idx": 0}, id="prep-sample-not-positive"),
        pytest.param({"processing_idx": 0}, id="processing-not-positive"),
        pytest.param({"prep_sample_idx": -1}, id="prep-sample-negative"),
        pytest.param({"feature_idx": [1, 2]}, id="smuggled-feature-idx"),
        pytest.param({"filter": {"feature_idx": [1]}}, id="smuggled-filter"),
        pytest.param({"columns": ["feature_idx"]}, id="smuggled-columns"),
    ],
)
async def test_doget_bad_body_422(ctx, env, mutate):
    """`extra="forbid"` is what keeps the caller out of the ticket's scope: there
    is no body field that reaches the filter, so a caller cannot widen (or
    narrow) what the control plane resolved."""
    resp = await ctx["sa"].post(URL_ASSEMBLY_DOGET, json=_body(env) | mutate)
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize("drop", ["prep_sample_idx", "processing_idx", "table"])
async def test_doget_missing_field_422(ctx, env, drop):
    body = _body(env)
    del body[drop]
    resp = await ctx["sa"].post(URL_ASSEMBLY_DOGET, json=body)
    assert resp.status_code == 422, resp.text


# ---------------------------------------------------------------------------
# CP-side allowlist consistency
# ---------------------------------------------------------------------------


async def test_doget_assembly_surfaces_in_cp_allowlist():
    """Both tables the route signs must be in the CP-side DoGet allowlist that
    mirrors the data plane's ALLOWED_TABLES — and the junction must NOT be."""
    from qiita_control_plane.routes.reference import _DOGET_ALLOWED_TABLES

    assert ASSEMBLED_SEQUENCE_TABLE in _DOGET_ALLOWED_TABLES
    assert ASSEMBLED_SEQUENCE_CHUNKS_TABLE in _DOGET_ALLOWED_TABLES
    assert "assembly_membership" not in _DOGET_ALLOWED_TABLES
    assert "bin_quality" not in _DOGET_ALLOWED_TABLES


async def test_doget_assembly_not_signable_via_reference_route():
    """The assembly surfaces are served only by this route, scoped to a run's
    roster — never by the reference route with a reference_idx filter."""
    from qiita_control_plane.routes.reference import _REFERENCE_DOGET_TABLES

    assert ASSEMBLED_SEQUENCE_TABLE not in _REFERENCE_DOGET_TABLES
    assert ASSEMBLED_SEQUENCE_CHUNKS_TABLE not in _REFERENCE_DOGET_TABLES


@pytest.mark.parametrize("table", [ASSEMBLED_SEQUENCE_TABLE, ASSEMBLED_SEQUENCE_CHUNKS_TABLE])
async def test_doget_assembly_rejected_by_reference_route_http(ctx, table):
    """HTTP-level pin of the contract above: the constant test passes even if the
    reference route stopped consulting the allowlist, so exercise the behavior.
    The table check precedes the reference lookup, so reference_idx need not
    exist."""
    resp = await ctx["sa"].post(URL_REFERENCE_DOGET.format(reference_idx=1), json={"table": table})
    assert resp.status_code == 422, resp.text
