"""Route tests for POST /alignment/ticket/doget.

Signs an Ed25519 Flight DoGet ticket scoped to a single alignment run + its
explicit prep_sample_idx cohort on the data plane's `alignment` table, for the
feature-table (OGU) compute job. Service-account-only (Scope.TICKET_DOGET).

The route reads alignment_idx + the cohort from the work ticket's
action_context (the body carries only work_ticket_idx), so the tests seed a
work_ticket with a known action_context. These tests exercise the auth matrix
and the signed ticket's filter shape; the data-plane serve side is covered by
the DP integration test.
"""

import base64
import json
import secrets
import struct
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_ALIGNMENT_DOGET
from qiita_common.auth_constants import Scope

from qiita_control_plane.auth.token import mint_api_token

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


async def _seed_feature_table_ticket(
    pool, *, alignment_idx, prep_sample_idx, denovo_alignment_idx=None
):
    """Insert the minimal reference + action + work_ticket carrying a
    feature-table action_context. Returns (work_ticket_idx, action_id, version,
    reference_idx) for teardown."""
    ref_idx = await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', false,"
        "         (SELECT MIN(idx) FROM qiita.principal)) RETURNING reference_idx",
        f"align-doget-{uuid.uuid4()}",
    )
    action_id = "alignment-doget-test-action"
    version = f"v-{uuid.uuid4()}"
    await pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb, $5::jsonb,"
        "          1, 1, '1 minute')",
        action_id,
        version,
        ["reference:write"],
        json.dumps({"service": False, "human_roles": ["system_admin"]}),
        json.dumps([]),
    )
    wt_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state, action_context)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal),"
        "         'reference', $3, 'processing'::qiita.work_ticket_state, $4::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        ref_idx,
        json.dumps(
            {"alignment_idx": alignment_idx, "prep_sample_idx": prep_sample_idx}
            | (
                {}
                if denovo_alignment_idx is None
                else {"denovo_alignment_idx": denovo_alignment_idx}
            )
        ),
    )
    return wt_idx, action_id, version, ref_idx


async def _cleanup_ticket(pool, wt_idx, action_id, version, ref_idx):
    await pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx)
    await pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
    )
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref_idx)


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
async def aligned(postgres_pool, regular_user_session):
    """A real alignment with three prep_samples whose gates are 'completed' — what
    the route re-checks before signing."""
    from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
    from qiita_control_plane.testing.db_seeds import seed_biosample_with_sequenced_prep_sample
    from qiita_control_plane.testing.db_teardown import teardown_entity_graph

    owner = regular_user_session["principal_idx"]
    seeded = [
        await seed_biosample_with_sequenced_prep_sample(postgres_pool, owner_idx=owner)
        for _ in range(3)
    ]
    samples = [p for _b, p in seeded]
    async with postgres_pool.acquire() as conn:
        row = await mint_alignment_definition(
            conn,
            params={"reference_idx": 1, "aligner": "bowtie2", "s": secrets.token_hex(4)},
            principal_idx=owner,
        )
    alignment_idx = row["alignment_idx"]
    await postgres_pool.executemany(
        "INSERT INTO qiita.alignment_sample (alignment_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed')",
        [(alignment_idx, p) for p in samples],
    )
    yield {"alignment_idx": alignment_idx, "samples": samples, "owner": owner}
    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[],
        biosample_idxs=[b for b, _p in seeded],
        prep_sample_idxs=samples,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )


@pytest_asyncio.fixture
async def sa_no_scope_client(postgres_pool, compute_worker_service_account):
    """An SA token carrying a scope that is NOT tickets:doget, to exercise the
    require_scope 403 path."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=compute_worker_service_account["principal_idx"],
        label=f"sa-no-doget-{secrets.token_hex(4)}",
        scopes=[Scope.FEATURE_MINT],
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Auth matrix
# ---------------------------------------------------------------------------


async def test_doget_anonymous_401(ctx):
    resp = await ctx["anon"].post(URL_ALIGNMENT_DOGET, json={"work_ticket_idx": 1})
    assert resp.status_code == 401, resp.text


async def test_doget_human_user_403(ctx, postgres_pool, regular_user_session):
    """Humans can't mint even carrying the scope — require_service rejects the
    HumanUser before require_scope runs."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=regular_user_session["principal_idx"],
        label=f"human-align-doget-{secrets.token_hex(4)}",
        scopes=[Scope.SELF_PROFILE, Scope.TICKET_DOGET],
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as human:
        resp = await human.post(URL_ALIGNMENT_DOGET, json={"work_ticket_idx": 1})
    assert resp.status_code == 403, resp.text
    assert "service accounts" in resp.json()["detail"]


async def test_doget_sa_without_scope_403(ctx, sa_no_scope_client):
    resp = await sa_no_scope_client.post(URL_ALIGNMENT_DOGET, json={"work_ticket_idx": 1})
    assert resp.status_code == 403, resp.text
    assert "ticket:doget" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Signed ticket contents
# ---------------------------------------------------------------------------


async def test_doget_sa_signs_scoped_filter(ctx, aligned):
    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"], alignment_idx=aligned["alignment_idx"], prep_sample_idx=aligned["samples"]
    )
    try:
        resp = await ctx["sa"].post(
            URL_ALIGNMENT_DOGET,
            json={"work_ticket_idx": wt_idx, "columns": ["feature_idx"]},
        )
        assert resp.status_code == 201, resp.text
        payload = _decode_ticket_payload(resp.json()["ticket"])
        assert payload["table"] == "alignment_visible"
        assert payload["filter"] == {
            "alignment_idx": [aligned["alignment_idx"]],
            "prep_sample_idx": aligned["samples"],
        }
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)


async def test_doget_non_int_alignment_idx_422(ctx):
    """A wrong-typed alignment_idx (e.g. a JSON string) in action_context is not a
    valid scope — the isinstance guard rejects it with 422, never signs."""
    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"], alignment_idx="not-an-int", prep_sample_idx=[11]
    )
    try:
        resp = await ctx["sa"].post(URL_ALIGNMENT_DOGET, json={"work_ticket_idx": wt_idx})
        assert resp.status_code == 422, resp.text
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)


async def test_doget_bool_alignment_idx_422(ctx):
    """bool is an int subclass — a JSON `true` alignment_idx must not masquerade as
    an identifier; the shared scope validator rejects it with 422."""
    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"], alignment_idx=True, prep_sample_idx=[11]
    )
    try:
        resp = await ctx["sa"].post(URL_ALIGNMENT_DOGET, json={"work_ticket_idx": wt_idx})
        assert resp.status_code == 422, resp.text
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)


async def test_doget_missing_scope_keys_422(ctx):
    """A work ticket whose action_context lacks alignment_idx / prep_sample_idx
    is not a feature-table request — refuse to sign (fail loud)."""
    ref_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', false,"
        "         (SELECT MIN(idx) FROM qiita.principal)) RETURNING reference_idx",
        f"align-doget-empty-{uuid.uuid4()}",
    )
    action_id = "alignment-doget-empty-action"
    version = f"v-{uuid.uuid4()}"
    await ctx["pool"].execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb, $5::jsonb, 1, 1, '1 minute')",
        action_id,
        version,
        ["reference:write"],
        json.dumps({"service": False, "human_roles": ["system_admin"]}),
        json.dumps([]),
    )
    ticket_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state, action_context)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal),"
        "         'reference', $3, 'processing'::qiita.work_ticket_state, '{}'::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        ref_idx,
    )
    try:
        resp = await ctx["sa"].post(URL_ALIGNMENT_DOGET, json={"work_ticket_idx": ticket_idx})
        assert resp.status_code == 422, resp.text
    finally:
        await ctx["pool"].execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket_idx
        )
        await ctx["pool"].execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
        )
        await ctx["pool"].execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref_idx)


async def test_doget_work_ticket_not_found_404(ctx):
    resp = await ctx["sa"].post(URL_ALIGNMENT_DOGET, json={"work_ticket_idx": 999_999_999})
    assert resp.status_code == 404, resp.text


# ---------------------------------------------------------------------------
# CP-side allowlist consistency
# ---------------------------------------------------------------------------


async def test_doget_alignment_in_cp_allowlist():
    """The view the route signs must be in the CP-side DoGet allowlist that
    mirrors the data plane's ALLOWED_TABLES — and the raw base table must NOT be
    (reads go through the exclusion view, never the raw sink)."""
    from qiita_control_plane.routes.reference import _DOGET_ALLOWED_TABLES

    assert "alignment_visible" in _DOGET_ALLOWED_TABLES
    assert "alignment" not in _DOGET_ALLOWED_TABLES


async def test_doget_alignment_not_signable_via_reference_route():
    """alignment_visible is served only by this route (scoped by alignment_idx +
    prep_sample_idx), never by the reference route with a reference_idx filter."""
    from qiita_control_plane.routes.reference import _REFERENCE_DOGET_TABLES

    assert "alignment_visible" not in _REFERENCE_DOGET_TABLES


# ---------------------------------------------------------------------------
# Signed projection (the columns the consumer asks for)
# ---------------------------------------------------------------------------


async def test_doget_signs_the_requested_columns(ctx, aligned):
    """The consumer names its columns; the route validates and signs them.

    Includes `cigar`, the column the whole mechanism exists for: it must be
    askable without being paid for by default.
    """
    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"], alignment_idx=aligned["alignment_idx"], prep_sample_idx=aligned["samples"][:1]
    )
    try:
        resp = await ctx["sa"].post(
            URL_ALIGNMENT_DOGET,
            json={"work_ticket_idx": wt_idx, "columns": ["feature_idx", "cigar", "position"]},
        )
        assert resp.status_code == 201, resp.text
        payload = _decode_ticket_payload(resp.json()["ticket"])
        assert payload["columns"] == ["feature_idx", "cigar", "position"]
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)


async def test_doget_without_columns_is_refused_at_mint(ctx, aligned):
    """Omitting the list on a projectable table is a 422, not a signed ticket.

    The data plane requires the list for this table, so a ticket signed without
    one can only fail at DoGet — one hop later, at a client already holding
    something the server told it was valid. Both halves of that rule live at the
    signing boundary so the caller learns here.
    """
    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"], alignment_idx=aligned["alignment_idx"], prep_sample_idx=aligned["samples"][:1]
    )
    try:
        resp = await ctx["sa"].post(URL_ALIGNMENT_DOGET, json={"work_ticket_idx": wt_idx})
        assert resp.status_code == 422, resp.text
        assert "requires a projection column list" in resp.text
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param(["feature_idx", "no_such_column"], id="unknown"),
        pytest.param(["feature_idx", "feature_idx"], id="duplicate"),
        pytest.param(["cigar; DROP TABLE alignment"], id="injection-shaped"),
        pytest.param([], id="empty"),
    ],
)
async def test_doget_bad_column_list_422(ctx, aligned, columns):
    """A bad projection is refused at mint and never signed.

    The empty case is the one that could not be caught downstream: on the wire
    an empty list is indistinguishable from an absent one, so this route is the
    last place it can be told apart from "no opinion".
    """
    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"], alignment_idx=aligned["alignment_idx"], prep_sample_idx=aligned["samples"][:1]
    )
    try:
        resp = await ctx["sa"].post(
            URL_ALIGNMENT_DOGET, json={"work_ticket_idx": wt_idx, "columns": columns}
        )
        assert resp.status_code == 422, resp.text
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)


# ---------------------------------------------------------------------------
# Withdrawn since submit
# ---------------------------------------------------------------------------


async def test_doget_refuses_a_cohort_invalidated_since_submit(ctx, aligned):
    """Submit validated a completed cohort; a sample invalidated afterwards is
    refused here rather than streamed."""
    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"], alignment_idx=aligned["alignment_idx"], prep_sample_idx=aligned["samples"]
    )
    await ctx["pool"].execute(
        "UPDATE qiita.alignment_sample SET state = 'invalidated', invalidated_at = now(),"
        "       invalidated_by_idx = $3, invalidation_reason = 'bad run'"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        aligned["alignment_idx"],
        aligned["samples"][1],
        aligned["owner"],
    )
    try:
        resp = await ctx["sa"].post(
            URL_ALIGNMENT_DOGET, json={"work_ticket_idx": wt_idx, "columns": ["feature_idx"]}
        )
        assert resp.status_code == 409, resp.text
        assert str(aligned["samples"][1]) in resp.json()["detail"]
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)


async def test_doget_refuses_an_alignment_deprecated_since_submit(ctx, aligned):
    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"], alignment_idx=aligned["alignment_idx"], prep_sample_idx=aligned["samples"]
    )
    await ctx["pool"].execute(
        "UPDATE qiita.alignment_definition SET status = 'deprecated', deprecated_at = now(),"
        "       deprecated_by_idx = $2, deprecation_reason = 'bad build'"
        " WHERE alignment_idx = $1",
        aligned["alignment_idx"],
        aligned["owner"],
    )
    try:
        resp = await ctx["sa"].post(
            URL_ALIGNMENT_DOGET, json={"work_ticket_idx": wt_idx, "columns": ["feature_idx"]}
        )
        assert resp.status_code == 409, resp.text
        assert "bad build" in resp.json()["detail"]
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)


@pytest_asyncio.fixture
async def denovo(postgres_pool, aligned):
    """A de novo alignment paired with `aligned`, over an assembly run in which the
    first and last prep_samples assembled and the middle one had nothing to assemble
    (`no_data`), so it has no de novo gate row."""
    from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition

    a, b, c = aligned["samples"]
    processing_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.processing (params_hash, workflow, version, params)"
        " VALUES ($1, 'long-read-assembly', '1.0.0', '{}'::jsonb) RETURNING processing_idx",
        uuid.uuid4().bytes + uuid.uuid4().bytes,
    )
    async with postgres_pool.acquire() as conn:
        row = await mint_alignment_definition(
            conn,
            # No mask_idx, like `aligned`'s params: the two arms must agree on it.
            params={"subject": "assembly", "aligner": "minimap2", "processing_idx": processing_idx},
            principal_idx=aligned["owner"],
        )
    denovo_alignment_idx = row["alignment_idx"]
    await postgres_pool.executemany(
        "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, $3)",
        [
            (processing_idx, a, "completed"),
            (processing_idx, b, "no_data"),
            (processing_idx, c, "completed"),
        ],
    )
    await postgres_pool.executemany(
        "INSERT INTO qiita.alignment_sample (alignment_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed')",
        [(denovo_alignment_idx, a), (denovo_alignment_idx, c)],
    )
    yield {"alignment_idx": denovo_alignment_idx, "processing_idx": processing_idx}
    # Runs before `aligned`'s entity sweep, and the definition and the run below
    # cannot be deleted while gate rows reference them, so their gate rows go first.
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_sample WHERE alignment_idx = $1", denovo_alignment_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", denovo_alignment_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.assembly_sample WHERE processing_idx = $1", processing_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = $1", processing_idx
    )


async def test_doget_denovo_arm_signs_a_cohort_with_a_no_data_member(ctx, aligned, denovo):
    """Submit expects a de novo row only for the assembled members; a `no_data` one is
    reference-only. The re-check at mint applies the same split, so the cohort is
    signed rather than refused for the member that never had a de novo row."""
    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"],
        alignment_idx=aligned["alignment_idx"],
        prep_sample_idx=aligned["samples"],
        denovo_alignment_idx=denovo["alignment_idx"],
    )
    try:
        resp = await ctx["sa"].post(
            URL_ALIGNMENT_DOGET,
            json={"work_ticket_idx": wt_idx, "columns": ["feature_idx"], "denovo": True},
        )
        assert resp.status_code == 201, resp.text
        payload = _decode_ticket_payload(resp.json()["ticket"])
        assert str(denovo["alignment_idx"]) in json.dumps(payload)
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)


async def test_doget_denovo_arm_refuses_an_assembled_member_invalidated_since_submit(
    ctx, aligned, denovo
):
    a, _b, c = aligned["samples"]
    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"],
        alignment_idx=aligned["alignment_idx"],
        prep_sample_idx=aligned["samples"],
        denovo_alignment_idx=denovo["alignment_idx"],
    )
    await ctx["pool"].execute(
        "UPDATE qiita.alignment_sample SET state = 'invalidated', invalidated_at = now(),"
        "       invalidated_by_idx = $3, invalidation_reason = 'bad run'"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        denovo["alignment_idx"],
        c,
        aligned["owner"],
    )
    try:
        resp = await ctx["sa"].post(
            URL_ALIGNMENT_DOGET,
            json={"work_ticket_idx": wt_idx, "columns": ["feature_idx"], "denovo": True},
        )
        assert resp.status_code == 409, resp.text
        assert str(c) in resp.json()["detail"]
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)


async def test_doget_denovo_arm_refuses_an_assembly_run_deprecated_since_submit(
    ctx, aligned, denovo
):
    """Submit refuses a deprecated assembly run as the de novo arm; the re-check at
    mint refuses one deprecated after submit, with the same wording."""
    from qiita_common.models import ProcessingStatus

    from qiita_control_plane.repositories.processing import transition_processing_status

    wt_idx, *rest = await _seed_feature_table_ticket(
        ctx["pool"],
        alignment_idx=aligned["alignment_idx"],
        prep_sample_idx=aligned["samples"],
        denovo_alignment_idx=denovo["alignment_idx"],
    )
    await transition_processing_status(
        ctx["pool"],
        processing_idx=denovo["processing_idx"],
        status=ProcessingStatus.DEPRECATED,
        reason="bad assembler build",
        superseded_by=None,
        principal_idx=aligned["owner"],
    )
    try:
        resp = await ctx["sa"].post(
            URL_ALIGNMENT_DOGET,
            json={"work_ticket_idx": wt_idx, "columns": ["feature_idx"], "denovo": True},
        )
        assert resp.status_code == 409, resp.text
        assert str(denovo["processing_idx"]) in resp.json()["detail"]
    finally:
        await _cleanup_ticket(ctx["pool"], wt_idx, *rest)
