"""Integration tests for POST /api/v1/sequencing-run/{idx}/sequenced-pool.

Exercises happy paths under wet_lab_admin and system_admin, the scope
guard, the caller-creator guard on the path's sequencing_run
(`require_caller_owns_run`, wet_lab_admin+ bypass), Pydantic body
validation, the 404 on a missing parent sequencing_run, round-trip
byte-equality of the run_preflight_blob via base64, and the
both-or-neither nullability of the (blob, filename) preflight pair.
"""

import base64
import json

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_SEQUENCING_RUN_SEQUENCED_POOL

from qiita_control_plane.main import app

from .conftest import delete_idxs, unique_instrument_id

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# FK-reverse cleanup
# ---------------------------------------------------------------------------


async def _cleanup_tracked(pool, created: dict) -> None:
    """Drop tracked rows in FK-reverse order: sequenced_pool then
    sequencing_run."""
    await delete_idxs(pool, "sequenced_pool", created["sequenced_pool"])
    await delete_idxs(pool, "sequencing_run", created["sequencing_run"])


@pytest_asyncio.fixture
async def ctx(role_keyed_clients):
    """Per-test fixture: route-keyed AsyncClient triple plus a `created`
    tracker for FK-reverse teardown."""
    created: dict = {"sequencing_run": [], "sequenced_pool": []}
    yield {**role_keyed_clients, "created": created}
    await _cleanup_tracked(role_keyed_clients["pool"], created)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_sequencing_run(ctx, suffix: str) -> int:
    """Insert a minimal sequencing_run row owned by the wet_lab_admin
    session, track for cleanup, and return its idx."""
    idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        unique_instrument_id(suffix),
        ctx["wet_session"]["principal_idx"],
    )
    ctx["created"]["sequencing_run"].append(idx)
    return idx


def _b64(blob: bytes) -> str:
    """Encode a bytes value for a JSON body field of type bytes (Pydantic
    decodes base64 automatically on the receiving side)."""
    return base64.b64encode(blob).decode("ascii")


async def _post_pool(client, ctx, sequencing_run_idx: int, **body):
    """POST the route and, on 201, track the created sequenced_pool idx."""
    resp = await client.post(
        URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=sequencing_run_idx),
        json=body,
    )
    if resp.status_code == 201:
        ctx["created"]["sequenced_pool"].append(resp.json()["sequenced_pool_idx"])
    return resp


# ===========================================================================
# Happy paths
# ===========================================================================


async def test_create_sequenced_pool_wet_lab_admin_minimal(ctx):
    # Minimal happy path: required blob + filename only. Verifies the
    # blob round-trips bytes-identical via base64 + BYTEA.
    run_idx = await _seed_sequencing_run(ctx, "wet")
    blob = b"\x00\x01\x02SQLite-magic-bytes-and-more\xff\xfe"
    resp = await _post_pool(
        ctx["wet"],
        ctx,
        run_idx,
        run_preflight_blob=_b64(blob),
        run_preflight_filename="run-preflight.sqlite",
    )
    assert resp.status_code == 201, resp.text
    rj = resp.json()
    expected = {
        # Auto-generated; copy actual into expected so the equality
        # confirms field presence without pinning the idx value.
        "sequenced_pool_idx": rj["sequenced_pool_idx"],
    }
    assert rj == expected

    # Round-trip check: BYTEA stored byte-identical, filename matches,
    # FK back to the parent run is intact.
    row = await ctx["pool"].fetchrow(
        "SELECT sequencing_run_idx, run_preflight_blob, run_preflight_filename,"
        " extra_metadata, created_by_idx"
        " FROM qiita.sequenced_pool WHERE idx = $1",
        rj["sequenced_pool_idx"],
    )
    # asyncpg returns BYTEA as a memoryview; coerce to bytes for equality.
    actual = dict(row)
    actual["run_preflight_blob"] = bytes(actual["run_preflight_blob"])
    expected = {
        "sequencing_run_idx": run_idx,
        "run_preflight_blob": blob,
        "run_preflight_filename": "run-preflight.sqlite",
        "extra_metadata": None,
        "created_by_idx": ctx["wet_session"]["principal_idx"],
    }
    assert actual == expected


async def test_create_sequenced_pool_system_admin_with_extra_metadata(ctx):
    # System_admin posts with extra_metadata; verifies the JSONB column
    # round-trips the supplied dict.
    run_idx = await _seed_sequencing_run(ctx, "adm")
    resp = await _post_pool(
        ctx["admin"],
        ctx,
        run_idx,
        run_preflight_blob=_b64(b"ABC"),
        run_preflight_filename="lane1.sqlite",
        extra_metadata={"lane": 1, "tag": "ADM"},
    )
    assert resp.status_code == 201, resp.text
    row = await ctx["pool"].fetchrow(
        "SELECT extra_metadata FROM qiita.sequenced_pool WHERE idx = $1",
        resp.json()["sequenced_pool_idx"],
    )
    assert json.loads(row["extra_metadata"]) == {"lane": 1, "tag": "ADM"}


# ===========================================================================
# Auth / scope / role guards
# ===========================================================================


async def test_create_sequenced_pool_anonymous_401(ctx):
    # No Authorization header → require_complete_profile chain raises 401.
    app.state.pool = ctx["pool"]
    run_idx = await _seed_sequencing_run(ctx, "anon")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(
            URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=run_idx),
            json={
                "run_preflight_blob": _b64(b"X"),
                "run_preflight_filename": "f.sqlite",
            },
        )
    assert resp.status_code == 401


async def test_create_sequenced_pool_missing_scope_403(ctx, no_prep_sample_write_client):
    # PAT omits Scope.PREP_SAMPLE_WRITE → require_scope rejects with 403.
    run_idx = await _seed_sequencing_run(ctx, "noscope")
    resp = await no_prep_sample_write_client.post(
        URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=run_idx),
        json={
            "run_preflight_blob": _b64(b"X"),
            "run_preflight_filename": "f.sqlite",
        },
    )
    assert resp.status_code == 403
    assert "prep_sample:write" in resp.json()["detail"]


async def test_create_sequenced_pool_regular_user_not_creator_403(ctx):
    # Regular user attempts to attach a pool to a run created by the
    # wet_lab_admin (see `_seed_sequencing_run`). require_scope passes
    # (prep_sample:write is in the USER ceiling) but
    # require_caller_owns_run() rejects with 403 because the caller is
    # not the run's `created_by_idx`. The 403 detail names the run so a
    # client can see *why* it was denied.
    run_idx = await _seed_sequencing_run(ctx, "user")
    resp = await _post_pool(
        ctx["user"],
        ctx,
        run_idx,
        run_preflight_blob=_b64(b"X"),
        run_preflight_filename="f.sqlite",
    )
    assert resp.status_code == 403
    assert f"sequencing_run {run_idx}" in resp.json()["detail"]


async def test_create_sequenced_pool_regular_user_creator_passes(ctx):
    # Regular user creates their own run, then posts a pool against it;
    # require_caller_owns_run sees them as the creator and the create
    # succeeds. Pins the end-to-end user-CLI flow.
    run_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        unique_instrument_id("user-creator"),
        ctx["user_session"]["principal_idx"],
    )
    ctx["created"]["sequencing_run"].append(run_idx)

    resp = await _post_pool(
        ctx["user"],
        ctx,
        run_idx,
        run_preflight_blob=_b64(b"\x00\x01"),
        run_preflight_filename="user.sqlite",
    )
    assert resp.status_code == 201, resp.text


# ===========================================================================
# Path / data validation
# ===========================================================================


async def test_create_sequenced_pool_nonexistent_run_404(ctx):
    # The pre-flight existence check fires before the write transaction;
    # a run idx past the highest existing row returns 404.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.sequencing_run")
    resp = await ctx["wet"].post(
        URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=max_idx + 100_000),
        json={
            "run_preflight_blob": _b64(b"X"),
            "run_preflight_filename": "f.sqlite",
        },
    )
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


async def test_create_sequenced_pool_empty_blob_422(ctx):
    # The Pydantic min_length=1 on `bytes` rejects an empty blob; the
    # zero-byte base64 encoding "" trips the validator without reaching SQL.
    run_idx = await _seed_sequencing_run(ctx, "empty")
    resp = await ctx["wet"].post(
        URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=run_idx),
        json={
            "run_preflight_blob": "",
            "run_preflight_filename": "f.sqlite",
        },
    )
    assert resp.status_code == 422


async def test_create_sequenced_pool_blob_without_filename_422(ctx):
    # The run preflight is a co-populated pair: a blob with no filename
    # trips the both-or-neither model_validator (422) before reaching SQL.
    run_idx = await _seed_sequencing_run(ctx, "nofile")
    resp = await ctx["wet"].post(
        URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=run_idx),
        json={"run_preflight_blob": _b64(b"X")},
    )
    assert resp.status_code == 422


async def test_create_sequenced_pool_filename_without_blob_422(ctx):
    # Symmetric to the blob-only case: a filename with no blob trips the
    # both-or-neither model_validator (422).
    run_idx = await _seed_sequencing_run(ctx, "noblob")
    resp = await ctx["wet"].post(
        URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=run_idx),
        json={"run_preflight_filename": "f.sqlite"},
    )
    assert resp.status_code == 422


async def test_create_sequenced_pool_empty_filename_422(ctx):
    # min_length=1 rejects an empty filename even when a blob is present;
    # the validator trips without reaching SQL.
    run_idx = await _seed_sequencing_run(ctx, "emptyfn")
    resp = await ctx["wet"].post(
        URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=run_idx),
        json={
            "run_preflight_blob": _b64(b"X"),
            "run_preflight_filename": "",
        },
    )
    assert resp.status_code == 422


async def test_create_sequenced_pool_no_preflight_201(ctx):
    # Neither blob nor filename: a pool with no preflight is valid. Both
    # columns land NULL; the row round-trips by full-object equality.
    run_idx = await _seed_sequencing_run(ctx, "noprefl")
    resp = await _post_pool(
        ctx["wet"],
        ctx,
        run_idx,
        extra_metadata={"lane": 2},
    )
    assert resp.status_code == 201, resp.text
    row = await ctx["pool"].fetchrow(
        "SELECT sequencing_run_idx, run_preflight_blob, run_preflight_filename,"
        " extra_metadata, created_by_idx"
        " FROM qiita.sequenced_pool WHERE idx = $1",
        resp.json()["sequenced_pool_idx"],
    )
    actual = dict(row)
    actual["extra_metadata"] = json.loads(actual["extra_metadata"])
    expected = {
        "sequencing_run_idx": run_idx,
        "run_preflight_blob": None,
        "run_preflight_filename": None,
        "extra_metadata": {"lane": 2},
        "created_by_idx": ctx["wet_session"]["principal_idx"],
    }
    assert actual == expected


async def test_create_sequenced_pool_extra_field_422(ctx):
    # The request model carries extra="forbid"; an unknown field fails the
    # Pydantic validation rather than being silently dropped.
    run_idx = await _seed_sequencing_run(ctx, "xtra")
    resp = await ctx["wet"].post(
        URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=run_idx),
        json={
            "run_preflight_blob": _b64(b"X"),
            "run_preflight_filename": "f.sqlite",
            "lane": 1,
        },
    )
    assert resp.status_code == 422
