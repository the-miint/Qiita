"""Route tests for GET /admin/study/{study_idx}/owner-biosample-id — the
owner-id re-identification export.

Covers the study-wide export, the pool-filtered variant (adds prep_sample_idx
+ ENA accessions and restricts to the pool's samples in the study), the
NULL-accession / NULL-owner-id surfacing, and the auth gates (401 anonymous,
403 regular user, 403 system_admin lacking the scope, 404 unknown study/pool).
Reuses the shared role-keyed clients fixture from tests/routes/conftest.py.
"""

import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_ADMIN_STUDY_OWNER_BIOSAMPLE_ID
from qiita_common.auth_constants import Scope

from qiita_control_plane.main import app
from qiita_control_plane.testing.db_seeds import (
    seed_biosample,
    seed_biosample_to_study_link,
    seed_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
)

pytestmark = pytest.mark.db


@pytest.fixture
def ctx(role_keyed_clients):
    """Alias the shared role-keyed clients ({pool, admin, user, *_session})."""
    return role_keyed_clients


def _url(study_idx: int) -> str:
    return URL_ADMIN_STUDY_OWNER_BIOSAMPLE_ID.format(study_idx=study_idx)


async def _seed_owner_id(pool, *, biosample_idx, field_idx, owner_name, creator_idx) -> None:
    """Attach an owner-biosample-id metadata row (value_text=owner_name,
    is_owner_biosample_id=true) on the shared study-local field."""
    await pool.execute(
        "INSERT INTO qiita.biosample_metadata"
        "  (biosample_idx, biosample_study_field_idx, value_text,"
        "   is_owner_biosample_id, created_by_idx)"
        " VALUES ($1, $2, $3, true, $4)",
        biosample_idx,
        field_idx,
        owner_name,
        creator_idx,
    )


@pytest_asyncio.fixture
async def seeded(ctx):
    """Seed a study with two biosamples carrying owner ids:

      - bs_a: has a biosample_accession, plus a sequenced prep_sample in a
        pool (with ENA experiment/run accessions) and a prep_sample_to_study
        link — so it appears in both the study-wide and pool-filtered exports.
      - bs_b: no accession, no prep_sample — study-wide export only.

    FK-reverse cleanup at teardown; the owning principal is the admin session
    principal (user-kind, cleaned by its own fixture).
    """
    pool = ctx["pool"]
    owner = ctx["admin_session"]["principal_idx"]
    token = secrets.token_hex(4)

    study_idx = await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner,
        f"owner-id-test-{token}",
    )
    # One study-local field holds the owner id for every biosample in the study.
    field_idx = await pool.fetchval(
        "INSERT INTO qiita.biosample_study_field"
        "  (study_idx, display_name, data_type, required, created_by_idx)"
        " VALUES ($1, $2, 'text'::qiita.field_data_type, true, $3) RETURNING idx",
        study_idx,
        f"owner_id_{token}",
        owner,
    )

    bs_a = await seed_biosample(pool, owner_idx=owner, created_by_idx=owner)
    accession_a = f"SAMN-A-{token}"
    await pool.execute(
        "UPDATE qiita.biosample SET biosample_accession = $2 WHERE idx = $1", bs_a, accession_a
    )
    await seed_biosample_to_study_link(
        pool, biosample_idx=bs_a, study_idx=study_idx, created_by_idx=owner
    )
    await _seed_owner_id(
        pool, biosample_idx=bs_a, field_idx=field_idx, owner_name="OWNER-A", creator_idx=owner
    )

    bs_b = await seed_biosample(pool, owner_idx=owner, created_by_idx=owner)
    await seed_biosample_to_study_link(
        pool, biosample_idx=bs_b, study_idx=study_idx, created_by_idx=owner
    )
    await _seed_owner_id(
        pool, biosample_idx=bs_b, field_idx=field_idx, owner_name="OWNER-B", creator_idx=owner
    )

    # Sequencing pathway on bs_a only.
    ps_a = await seed_sequenced_prep_sample(pool, biosample_idx=bs_a, owner_idx=owner)
    await pool.execute(
        "INSERT INTO qiita.prep_sample_to_study (prep_sample_idx, study_idx, created_by_idx)"
        " VALUES ($1, $2, $3)",
        ps_a,
        study_idx,
        owner,
    )
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        pool, prep_sample_idx=ps_a, owner_idx=owner, sequenced_pool_item_id=f"item-{token}"
    )
    exp_acc = f"ERX-{token}"
    run_acc = f"ERR-{token}"
    await pool.execute(
        "UPDATE qiita.sequenced_sample"
        " SET ena_experiment_accession = $2, ena_run_accession = $3 WHERE idx = $1",
        ss_idx,
        exp_acc,
        run_acc,
    )

    yield {
        "study_idx": study_idx,
        "bs_a": bs_a,
        "bs_b": bs_b,
        "accession_a": accession_a,
        "ps_a": ps_a,
        "pool_idx": pool_idx,
        "exp_acc": exp_acc,
        "run_acc": run_acc,
    }

    await pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await pool.execute("DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = $1", ps_a)
    await pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps_a)
    await pool.execute(
        "DELETE FROM qiita.biosample_metadata WHERE biosample_idx = ANY($1::bigint[])",
        [bs_a, bs_b],
    )
    await pool.execute("DELETE FROM qiita.biosample_study_field WHERE idx = $1", field_idx)
    await pool.execute("DELETE FROM qiita.biosample_to_study WHERE study_idx = $1", study_idx)
    await pool.execute("DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [bs_a, bs_b])
    await pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)


async def test_study_scope_export(ctx, seeded):
    """Study-wide export: one row per active biosample link, accession +
    owner name; the pathway columns are absent (None)."""
    resp = await ctx["admin"].get(_url(seeded["study_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["study_idx"] == seeded["study_idx"]
    assert body["sequenced_pool_idx"] is None
    assert body["row_count"] == 2
    by_idx = {r["biosample_idx"]: r for r in body["rows"]}

    row_a = by_idx[seeded["bs_a"]]
    assert row_a["biosample_accession"] == seeded["accession_a"]
    assert row_a["owner_biosample_id"] == "OWNER-A"
    assert row_a["prep_sample_idx"] is None
    assert row_a["ena_experiment_accession"] is None

    row_b = by_idx[seeded["bs_b"]]
    # No accession yet (not submitted) — surfaced as null, biosample still listed.
    assert row_b["biosample_accession"] is None
    assert row_b["owner_biosample_id"] == "OWNER-B"


async def test_pool_filtered_export(ctx, seeded):
    """Pool filter: restricts to the pool's samples in the study and carries
    prep_sample_idx + ENA accessions. bs_b (not in the pool) is excluded."""
    resp = await ctx["admin"].get(
        _url(seeded["study_idx"]), params={"sequenced_pool_idx": seeded["pool_idx"]}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequenced_pool_idx"] == seeded["pool_idx"]
    assert body["row_count"] == 1
    row = body["rows"][0]
    assert row["biosample_idx"] == seeded["bs_a"]
    assert row["prep_sample_idx"] == seeded["ps_a"]
    assert row["ena_experiment_accession"] == seeded["exp_acc"]
    assert row["ena_run_accession"] == seeded["run_acc"]
    assert row["owner_biosample_id"] == "OWNER-A"


async def test_regular_user_403(ctx, seeded):
    resp = await ctx["user"].get(_url(seeded["study_idx"]))
    assert resp.status_code == 403


async def test_system_admin_missing_scope_403(ctx, seeded):
    """A system_admin token that lacks admin:biosample_owner_id_read is rejected
    by the scope gate even though the role gate passes."""
    from qiita_control_plane.auth.token import mint_api_token

    token, _ = await mint_api_token(
        ctx["pool"],
        principal_idx=ctx["admin_session"]["principal_idx"],
        label="admin-without-owner-id-scope",
        scopes=[Scope.SELF_PROFILE],
    )
    resp = await ctx["admin"].get(
        _url(seeded["study_idx"]), headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 403


async def test_anonymous_401(ctx, seeded):
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(_url(seeded["study_idx"]))
    assert resp.status_code == 401


async def test_unknown_study_404(ctx, seeded):
    resp = await ctx["admin"].get(_url(999_999_999))
    assert resp.status_code == 404


async def test_unknown_pool_404(ctx, seeded):
    resp = await ctx["admin"].get(
        _url(seeded["study_idx"]), params={"sequenced_pool_idx": 999_999_999}
    )
    assert resp.status_code == 404
