"""Tests for DELETE /api/v1/reference-artifact/{reference_idx}.

The control-plane reference-delete flow calls this so the orchestrator can
remove a reference's on-disk index artifacts under PATH_DERIVED. The endpoint
is a synchronous, idempotent rmtree — these tests cover the bearer-token gate,
the populated-directory delete, and the missing-directory no-op.
"""

import dataclasses

import pytest
from fastapi.testclient import TestClient
from qiita_common.api_paths import URL_REFERENCE_ARTIFACT_BY_IDX

from qiita_compute_orchestrator.main import app


@pytest.fixture
def client(tmp_path):
    """TestClient with path_derived pointed at a tmp dir so the rmtree is
    sandboxed. Overrides the lifespan-resolved settings after startup."""
    with TestClient(app) as c:
        app.state.settings = dataclasses.replace(app.state.settings, path_derived=str(tmp_path))
        yield c, tmp_path


def _ref_dir(root, reference_idx):
    d = root / "references" / str(reference_idx)
    (d / "rype" / "index.ryxdi").mkdir(parents=True)
    (d / "rype" / "index.ryxdi" / "manifest.toml").write_text("k = 15\n")
    (d / "minimap2").mkdir(parents=True)
    (d / "minimap2" / "index.mmi").write_text("binary")
    return d


def test_purge_requires_bearer_token(client):
    c, _ = client
    resp = c.request("DELETE", URL_REFERENCE_ARTIFACT_BY_IDX.format(reference_idx=1))
    assert resp.status_code == 401


def test_purge_removes_populated_directory(client, cp_to_co_token):
    c, root = client
    ref_dir = _ref_dir(root, 42)
    assert ref_dir.exists()

    resp = c.request(
        "DELETE",
        URL_REFERENCE_ARTIFACT_BY_IDX.format(reference_idx=42),
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["reference_idx"] == 42
    assert body["removed"] is True
    assert body["path"].endswith("references/42")
    assert not ref_dir.exists()


def test_purge_missing_directory_is_noop(client, cp_to_co_token):
    c, _ = client
    resp = c.request(
        "DELETE",
        URL_REFERENCE_ARTIFACT_BY_IDX.format(reference_idx=999),
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["removed"] is False
