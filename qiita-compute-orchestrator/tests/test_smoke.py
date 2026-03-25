import qiita_common
from fastapi.testclient import TestClient

from qiita_compute_orchestrator.main import app

client = TestClient(app)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "qiita-compute-orchestrator"


def test_dependencies_importable():
    assert qiita_common is not None
