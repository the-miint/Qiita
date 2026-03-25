from fastapi.testclient import TestClient

from qiita_control_plane.main import app

client = TestClient(app)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "qiita-control-plane"
