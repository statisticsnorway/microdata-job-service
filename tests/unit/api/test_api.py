from job_service.app import app
from fastapi.testclient import TestClient

client = TestClient(app)


def test_client_sends_x_request_id():
    response = client.get("/health/alive", headers={"X-Request-ID": "abc123"})
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "abc123"


def test_client_does_not_send_x_request_id():
    response = client.get("/health/alive")
    assert response.status_code == 200
    # Check that header is set (non-empty)
    assert "X-Request-ID" in response.headers
    assert response.headers["X-Request-ID"] != ""

