from flask import Response, url_for


def test_client_sends_x_request_id(flask_app):
    response: Response = flask_app.get(
        url_for("observability.alive"), headers={"X-Request-ID": "abc123"}
    )
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "abc123"


def test_client_does_not_send_x_request_id(flask_app):
    response: Response = flask_app.get(url_for("observability.alive"))
    assert response.status_code == 200
    assert response.headers["X-Request-ID"]
