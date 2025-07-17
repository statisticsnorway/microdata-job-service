# TODO: move to integration
from pytest_mock import MockFixture

from job_service.adapter.db import CLIENT
from fastapi.testclient import TestClient

from job_service.app import app

client = TestClient(app)


MAINTENANCE_STATUS_REQUEST_VALID = {"msg": "we upgrade chill", "paused": True}
MAINTENANCE_STATUS_REQUEST_NO_MSG = {"paused": True}
MAINTENANCE_STATUS_REQUEST_INVALID_PAUSE_VALUE = {
    "msg": "we upgrade chill",
    "paused": "Should not be a string",
}

NEW_STATUS = {
    "msg": "we upgrade chill",
    "paused": 1,
    "timestamp": "2023-08-31 16:26:27.575276",
}

RESPONSE_FROM_DB = [
    {
        "msg": "Today is 2023-08-31, we need to upgrade again",
        "paused": 1,
        "timestamp": "2023-08-31 16:26:27.575276",
    },
    {
        "msg": "Today is 2023-08-30, finished upgrading",
        "paused": 0,
        "timestamp": "2023-08-30 16:26:27.575276",
    },
    {
        "msg": "Today is 2023-08-29, we need to upgrade",
        "paused": 1,
        "timestamp": "2023-08-29 16:26:27.575276",
    },
]


def test_set_maintenance_status(mocker: MockFixture):
    db_set_status = mocker.patch.object(
        CLIENT, "set_maintenance_status", return_value=NEW_STATUS
    )
    response = client.post(
        "/maintenance-status",
        json=MAINTENANCE_STATUS_REQUEST_VALID,
    )

    db_set_status.assert_called_once()
    assert response.status_code == 200
    assert response.json() == NEW_STATUS


def test_set_maintenance_status_with_no_msg(mocker: MockFixture):
    db_set_status = mocker.patch.object(CLIENT, "set_maintenance_status")
    response = client.post(
        "/maintenance-status",
        json=MAINTENANCE_STATUS_REQUEST_NO_MSG,
    )

    db_set_status.assert_not_called()
    assert response.status_code == 400
    assert response.json().get("details") is not None


def test_set_maintenance_status_with_invalid_paused(mocker: MockFixture):
    db_set_status = mocker.patch.object(CLIENT, "set_maintenance_status")
    response = client.post(
        "/maintenance-status",
        json=MAINTENANCE_STATUS_REQUEST_INVALID_PAUSE_VALUE,
    )

    db_set_status.assert_not_called()
    assert response.status_code == 400
    assert response.json().get("details") is not None


def test_get_maintenance_status(mocker: MockFixture, caplog):
    get_status = mocker.patch.object(
        CLIENT,
        "get_latest_maintenance_status",
        return_value=RESPONSE_FROM_DB[0],
    )

    response = client.get("/maintenance-status")

    get_status.assert_called_once()
    assert response.status_code == 200
    assert response.json() == RESPONSE_FROM_DB[0]
    assert (
        "GET /maintenance-status, paused: 1, "
        "msg: Today is 2023-08-31, we need to upgrade again\n" in caplog.text
    )


def test_get_maintenance_status_from_empty_collection(mocker: MockFixture):
    get_status = mocker.patch.object(
        CLIENT, "get_latest_maintenance_status", return_value={}
    )

    response = client.get("/maintenance-status")

    get_status.assert_called_once()
    assert response.status_code == 200
    assert response.json() == {}


def test_get_maintenance_history(mocker: MockFixture):
    get_history = mocker.patch.object(
        CLIENT, "get_maintenance_history", return_value=RESPONSE_FROM_DB
    )
    response = client.get("/maintenance-history")
    get_history.assert_called_once()

    assert response.status_code == 200
    assert response.json() == RESPONSE_FROM_DB


def test_get_maintenance_history_from_empty_collection(mocker: MockFixture):
    get_history = mocker.patch.object(
        CLIENT, "get_maintenance_history", return_value=[]
    )
    response = client.get("/maintenance-history")
    get_history.assert_called_once()

    assert response.status_code == 200
    assert response.json() == []
