from flask import url_for
from pytest_mock import MockFixture

from job_service.adapter import maintenance_db

MAINTENANCE_STATUS_REQUEST_VALID = {"msg": "we upgrade chill", "paused": True}
MAINTENANCE_STATUS_REQUEST_NO_MSG = {"paused": True}
MAINTENANCE_STATUS_REQUEST_INVALID_PAUSE_VALUE = {
    "msg": "we upgrade chill",
    "paused": "Should not be a string",
}

RESPONSE_FROM_DB = [
    {
        "_id": "64ee001303a2f9d32f549e0d",
        "msg": "Today is 2023-08-31, we need to upgrade again",
        "paused": 1,
        "timestamp": "2023-08-31 16:26:27.575276",
    },
    {
        "_id": "64ee001303a2f9d32f549e0d",
        "msg": "Today is 2023-08-30, finished upgrading",
        "paused": 0,
        "timestamp": "2023-08-30 16:26:27.575276",
    },
    {
        "_id": "64ee001303a2f9d32f549e0d",
        "msg": "Today is 2023-08-29, we need to upgrade",
        "paused": 1,
        "timestamp": "2023-08-29 16:26:27.575276",
    },
]


def test_set_maintenance_status(flask_app, mocker: MockFixture):
    db_set_status = mocker.patch.object(maintenance_db, "set_status")
    response = flask_app.post(
        url_for("maintenance_api.set_status"),
        json=MAINTENANCE_STATUS_REQUEST_VALID,
    )

    db_set_status.assert_called_once()
    assert response.status_code == 200

    assert response.json == {
        "status": "SUCCESS",
        "msg": "we upgrade chill",
        "paused": 1,
    }


def test_set_maintenance_status_with_no_msg(flask_app, mocker: MockFixture):
    db_set_status = mocker.patch.object(maintenance_db, "set_status")
    response = flask_app.post(
        url_for("maintenance_api.set_status"),
        json=MAINTENANCE_STATUS_REQUEST_NO_MSG,
    )

    db_set_status.assert_not_called()
    assert response.status_code == 400
    assert "validation_error" in response.json


def test_set_maintenance_status_with_invalid_paused(
        flask_app, mocker: MockFixture
):
    db_set_status = mocker.patch.object(maintenance_db, "set_status")
    response = flask_app.post(
        url_for("maintenance_api.set_status"),
        json=MAINTENANCE_STATUS_REQUEST_INVALID_PAUSE_VALUE,
    )

    db_set_status.assert_not_called()
    assert response.status_code == 400
    assert "validation_error" in response.json


def test_get_maintenance_status(flask_app, mocker: MockFixture):
    get_status = mocker.patch.object(
        maintenance_db, "get_latest_status", return_value=RESPONSE_FROM_DB[0]
    )

    response = flask_app.get(url_for("maintenance_api.get_status"))

    get_status.assert_called_once()
    assert response.status_code == 200
    assert response.json == RESPONSE_FROM_DB[0]


def test_get_maintenance_status_from_empty_collection(flask_app, mocker: MockFixture):
    get_status = mocker.patch.object(
        maintenance_db, "get_latest_status", return_value={}
    )

    response = flask_app.get(url_for("maintenance_api.get_status"))

    get_status.assert_called_once()
    assert response.status_code == 200
    assert response.json == {}


def test_get_maintenance_history(flask_app, mocker: MockFixture):
    get_history = mocker.patch.object(
        maintenance_db, "get_history", return_value=RESPONSE_FROM_DB
    )
    response = flask_app.get(url_for("maintenance_api.get_history"))
    get_history.assert_called_once()

    assert response.status_code == 200
    assert response.json == RESPONSE_FROM_DB


def test_get_maintenance_history_from_empty_collection(flask_app, mocker: MockFixture):
    get_history = mocker.patch.object(
        maintenance_db, "get_history", return_value=[]
    )
    response = flask_app.get(url_for("maintenance_api.get_history"))
    get_history.assert_called_once()

    assert response.status_code == 200
    assert response.json == []
