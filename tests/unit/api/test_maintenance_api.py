from flask import url_for
from pytest_mock import MockFixture
from job_service.adapter import maintenance_db

MAINTENANCE_STATUS_REQUEST = {"msg": "we upgrade chill", "pause": 1}

DOCUMENT_FROM_DB = {'_id': '64ee001303a2f9d32f549e0d',
                    'msg': 'we need to upgrade again',
                    'pause': 1,
                    'timestamp': '2023-08-29 16:26:27.575276'}


def test_set_maintenance_status(flask_app, mocker: MockFixture):
    db_set_status = mocker.patch.object(maintenance_db, "set_status")
    response = flask_app.post(url_for("maintenance_api.set_status"), json=MAINTENANCE_STATUS_REQUEST)

    db_set_status.assert_called_once()
    assert response.status_code == 200

    assert response.json == {'status': 'SUCCESS', 'msg': 'we upgrade chill', 'pause': 1}


def test_get_maintenance_status(flask_app, mocker: MockFixture):
    get_status = mocker.patch.object(maintenance_db, "get_latest_status", return_value=DOCUMENT_FROM_DB)

    response = flask_app.get(url_for("maintenance_api.get_status"))

    get_status.assert_called_once()
    assert response.status_code == 200
    assert response.json == DOCUMENT_FROM_DB
