from flask import url_for
from pytest_mock import MockFixture

from job_service.adapter import maintenance_db


def test_prepare_for_upgrade(flask_app, mocker: MockFixture):
    set_upgrade_in_progress = mocker.patch.object(maintenance_db, "set_upgrade_in_progress")
    response = flask_app.post(url_for("maintenance_api.prepare_for_upgrade"))

    set_upgrade_in_progress.assert_called_once()
    assert response.status_code == 200
    assert response.json == {"status": "SUCCESS", "msg": "PREPARE_FOR_UPGRADE"}


def test_upgrade_done(flask_app, mocker: MockFixture):
    set_upgrade_in_progress = mocker.patch.object(maintenance_db, "set_upgrade_in_progress")
    response = flask_app.post(url_for("maintenance_api.upgrade_done"))

    set_upgrade_in_progress.assert_called_once()
    assert response.status_code == 200
    assert response.json == {"status": "SUCCESS", "msg": "UPGRADE_DONE"}
