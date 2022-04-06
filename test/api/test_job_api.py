from flask import url_for
from job_service.exceptions.exceptions import NotFoundException

from job_service.repository.job_db import JobDb

NOT_FOUND_MESSAGE = 'not found'
JOB_ID = '123-123-123-123'
JOB = {'job_id': JOB_ID}
JOB_LIST = [
    {'job_id': '123-123-123-123'},
    {'job_id': 'abc-abc-abc-abc'}
]
NEW_JOB_REQUEST = {
    "command": "ADD_DATA",
    "datasetName": "SOME_DATASET",
    "status": "initiated"
}
UPDATE_JOB_REQUEST = {
    "status": "failed",
    "log": "Failed to contact versioning-manager for import"
}


def test_get_jobs(flask_app, mocker):
    get_jobs = mocker.patch.object(
        JobDb, 'get_jobs', return_value=JOB_LIST
    )
    response = flask_app.get(
        url_for("job_api.get_jobs")
    )
    get_jobs.assert_called_once()
    assert response.status_code == 200
    assert response.json == JOB_LIST


def test_get_jobs_status_query(flask_app, mocker):
    get_jobs = mocker.patch.object(
        JobDb, 'get_jobs', return_value=JOB_LIST
    )
    response = flask_app.get(
        url_for('job_api.get_jobs', status='queued')
    )
    get_jobs.assert_called_once()
    get_jobs.assert_called_with('queued')
    assert response.status_code == 200
    assert response.json == JOB_LIST


def test_get_job(flask_app, mocker):
    get_job = mocker.patch.object(
        JobDb, 'get_job', return_value=JOB
    )
    response = flask_app.get(
        url_for('job_api.get_job', job_id=JOB_ID)
    )
    get_job.assert_called_once()
    get_job.assert_called_with(JOB_ID)
    assert response.status_code == 200
    assert response.json == JOB


def test_get_job_not_found(flask_app, mocker):
    get_job = mocker.patch.object(
        JobDb, 'get_job', side_effect=NotFoundException(NOT_FOUND_MESSAGE)
    )
    response = flask_app.get(
        url_for('job_api.get_job', job_id=JOB_ID)
    )
    get_job.assert_called_once()
    get_job.assert_called_with(JOB_ID)
    assert response.status_code == 404
    assert response.json == {"message": NOT_FOUND_MESSAGE}


def test_new_job(flask_app, mocker):
    new_job = mocker.patch.object(
        JobDb, 'new_job', return_value=JOB_ID
    )
    response = flask_app.post(
        url_for('job_api.new_job'),
        json=NEW_JOB_REQUEST
    )
    new_job.assert_called_once()
    new_job.assert_called_with(
        NEW_JOB_REQUEST['command'],
        NEW_JOB_REQUEST['status'],
        NEW_JOB_REQUEST['datasetName']
    )
    assert response.status_code == 200
    assert response.json == {"jobId": JOB_ID}


def test_update_job(flask_app, mocker):
    update_job = mocker.patch.object(
        JobDb, 'update_job', return_value=JOB_ID
    )
    response = flask_app.put(
        url_for('job_api.update_job', job_id=JOB_ID),
        json=UPDATE_JOB_REQUEST
    )
    update_job.assert_called_once()
    update_job.assert_called_with(
        JOB_ID,
        UPDATE_JOB_REQUEST['status'],
        UPDATE_JOB_REQUEST['log']
    )
    assert response.status_code == 200
    assert response.json == {'message': f'Updated job with jobId {JOB_ID}'}


def test_update_job_bad_request(flask_app, mocker):
    response = flask_app.put(
        url_for('job_api.update_job', job_id=JOB_ID),
        json={"status": "no-such-status"}
    )
    assert response.status_code == 400
    assert 'validation_error' in response.json
