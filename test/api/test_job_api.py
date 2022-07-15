from flask import url_for
from job_service.exceptions import NotFoundException

from job_service.adapter import job_db
from job_service.model.job import Job
from job_service.model.request import (
    NewJobRequest, UpdateJobRequest
)


NOT_FOUND_MESSAGE = 'not found'
JOB_ID = '123-123-123-123'
JOB_LIST = [
    Job(
        job_id='123-123-123-123',
        operation='ADD',
        status='completed',
        parameters={
            'target': 'MY_DATASET',
            'operation': 'ADD'
        }
    ),
    Job(
        job_id='123-123-123-123',
        status='completed',
        parameters={
            'target': 'OTHER_DATASET',
            'operation': 'ADD'
        }
    )
]
NEW_JOB_REQUEST = {
    'jobs': [
        {'operation': 'ADD', 'target': 'MY_DATASET'},
        {'operation': 'CHANGE_DATA', 'target': 'OTHER_DATASET'}
    ]
}
UPDATE_JOB_REQUEST = {
    'status': 'initiated',
    'log': 'extra logging'
}

def test_get_jobs(flask_app, mocker):
    get_jobs = mocker.patch.object(
        job_db, 'get_jobs', return_value=JOB_LIST
    )
    response = flask_app.get(
        url_for(
            "job_api.get_jobs",
            status="completed",
            operation="ADD,CHANGE_DATA,PATCH_METADATA"
        ),
    )
    assert response.json == [job.dict(by_alias=True) for job in JOB_LIST]
    assert response.status_code == 200
    get_jobs.assert_called_once()


def test_get_job(flask_app, mocker):
    get_job = mocker.patch.object(
        job_db, 'get_job', return_value=JOB_LIST[0]
    )
    response = flask_app.get(
        url_for('job_api.get_job', job_id=JOB_ID)
    )
    get_job.assert_called_once()
    get_job.assert_called_with(JOB_ID)
    assert response.status_code == 200
    assert response.json == JOB_LIST[0].dict(by_alias=True)


def test_get_job_not_found(flask_app, mocker):
    get_job = mocker.patch.object(
        job_db, 'get_job', side_effect=NotFoundException(NOT_FOUND_MESSAGE)
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
        job_db, 'new_job', return_value=JOB_ID
    )
    response = flask_app.post(
        url_for('job_api.new_job'),
        json=NEW_JOB_REQUEST
    )
    assert new_job.call_count == 2
    new_job.assert_any_call(
        NewJobRequest(**NEW_JOB_REQUEST['jobs'][0])
    )
    new_job.assert_any_call(
        NewJobRequest(**NEW_JOB_REQUEST['jobs'][1])
    )
    assert response.status_code == 200
    assert response.json == [
        {'msg': 'CREATED', 'status': 'queued'},
        {'msg': 'CREATED', 'status': 'queued'}
    ]


def test_update_job(flask_app, mocker):
    update_job = mocker.patch.object(
        job_db, 'update_job', return_value=JOB_ID
    )
    response = flask_app.put(
        url_for('job_api.update_job', job_id=JOB_ID),
        json=UPDATE_JOB_REQUEST
    )
    update_job.assert_called_once()
    update_job.assert_called_with(
        JOB_ID,
        UpdateJobRequest(**UPDATE_JOB_REQUEST)
    )
    assert response.status_code == 200
    assert response.json == {'message': f'Updated job with jobId {JOB_ID}'}


def test_update_job_bad_request(flask_app):
    response = flask_app.put(
        url_for('job_api.update_job', job_id=JOB_ID),
        json={"status": "no-such-status"}
    )
    assert response.status_code == 400
    assert 'validation_error' in response.json
