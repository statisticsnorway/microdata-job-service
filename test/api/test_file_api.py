from flask import url_for
from job_service.exceptions.exceptions import JobExistsException

from job_service.repository.job_db import JobDb

JOB_ID = "123-123-123-123"
JOB_EXISTS_MESSAGE = 'MY_DATASET already imported'


def test_get_files(flask_app):
    response = flask_app.get(
        url_for("file_api.get_files")
    )
    assert response.status_code == 200
    assert response.json == ["MY_DATASET", "OTHER_DATASET"]


def test_import_dataset_files(flask_app, mocker):
    new_job = mocker.patch.object(
        JobDb, 'new_job',
        return_value=JOB_ID
    )
    response = flask_app.post(
        url_for("file_api.import_dataset_files"),
        json={'datasetList': ['MY_DATASET', 'OTHER_DATASET']}
    )
    assert new_job.call_count == 2
    new_job.assert_any_call('ADD_DATA', 'queued', 'MY_DATASET')
    new_job.assert_any_call('ADD_DATA', 'queued', 'OTHER_DATASET')
    assert response.status_code == 200
    assert response.json == [
        {
            "status": "OK",
            "message": "Dataset import job queued with jobId: 123-123-123-123",
            "dataset": "MY_DATASET"
        },
        {
            "status": "OK",
            "message": "Dataset import job queued with jobId: 123-123-123-123",
            "dataset": "OTHER_DATASET"
        }
    ]


def test_import_dataset_files_job_exists(flask_app, mocker):
    new_job = mocker.patch.object(
        JobDb, 'new_job',
        side_effect=JobExistsException(JOB_EXISTS_MESSAGE)
    )
    response = flask_app.post(
        url_for("file_api.import_dataset_files"),
        json={'datasetList': ['MY_DATASET']}
    )
    new_job.assert_called_once()
    new_job.assert_any_call('ADD_DATA', 'queued', 'MY_DATASET')
    assert response.status_code == 200
    assert response.json == [
        {
            "status": "ERROR",
            "message": JOB_EXISTS_MESSAGE,
            "dataset": "MY_DATASET"
        }
    ]
