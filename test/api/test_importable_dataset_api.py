from flask import url_for
from job_service.exceptions.exceptions import JobExistsException
from job_service.repository import local_storage
from job_service.repository.job_db import JobDb

JOB_ID = "123-123-123-123"
JOB_EXISTS_MESSAGE = 'MY_DATASET already imported'


def test_get_files(flask_app):
    response = flask_app.get(
        url_for("importable_datasets_api.get_importable_datasets")
    )
    assert response.status_code == 200
    assert len(response.json) == 3
    expected_datasets = [
        {'operation': 'ADD_OR_CHANGE_DATA', 'datasetName': 'MY_DATASET'},
        {'operation': 'PATCH_METADATA', 'datasetName': 'YOUR_DATASET'},
        {'operation': 'ADD_OR_CHANGE_DATA', 'datasetName': 'OTHER_DATASET'}
    ]
    for dataset in expected_datasets:
        assert dataset in response.json


def test_post_importable_datasets(flask_app, mocker):
    mocker.patch.object(
        local_storage, 'get_importable_dataset_operation',
        return_value='ADD_OR_CHANGE_DATA'
    )
    new_job = mocker.patch.object(
        JobDb, 'new_job',
        return_value=JOB_ID
    )
    response = flask_app.post(
        url_for("importable_datasets_api.import_datasets"),
        json={
            'importableDatasets': [
                {
                    'datasetName': 'MY_DATASET',
                    'operation': 'ADD_OR_CHANGE_DATA'
                },
                {
                    'datasetName': 'OTHER_DATASET',
                    'operation': 'PATCH_METADATA'
                }
            ]
        }
    )
    assert response.status_code == 200
    assert response.json == [
        {
            "status": "OK",
            "message": "Dataset import job queued with jobId: 123-123-123-123",
            "dataset": "MY_DATASET"
        },
        {
            "status": "ERROR",
            "message": "Unexpected error when importing dataset",
            "dataset": "OTHER_DATASET"
        }
    ]
    assert new_job.call_count == 1
    new_job.assert_any_call('ADD_OR_CHANGE_DATA', 'queued', 'MY_DATASET')


def test_post_importable_dataset_job_exists(flask_app, mocker):
    mocker.patch.object(
        local_storage, 'get_importable_dataset_operation',
        return_value='ADD_OR_CHANGE_DATA'
    )
    new_job = mocker.patch.object(
        JobDb, 'new_job',
        side_effect=JobExistsException(JOB_EXISTS_MESSAGE)
    )
    response = flask_app.post(
        url_for("importable_datasets_api.import_datasets"),
        json={
            'importableDatasets': [
                {
                    'datasetName': 'MY_DATASET',
                    'operation': 'ADD_OR_CHANGE_DATA'
                }
            ]
        }
    )
    assert response.status_code == 200
    assert response.json == [
        {
            "status": "ERROR",
            "message": JOB_EXISTS_MESSAGE,
            "dataset": "MY_DATASET"
        }
    ]
    new_job.assert_called_once()
    new_job.assert_any_call('ADD_OR_CHANGE_DATA', 'queued', 'MY_DATASET')
