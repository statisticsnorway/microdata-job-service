import pytest
from pytest_mock import MockFixture

from unittest.mock import Mock

from fastapi.testclient import TestClient

from job_service.app import app
from job_service.adapter import db
from job_service.api import auth
from job_service.config import environment
from job_service.exceptions import NotFoundException
from job_service.model.job import Job, UserInfo
from job_service.model.request import (
    NewJobRequest,
    UpdateJobRequest,
)


NOT_FOUND_MESSAGE = "not found"
JOB_ID = "123-123-123-123"
USER_INFO_DICT = {
    "userId": "123-123-123",
    "firstName": "Data",
    "lastName": "Admin",
}
USER_INFO = UserInfo(**USER_INFO_DICT)
JOB_LIST = [
    Job(
        job_id="123-123-123-123",
        status="completed",
        parameters={"target": "MY_DATASET", "operation": "ADD"},
        created_at="2022-05-18T11:40:22.519222",
        created_by=USER_INFO_DICT,
    ),
    Job(
        job_id="123-123-123-123",
        status="completed",
        parameters={"target": "OTHER_DATASET", "operation": "ADD"},
        created_at="2022-05-18T11:40:22.519222",
        created_by=USER_INFO_DICT,
    ),
]
NEW_JOB_REQUEST = {
    "jobs": [
        {"operation": "ADD", "target": "MY_DATASET"},
        {"operation": "CHANGE", "target": "OTHER_DATASET"},
    ]
}
BUMP_JOB_REQUEST = {
    "jobs": [
        {
            "operation": "BUMP",
            "target": "DATASTORE",
            "description": "Bump datastore version",
            "bumpFromVersion": "1.0.0",
            "bumpToVersion": "1.1.0",
            "bumpManifesto": {
                "version": "0.0.0.1634512323",
                "description": "Draft",
                "releaseTime": 1634512323,
                "languageCode": "no",
                "updateType": "MINOR",
                "dataStructureUpdates": [
                    {
                        "name": "MY_DATASET",
                        "description": "Første publisering",
                        "operation": "ADD",
                        "releaseStatus": "PENDING_RELEASE",
                    }
                ],
            },
        }
    ]
}
UPDATE_JOB_REQUEST = {"status": "initiated", "log": "extra logging"}


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.update_target.return_value = None
    mock.get_job.return_value = JOB_LIST[0]
    mock.get_jobs.return_value = JOB_LIST
    mock.new_job.return_value = JOB_LIST[0]
    mock.update_job.return_value = JOB_LIST[0]
    return mock


@pytest.fixture
def client(mock_db_client):
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_get_jobs(client, mock_db_client, mocker: MockFixture):
    response = client.get(
        "jobs?status=completed&operation=ADD,CHANGE,PATCH_METADATA"
    )
    assert response.json() == [
        job.model_dump(exclude_none=True, by_alias=True) for job in JOB_LIST
    ]
    assert response.status_code == 200
    mock_db_client.get_jobs.assert_called_once()


def test_get_job(client, mock_db_client, mocker: MockFixture):
    response = client.get(f"/jobs/{JOB_ID}")
    mock_db_client.get_job.assert_called_once()
    mock_db_client.get_job.assert_called_with(JOB_ID)
    assert response.status_code == 200
    assert response.json() == JOB_LIST[0].model_dump(
        exclude_none=True, by_alias=True
    )


def test_get_job_not_found(client, mock_db_client, mocker: MockFixture):
    mock_db_client.get_job.side_effect = NotFoundException(NOT_FOUND_MESSAGE)
    response = client.get(f"/jobs/{JOB_ID}")
    mock_db_client.get_job.assert_called_once()
    mock_db_client.get_job.assert_called_with(JOB_ID)
    assert response.status_code == 404
    assert response.json() == {"message": NOT_FOUND_MESSAGE}


def test_new_job(client, mock_db_client, mocker: MockFixture):
    mocker.patch.object(
        auth, "authorize_user", return_value=UserInfo(**USER_INFO_DICT)
    )
    response = client.post("/jobs", json=NEW_JOB_REQUEST)
    assert mock_db_client.new_job.call_count == 2
    mock_db_client.new_job.assert_any_call(
        NewJobRequest(**NEW_JOB_REQUEST["jobs"][0]), USER_INFO
    )
    mock_db_client.new_job.assert_any_call(
        NewJobRequest(**NEW_JOB_REQUEST["jobs"][1]), USER_INFO
    )
    assert mock_db_client.update_target.call_count == 2
    assert response.status_code == 200
    assert response.json() == [
        {"msg": "CREATED", "status": "queued", "job_id": JOB_ID},
        {"msg": "CREATED", "status": "queued", "job_id": JOB_ID},
    ]


def test_update_job(client, mock_db_client, mocker: MockFixture):
    response = client.put(f"/jobs/{JOB_ID}", json=UPDATE_JOB_REQUEST)
    mock_db_client.update_target.assert_called_once()
    mock_db_client.update_job.assert_called_once()
    mock_db_client.update_job.assert_called_with(
        JOB_ID, UpdateJobRequest(**UPDATE_JOB_REQUEST)
    )
    assert response.status_code == 200
    assert response.json() == {"message": f"Updated job with jobId {JOB_ID}"}


def test_update_job_bad_request(client, mock_db_client, mocker: MockFixture):
    response = client.put(
        f"/jobs/{JOB_ID}",
        json={"status": "no-such-status"},
    )
    mock_db_client.update_target.assert_not_called()
    assert response.status_code == 400
    assert response.json().get("details") is not None


def test_update_job_disabled_bump(client, mock_db_client, mocker: MockFixture):
    environment._ENVIRONMENT_VARIABLES["BUMP_ENABLED"] = False
    mocker.patch.object(
        auth, "authorize_user", return_value=UserInfo(**USER_INFO_DICT)
    )
    response = client.post("/jobs", json=BUMP_JOB_REQUEST)
    assert response.status_code == 200
    assert response.json() == [
        {
            "msg": "FAILED: Bumping the datastore is disabled",
            "status": "FAILED",
        },
    ]
