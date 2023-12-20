from flask import url_for
from pytest_mock import MockFixture

from job_service.api import auth
from job_service.exceptions import NotFoundException
from job_service.adapter import job_db, target_db
from job_service.model.job import Job, UserInfo
from job_service.model.request import NewJobRequest, UpdateJobRequest
from job_service.config import environment

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


def test_get_jobs(flask_app, mocker: MockFixture):
    get_jobs = mocker.patch.object(job_db, "get_jobs", return_value=JOB_LIST)
    response = flask_app.get(
        url_for(
            "job_api.get_jobs",
            status="completed",
            operation="ADD,CHANGE,PATCH_METADATA",
        ),
    )
    assert response.json == [job.dict(by_alias=True) for job in JOB_LIST]
    assert response.status_code == 200
    get_jobs.assert_called_once()


def test_get_job(flask_app, mocker: MockFixture):
    get_job = mocker.patch.object(job_db, "get_job", return_value=JOB_LIST[0])
    response = flask_app.get(url_for("job_api.get_job", job_id=JOB_ID))
    get_job.assert_called_once()
    get_job.assert_called_with(JOB_ID)
    assert response.status_code == 200
    assert response.json == JOB_LIST[0].dict(by_alias=True)


def test_get_job_not_found(flask_app, mocker: MockFixture):
    get_job = mocker.patch.object(
        job_db, "get_job", side_effect=NotFoundException(NOT_FOUND_MESSAGE)
    )
    response = flask_app.get(url_for("job_api.get_job", job_id=JOB_ID))
    get_job.assert_called_once()
    get_job.assert_called_with(JOB_ID)
    assert response.status_code == 404
    assert response.json == {"message": NOT_FOUND_MESSAGE}


def test_new_job(flask_app, mocker: MockFixture):
    mocker.patch.object(
        auth, "authorize_user", return_value=UserInfo(**USER_INFO_DICT)
    )
    new_job = mocker.patch.object(job_db, "new_job", return_value=JOB_LIST[0])
    update_target = mocker.patch.object(target_db, "update_target")
    response = flask_app.post(url_for("job_api.new_job"), json=NEW_JOB_REQUEST)
    assert new_job.call_count == 2
    new_job.assert_any_call(
        NewJobRequest(**NEW_JOB_REQUEST["jobs"][0]), USER_INFO
    )
    new_job.assert_any_call(
        NewJobRequest(**NEW_JOB_REQUEST["jobs"][1]), USER_INFO
    )
    assert update_target.call_count == 2
    assert response.status_code == 200
    assert response.json == [
        {"msg": "CREATED", "status": "queued", "job_id": JOB_ID},
        {"msg": "CREATED", "status": "queued", "job_id": JOB_ID},
    ]


def test_update_job(flask_app, mocker: MockFixture):
    update_job = mocker.patch.object(
        job_db, "update_job", return_value=JOB_LIST[0]
    )
    update_target = mocker.patch.object(target_db, "update_target")
    response = flask_app.put(
        url_for("job_api.update_job", job_id=JOB_ID), json=UPDATE_JOB_REQUEST
    )
    update_target.assert_called_once()
    update_job.assert_called_once()
    update_job.assert_called_with(
        JOB_ID, UpdateJobRequest(**UPDATE_JOB_REQUEST)
    )
    assert response.status_code == 200
    assert response.json == {"message": f"Updated job with jobId {JOB_ID}"}


def test_update_job_bad_request(flask_app, mocker: MockFixture):
    update_target = mocker.patch.object(target_db, "update_target")
    response = flask_app.put(
        url_for("job_api.update_job", job_id=JOB_ID),
        json={"status": "no-such-status"},
    )
    update_target.assert_not_called()
    assert response.status_code == 400
    assert "validation_error" in response.json


def test_update_job_disabled_bump(flask_app, mocker: MockFixture):
    environment._ENVIRONMENT_VARIABLES["BUMP_ENABLED"] = False
    mocker.patch.object(
        auth, "authorize_user", return_value=UserInfo(**USER_INFO_DICT)
    )
    response = flask_app.post(
        url_for("job_api.new_job"), json=BUMP_JOB_REQUEST
    )

    print(response.json)
    assert response.status_code == 200
    assert response.json == [
        {
            "msg": "FAILED: Bumping the datastore is disabled",
            "status": "FAILED",
        },
    ]
