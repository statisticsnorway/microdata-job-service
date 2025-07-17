from job_service.adapter.db import CLIENT
from job_service.model.job import Job, UserInfo
from job_service.model.target import Target
from fastapi.testclient import TestClient

from job_service.app import app

client = TestClient(app)


JOB_ID = "123-123-123-123"
USER_INFO_DICT = {
    "userId": "123-123-123",
    "firstName": "Data",
    "lastName": "Admin",
}
USER_INFO = UserInfo(**USER_INFO_DICT)
TARGET_LIST = [
    Target(
        name="MY_DATASET",
        last_updated_at="2022-05-18T11:40:22.519222",
        status="completed",
        action=["ADD"],
        last_updated_by=USER_INFO_DICT,
    ),
    Target(
        name="OTHER_DATASET",
        last_updated_at="2022-05-18T11:40:22.519222",
        status="completed",
        action=["SET_STATUS", "PENDING_RELEASE"],
        last_updated_by=USER_INFO_DICT,
    ),
]
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


def test_get_targets(mocker):
    get_jobs = mocker.patch.object(
        CLIENT, "get_targets", return_value=TARGET_LIST
    )
    response = client.get("/targets")
    assert response.json() == [
        target.model_dump(exclude_none=True, by_alias=True)
        for target in TARGET_LIST
    ]
    assert response.status_code == 200
    get_jobs.assert_called_once()


def test_get_targets_none_found(mocker):
    get_jobs = mocker.patch.object(CLIENT, "get_targets", return_value=[])
    response = client.get("/targets")
    assert response.json() == []
    assert response.status_code == 200
    get_jobs.assert_called_once()


def test_get_target(mocker):
    get_jobs_for_target = mocker.patch.object(
        CLIENT, "get_jobs_for_target", return_value=JOB_LIST
    )
    response = client.get("/targets/MY_DATASET/jobs")
    get_jobs_for_target.assert_called_once()
    get_jobs_for_target.assert_called_with("MY_DATASET")
    assert response.status_code == 200
    assert response.json() == [
        job.model_dump(exclude_none=True, by_alias=True) for job in JOB_LIST
    ]


def test_get_target_none_found(mocker):
    get_job = mocker.patch.object(
        CLIENT, "get_jobs_for_target", return_value=[]
    )
    response = client.get("/targets/MY_DATASET/jobs")
    get_job.assert_called_once()
    get_job.assert_called_with("MY_DATASET")
    assert response.status_code == 200
    assert response.json() == []
