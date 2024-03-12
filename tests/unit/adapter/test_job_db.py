# pylint: disable=unused-argument
from datetime import datetime

import pytest
from pytest_mock import MockFixture
from testcontainers.mongodb import MongoDbContainer

from job_service.adapter import job_db
from job_service.exceptions import JobExistsException, NotFoundException
from job_service.model.job import Job, UserInfo
from job_service.model.request import (
    GetJobRequest,
    NewJobRequest,
    UpdateJobRequest,
)

JOB_ID = "123-123-123-123"
USER_INFO_DICT = {
    "userId": "123-123-123",
    "firstName": "Data",
    "lastName": "Admin",
}
USER_INFO = UserInfo(**USER_INFO_DICT)
NON_EXISTING_JOB_ID = "abc-abc-abc-abc"
JOB = {
    "_id": "MONGO_DB_ID",
    "jobId": "123-123-123-123",
    "status": "queued",
    "parameters": {"operation": "ADD", "target": "MY_DATASET"},
    "created_by": USER_INFO_DICT,
    "logs": [{"at": datetime.now(), "message": "example log"}],
    "created_at": str(datetime.now()),
}


mongo = MongoDbContainer("mongo:5.0")
mongo.start()
DB_CLIENT = mongo.get_connection_client()


def teardown_module():
    mongo.stop()


def setup_function():
    DB_CLIENT.jobdb.drop_collection("in_progress")
    DB_CLIENT.jobdb.drop_collection("completed")
    DB_CLIENT.jobdb.inprogress.create_index("parameters.target", unique=True)


def test_get_job(mocker: MockFixture):
    DB_CLIENT.jobdb.in_progress.insert_one(JOB)
    assert DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert DB_CLIENT.jobdb.completed.count_documents({}) == 0

    mocker.patch.object(job_db, "in_progress", DB_CLIENT.jobdb.in_progress)
    mocker.patch.object(job_db, "completed", DB_CLIENT.jobdb.completed)
    assert job_db.get_job(JOB_ID) == Job(**JOB)

    with pytest.raises(NotFoundException) as e:
        job_db.get_job(NON_EXISTING_JOB_ID)
    assert "No job found for jobId:" in str(e)


def test_get_jobs(mocker: MockFixture):
    DB_CLIENT.jobdb.in_progress.insert_one(JOB)
    assert DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert DB_CLIENT.jobdb.completed.count_documents({}) == 0

    mocker.patch.object(job_db, "in_progress", DB_CLIENT.jobdb.in_progress)
    mocker.patch.object(job_db, "completed", DB_CLIENT.jobdb.completed)
    get_job_request = GetJobRequest(status="queued")
    assert job_db.get_jobs(get_job_request) == [Job(**JOB)]


def test_new_job(mocker: MockFixture):
    mocker.patch.object(job_db, "in_progress", DB_CLIENT.jobdb.in_progress)
    mocker.patch.object(job_db, "completed", DB_CLIENT.jobdb.completed)
    assert (
        job_db.new_job(
            NewJobRequest(operation="ADD", target="NEW_DATASET"), USER_INFO
        )
        is not None
    )
    assert DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert DB_CLIENT.jobdb.completed.count_documents({}) == 0
    actual = job_db.get_jobs(GetJobRequest())[0]
    assert actual.created_by.model_dump() == USER_INFO.model_dump()
    assert actual.parameters.target == "NEW_DATASET"
    assert actual.parameters.operation == "ADD"
    with pytest.raises(JobExistsException) as e:
        assert (
            job_db.new_job(
                NewJobRequest(operation="ADD", target="NEW_DATASET"), USER_INFO
            )
            is not None
        )
    assert "NEW_DATASET already in progress" in str(e)


def test_update_job(mocker: MockFixture):
    DB_CLIENT.jobdb.in_progress.insert_one(JOB)
    assert DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert DB_CLIENT.jobdb.completed.count_documents({}) == 0

    mocker.patch.object(job_db, "in_progress", DB_CLIENT.jobdb.in_progress)
    mocker.patch.object(job_db, "completed", DB_CLIENT.jobdb.completed)
    job_db.update_job("123-123-123-123", UpdateJobRequest(status="validating"))
    actual = job_db.get_job(JOB_ID)
    assert actual.status == "validating"

    job_db.update_job(
        "123-123-123-123",
        UpdateJobRequest(status="validating", log="update log"),
    )
    actual = job_db.get_job(JOB_ID)
    assert actual.status == "validating"
    assert actual.log[len(actual.log) - 1].message == "update log"
    assert DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert DB_CLIENT.jobdb.completed.count_documents({}) == 0


def test_new_job_different_created_at():
    job1 = NewJobRequest(
        operation="ADD", target="NEW_DATASET"
    ).generate_job_from_request("abc", USER_INFO)

    job2 = NewJobRequest(
        operation="ADD", target="NEW_DATASET"
    ).generate_job_from_request("def", USER_INFO)

    assert job1.created_at != job2.created_at


def test_update_job_completed(mocker: MockFixture):
    DB_CLIENT.jobdb.in_progress.insert_one(JOB)
    assert DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert DB_CLIENT.jobdb.completed.count_documents({}) == 0
    mocker.patch.object(job_db, "in_progress", DB_CLIENT.jobdb.in_progress)
    mocker.patch.object(job_db, "completed", DB_CLIENT.jobdb.completed)

    job_db.update_job(
        "123-123-123-123",
        UpdateJobRequest(status="completed", log="my new log"),
    )
    assert DB_CLIENT.jobdb.in_progress.count_documents({}) == 0
    assert DB_CLIENT.jobdb.completed.count_documents({}) == 1


def test_update_job_failed(mocker: MockFixture):
    DB_CLIENT.jobdb.in_progress.insert_one(JOB)
    assert DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert DB_CLIENT.jobdb.completed.count_documents({}) == 0
    mocker.patch.object(job_db, "in_progress", DB_CLIENT.jobdb.in_progress)
    mocker.patch.object(job_db, "completed", DB_CLIENT.jobdb.completed)

    job_db.update_job(
        "123-123-123-123", UpdateJobRequest(status="failed", log="my new log")
    )
    assert DB_CLIENT.jobdb.in_progress.count_documents({}) == 0
    assert DB_CLIENT.jobdb.completed.count_documents({}) == 1
