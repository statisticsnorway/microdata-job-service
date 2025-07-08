# pylint: disable=unused-argument
from datetime import datetime

import pytest
from pytest_mock import MockFixture
from testcontainers.mongodb import MongoDbContainer

from job_service.adapter.db.mongo import MongoDbClient
from job_service.exceptions import JobExistsException, NotFoundException
from job_service.model.job import (
    Job,
    UserInfo,
    JobParameters,
    DatastoreVersion,
    DataStructureUpdate,
)
from job_service.model.target import Target
from job_service.model.request import (
    GetJobRequest,
    NewJobRequest,
    UpdateJobRequest,
    MaintenanceStatusRequest,
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

TARGET_LIST = [
    Target(
        name="MY_DATASET",
        last_updated_at="2022-05-18T11:40:22.519222",
        status="completed",
        action=["SET_STATUS", "PENDING_RELEASE"],
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
TARGET_UPDATE_JOB = Job(
    job_id="123-123-123-123",
    status="queued",
    parameters={"target": "MY_DATASET", "operation": "ADD"},
    created_at="2022-05-18T11:40:22.519222",
    created_by=USER_INFO_DICT,
)
NEW_TARGET_JOB = Job(
    job_id="123-123-123-123",
    status="queued",
    parameters={"target": "NEW_DATASET", "operation": "ADD"},
    created_at="2022-05-18T11:40:22.519222",
    created_by=USER_INFO_DICT,
)
BUMP_JOB = Job(
    job_id="bump-bump-bump-bump",
    status="completed",
    created_at="2022-05-18T11:40:22.519222",
    created_by=USER_INFO_DICT,
    parameters=JobParameters(
        bump_from_version="1.0.0",
        bump_to_version="2.0.0",
        operation="BUMP",
        target="DATASTORE",
        description="Updates",
        bumpManifesto=DatastoreVersion(
            version="0.0.0.123123",
            description="Draft version",
            release_time="123123",
            language_code="no",
            update_type="MAJOR",
            data_structure_updates=[
                DataStructureUpdate(
                    name="MY_DATASET",
                    description="Update",
                    operation="PATCH_METADATA",
                    release_status="PENDING_RELEASE",
                ),
                DataStructureUpdate(
                    name="FRESH_DATASET",
                    description="Update",
                    operation="REMOVE",
                    release_status="PENDING_DELETE",
                ),
                DataStructureUpdate(
                    name="FRESH_DATASET2",
                    description="Update",
                    operation="ADD",
                    release_status="PENDING_RELEASE",
                ),
                DataStructureUpdate(
                    name="NOT_BUMPED_DATASET",
                    description="Update",
                    operation="ADD",
                    release_status="DRAFT",
                ),
            ],
        ),
    ),
)

mongo = MongoDbContainer("mongo:5.0")
mongo.start()
TEST_DB_CLIENT = mongo.get_connection_client()
CLIENT = MongoDbClient()


def teardown_module():
    mongo.stop()


def setup_function():
    TEST_DB_CLIENT.jobdb.drop_collection("in_progress")
    TEST_DB_CLIENT.jobdb.drop_collection("completed")
    TEST_DB_CLIENT.jobdb.inprogress.create_index(
        "parameters.target", unique=True
    )
    TEST_DB_CLIENT.jobdb.drop_collection("maintenance")
    TEST_DB_CLIENT.jobdb.drop_collection("targets")
    TEST_DB_CLIENT.jobdb.inprogress.create_index("name", unique=True)


def test_get_job(mocker: MockFixture):
    TEST_DB_CLIENT.jobdb.in_progress.insert_one(JOB)
    assert TEST_DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert TEST_DB_CLIENT.jobdb.completed.count_documents({}) == 0

    mocker.patch.object(
        CLIENT, "in_progress", TEST_DB_CLIENT.jobdb.in_progress
    )
    mocker.patch.object(CLIENT, "completed", TEST_DB_CLIENT.jobdb.completed)
    assert CLIENT.get_job(JOB_ID) == Job(**JOB)

    with pytest.raises(NotFoundException) as e:
        CLIENT.get_job(NON_EXISTING_JOB_ID)
    assert "No job found for jobId:" in str(e)


def test_get_jobs(mocker: MockFixture):
    TEST_DB_CLIENT.jobdb.in_progress.insert_one(JOB)
    assert TEST_DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert TEST_DB_CLIENT.jobdb.completed.count_documents({}) == 0
    mocker.patch.object(
        CLIENT, "in_progress", TEST_DB_CLIENT.jobdb.in_progress
    )
    mocker.patch.object(CLIENT, "completed", TEST_DB_CLIENT.jobdb.completed)
    get_job_request = GetJobRequest(status="queued")
    assert CLIENT.get_jobs(get_job_request) == [Job(**JOB)]


def test_new_job(mocker: MockFixture):
    mocker.patch.object(
        CLIENT, "in_progress", TEST_DB_CLIENT.jobdb.in_progress
    )
    mocker.patch.object(CLIENT, "completed", TEST_DB_CLIENT.jobdb.completed)
    assert (
        CLIENT.new_job(
            NewJobRequest(operation="ADD", target="NEW_DATASET"), USER_INFO
        )
        is not None
    )
    assert TEST_DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert TEST_DB_CLIENT.jobdb.completed.count_documents({}) == 0
    actual = CLIENT.get_jobs(GetJobRequest())[0]
    assert actual.created_by.model_dump() == USER_INFO.model_dump()
    assert actual.parameters.target == "NEW_DATASET"
    assert actual.parameters.operation == "ADD"
    with pytest.raises(JobExistsException) as e:
        CLIENT.new_job(
            NewJobRequest(operation="ADD", target="NEW_DATASET"), USER_INFO
        )
    assert "NEW_DATASET already in progress" in str(e)


def test_update_job(mocker: MockFixture):
    TEST_DB_CLIENT.jobdb.in_progress.insert_one(JOB)
    assert TEST_DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert TEST_DB_CLIENT.jobdb.completed.count_documents({}) == 0

    mocker.patch.object(
        CLIENT, "in_progress", TEST_DB_CLIENT.jobdb.in_progress
    )
    mocker.patch.object(CLIENT, "completed", TEST_DB_CLIENT.jobdb.completed)
    CLIENT.update_job("123-123-123-123", UpdateJobRequest(status="validating"))
    actual = CLIENT.get_job(JOB_ID)
    assert actual.status == "validating"

    CLIENT.update_job(
        "123-123-123-123",
        UpdateJobRequest(status="validating", log="update log"),
    )
    actual = CLIENT.get_job(JOB_ID)
    assert actual.status == "validating"
    assert actual.log[len(actual.log) - 1].message == "update log"
    assert TEST_DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert TEST_DB_CLIENT.jobdb.completed.count_documents({}) == 0


def test_new_job_different_created_at():
    job1 = NewJobRequest(
        operation="ADD", target="NEW_DATASET"
    ).generate_job_from_request("abc", USER_INFO)

    job2 = NewJobRequest(
        operation="ADD", target="NEW_DATASET"
    ).generate_job_from_request("def", USER_INFO)

    assert job1.created_at != job2.created_at


def test_update_job_completed(mocker: MockFixture):
    TEST_DB_CLIENT.jobdb.in_progress.insert_one(JOB)
    assert TEST_DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert TEST_DB_CLIENT.jobdb.completed.count_documents({}) == 0
    mocker.patch.object(
        CLIENT, "in_progress", TEST_DB_CLIENT.jobdb.in_progress
    )
    mocker.patch.object(CLIENT, "completed", TEST_DB_CLIENT.jobdb.completed)

    CLIENT.update_job(
        "123-123-123-123",
        UpdateJobRequest(status="completed", log="my new log"),
    )
    assert TEST_DB_CLIENT.jobdb.in_progress.count_documents({}) == 0
    assert TEST_DB_CLIENT.jobdb.completed.count_documents({}) == 1


def test_update_job_failed(mocker: MockFixture):
    TEST_DB_CLIENT.jobdb.in_progress.insert_one(JOB)
    assert TEST_DB_CLIENT.jobdb.in_progress.count_documents({}) == 1
    assert TEST_DB_CLIENT.jobdb.completed.count_documents({}) == 0
    mocker.patch.object(
        CLIENT, "in_progress", TEST_DB_CLIENT.jobdb.in_progress
    )
    mocker.patch.object(CLIENT, "completed", TEST_DB_CLIENT.jobdb.completed)

    CLIENT.update_job(
        "123-123-123-123", UpdateJobRequest(status="failed", log="my new log")
    )
    assert TEST_DB_CLIENT.jobdb.in_progress.count_documents({}) == 0
    assert TEST_DB_CLIENT.jobdb.completed.count_documents({}) == 1


def test_initialize_after_get_latest_status(mocker: MockFixture):
    mocker.patch.object(
        CLIENT, "maintenance", TEST_DB_CLIENT.jobdb.maintenance
    )
    latest_status = CLIENT.get_latest_maintenance_status()

    assert (
        latest_status["msg"]
        == "Initial status inserted by job service at startup."
    )
    assert latest_status["paused"] == 0
    assert "timestamp" in latest_status.keys()


def test_initialize_after_get_history(mocker: MockFixture):
    mocker.patch.object(
        CLIENT, "maintenance", TEST_DB_CLIENT.jobdb.maintenance
    )
    statuses = CLIENT.get_maintenance_history()

    assert (
        statuses[0]["msg"]
        == "Initial status inserted by job service at startup."
    )
    assert statuses[0]["paused"] == 0
    assert "timestamp" in statuses[0].keys()


def test_set_status(mocker: MockFixture):
    mocker.patch.object(
        CLIENT, "maintenance", TEST_DB_CLIENT.jobdb.maintenance
    )

    status = CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="we upgrade chill", paused=True)
    )
    assert status["msg"] == "we upgrade chill"
    assert status["paused"]
    assert "timestamp" in status
    assert "_id" not in status


def test_set_and_get_status(mocker: MockFixture):
    mocker.patch.object(
        CLIENT, "maintenance", TEST_DB_CLIENT.jobdb.maintenance
    )

    CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="we upgrade chill", paused=True)
    )
    assert TEST_DB_CLIENT.jobdb.maintenance.count_documents({}) == 1
    CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="finished upgrading", paused=False)
    )
    assert TEST_DB_CLIENT.jobdb.maintenance.count_documents({}) == 2
    CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="we need to upgrade again", paused=True)
    )
    assert TEST_DB_CLIENT.jobdb.maintenance.count_documents({}) == 3

    latest_status = CLIENT.get_latest_maintenance_status()

    assert latest_status["msg"] == "we need to upgrade again"
    assert latest_status["paused"]
    assert "timestamp" in latest_status.keys()


def test_get_history(mocker: MockFixture):
    mocker.patch.object(
        CLIENT, "maintenance", TEST_DB_CLIENT.jobdb.maintenance
    )

    CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="first", paused=True)
    )
    CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="second", paused=False)
    )
    CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="last", paused=True)
    )

    documents = CLIENT.get_maintenance_history()

    assert documents[0]["msg"] == "last"
    assert documents[1]["msg"] == "second"
    assert documents[2]["msg"] == "first"


def test_get_targets(mocker: MockFixture):
    TEST_DB_CLIENT.jobdb.targets.insert_one(
        TARGET_LIST[0].model_dump(exclude_none=True, by_alias=True)
    )
    assert TEST_DB_CLIENT.jobdb.targets.count_documents({}) == 1

    mocker.patch.object(CLIENT, "targets", TEST_DB_CLIENT.jobdb.targets)
    assert CLIENT.get_targets() == [TARGET_LIST[0]]


def test_update_target(mocker: MockFixture):
    TEST_DB_CLIENT.jobdb.targets.insert_one(
        TARGET_LIST[0].model_dump(exclude_none=True, by_alias=True)
    )
    assert TEST_DB_CLIENT.jobdb.targets.count_documents({}) == 1

    mocker.patch.object(CLIENT, "targets", TEST_DB_CLIENT.jobdb.targets)
    CLIENT.update_target(TARGET_UPDATE_JOB)
    assert TEST_DB_CLIENT.jobdb.targets.count_documents({}) == 1
    updated_target = CLIENT.get_targets()[0]
    assert updated_target.action == ["ADD"]
    assert updated_target.status == "queued"

    CLIENT.update_target(NEW_TARGET_JOB)
    assert TEST_DB_CLIENT.jobdb.targets.count_documents({}) == 2
    targets = CLIENT.get_targets()
    updated_target = next(
        (target for target in targets if target.name == "NEW_DATASET"), None
    )
    assert updated_target.action == ["ADD"]


def test_update_targets_bump(mocker: MockFixture):
    TEST_DB_CLIENT.jobdb.targets.insert_one(
        TARGET_LIST[0].model_dump(exclude_none=True, by_alias=True)
    )
    TEST_DB_CLIENT.jobdb.targets.insert_one(
        TARGET_LIST[1].model_dump(exclude_none=True, by_alias=True)
    )
    assert TEST_DB_CLIENT.jobdb.targets.count_documents({}) == 2

    mocker.patch.object(CLIENT, "targets", TEST_DB_CLIENT.jobdb.targets)

    CLIENT.update_bump_targets(BUMP_JOB)
    assert TEST_DB_CLIENT.jobdb.targets.count_documents({}) == 4
    targets = CLIENT.get_targets()
    assert len(targets) == 4
    my_dataset_target = next(
        target for target in targets if target.name == "MY_DATASET"
    )
    fresh_dataset_target = next(
        target for target in targets if target.name == "FRESH_DATASET"
    )
    fresh_dataset2_target = next(
        target for target in targets if target.name == "FRESH_DATASET2"
    )
    other_dataset_target = next(
        target for target in targets if target.name == "OTHER_DATASET"
    )
    assert my_dataset_target.action == ["RELEASED", "2.0.0"]
    assert fresh_dataset_target.action == ["REMOVED", "2.0.0"]
    assert fresh_dataset2_target.action == ["RELEASED", "2.0.0"]
    assert other_dataset_target == TARGET_LIST[1]
