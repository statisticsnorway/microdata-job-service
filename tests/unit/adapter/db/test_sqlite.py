import os
import json
import sqlite3
from datetime import datetime

import pytest
from pytest_mock import MockFixture

from job_service.adapter.db.sqlite import (
    JobAlreadyCompleteException,
    NotFoundException,
    MaintenanceStatusRequest,
    SqliteDbClient,
    UpdateJobRequest,
    JobExistsException,
)
from job_service.model.job import (
    Job,
    UserInfo,
    DataStructureUpdate,
    DatastoreVersion,
    JobParameters,
)
from job_service.model.target import Target
from job_service.model.request import GetJobRequest, NewJobRequest

sqlite_file = "test.db"
CLIENT = SqliteDbClient(f"sqlite://{sqlite_file}")

USER_INFO_DICT = {
    "userId": "123-123-123",
    "firstName": "Data",
    "lastName": "Admin",
}

JOB = {
    "status": "completed",
    "parameters": {"operation": "ADD", "target": "MY_DATASET"},
    "created_by": USER_INFO_DICT,
    "logs": [
        {"at": datetime.now(), "message": "example log"},
        {"at": datetime.now(), "message": "other example"},
    ],
    "created_at": datetime.now(),
}
JOB2 = {
    "status": "queued",
    "parameters": {"operation": "ADD", "target": "MY_OTHER_DATASET"},
    "created_by": USER_INFO_DICT,
    "logs": [],
    "created_at": datetime.now(),
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


def teardown_function():
    os.remove(sqlite_file)


def setup_function():
    global CLIENT
    CLIENT = SqliteDbClient(f"sqlite://{sqlite_file}")
    conn = sqlite3.connect(sqlite_file)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO datastore (rdn, description, directory, name)
        VALUES (?, ?, ?, ?)
        """,
        (
            "no.ssb.test",
            "unit test datastore",
            "test/resources/test_datastore",
            "SSB test datastore",
        ),
    )
    cursor.execute(
        """
        INSERT INTO job (target, datastore_id, status, created_at, created_by, parameters)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            JOB["parameters"]["target"],
            1,
            JOB["status"],
            JOB["created_at"],
            json.dumps(JOB["created_by"]),
            json.dumps(JOB["parameters"]),
        ),
    )
    job_id = cursor.lastrowid
    for log in JOB.get("logs", []):
        cursor.execute(
            """
            INSERT INTO job_log (job_id, msg, at)
            VALUES (?, ?, ?)
        """,
            (
                job_id,
                log["message"],
                log["at"],
            ),
        )
    cursor.execute(
        """
        INSERT INTO job (target, datastore_id, status, created_at, created_by, parameters)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (
            JOB2["parameters"]["target"],
            1,
            JOB2["status"],
            JOB2["created_at"],
            json.dumps(JOB2["created_by"]),
            json.dumps(JOB2["parameters"]),
        ),
    )
    for target in TARGET_LIST:
        cursor.execute(
            """
            INSERT INTO target (name, datastore, last_updated_at, status, action, last_updated_by)
            VALUES (?,  ?, ?, ?, ?, ?)
            """,
            (
                target.name,
                1,
                target.last_updated_at,
                str(target.status),
                ",".join(target.action),
                json.dumps(USER_INFO_DICT),
            ),
        )
    conn.commit()


def test_get_job():
    job = CLIENT.get_job(1)
    assert isinstance(job, Job)
    assert job.log
    job = CLIENT.get_job(2)
    assert isinstance(job, Job)
    assert not job.log


def test_get_jobs():
    jobs = CLIENT.get_jobs(
        GetJobRequest(status=None, operation=None, ignoreCompleted=False)
    )
    assert len(jobs) == 2
    jobs = CLIENT.get_jobs(
        GetJobRequest(status="queued", operation=["ADD"], ignoreCompleted=True)
    )
    assert len(jobs) == 1


def test_get_jobs_for_target():
    jobs = CLIENT.get_jobs_for_target("MY_DATASET")
    assert len(jobs) == 1


def test_new_job():
    job = CLIENT.new_job(
        NewJobRequest(operation="ADD", target="NEW_DATASET"),
        UserInfo(**USER_INFO_DICT),
    )
    assert job
    jobs = CLIENT.get_jobs_for_target("NEW_DATASET")
    assert len(jobs) == 1
    with pytest.raises(JobExistsException):
        CLIENT.new_job(
            NewJobRequest(operation="ADD", target="NEW_DATASET"),
            UserInfo(**USER_INFO_DICT),
        )


def test_update_job():
    existing_job = CLIENT.get_job(2)
    assert existing_job.status == "queued"
    updated_job = CLIENT.update_job(
        "2",
        UpdateJobRequest(
            status="validating",
            log="update log",
        ),
    )
    assert updated_job
    assert updated_job.status == "validating"
    assert updated_job.log[0].message == "update log"
    assert updated_job == CLIENT.get_job(2)
    updated_job = CLIENT.update_job(
        "2",
        UpdateJobRequest(
            status="pseudonymizing",
            log="even newer update log",
        ),
    )
    assert updated_job
    assert updated_job.status == "pseudonymizing"
    assert updated_job.log[1].message == "even newer update log"
    assert updated_job == CLIENT.get_job(2)

    with pytest.raises(NotFoundException):
        CLIENT.update_job("33", UpdateJobRequest(status="validating"))


def test_new_job_different_created_at():
    job1 = NewJobRequest(
        operation="ADD", target="NEW_DATASET"
    ).generate_job_from_request("abc", UserInfo(**USER_INFO_DICT))

    job2 = NewJobRequest(
        operation="ADD", target="NEW_DATASET"
    ).generate_job_from_request("def", UserInfo(**USER_INFO_DICT))
    assert job1.created_at != job2.created_at


def test_update_job_completed():
    existing_job = CLIENT.get_job(2)
    assert existing_job.status == "queued"
    updated_job = CLIENT.update_job(
        "2",
        UpdateJobRequest(
            status="completed",
            log="my new log",
        ),
    )
    assert updated_job
    assert updated_job.status == "completed"
    assert updated_job.log[0].message == "my new log"
    assert updated_job == CLIENT.get_job(2)

    with pytest.raises(JobAlreadyCompleteException):
        CLIENT.update_job("1", UpdateJobRequest(status="completed"))


def test_update_job_failed():
    existing_job = CLIENT.get_job(2)
    assert existing_job.status == "queued"
    updated_job = CLIENT.update_job(
        "2",
        UpdateJobRequest(
            status="failed",
            log="my new log",
        ),
    )
    assert updated_job
    assert updated_job.status == "failed"
    assert updated_job.log[0].message == "my new log"
    assert updated_job == CLIENT.get_job(2)

    with pytest.raises(JobAlreadyCompleteException):
        CLIENT.update_job("1", UpdateJobRequest(status="failed"))


def test_initialize_after_get_maintenance_latest_status(mocker: MockFixture):
    spy = mocker.spy(CLIENT, "initialize_maintenance")
    latest = CLIENT.get_latest_maintenance_status()
    assert spy.call_count == 1
    assert latest


def test_initialize_after_get_maintenance_history(mocker: MockFixture):
    spy = mocker.spy(CLIENT, "initialize_maintenance")
    history = CLIENT.get_maintenance_history()
    assert spy.call_count == 1
    assert history
    assert len(history) == 1


def test_set_and_get_maintenance_status():
    maintenance_status = CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(
            msg="test",
            paused=True,
        )
    )
    assert maintenance_status
    assert CLIENT.get_latest_maintenance_status() == maintenance_status
    assert CLIENT.get_maintenance_history() == [maintenance_status]

    new_maintenance_status = CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(
            msg="test2",
            paused=False,
        )
    )
    assert new_maintenance_status
    assert CLIENT.get_latest_maintenance_status() == new_maintenance_status
    assert CLIENT.get_maintenance_history() == [
        new_maintenance_status,
        maintenance_status,
    ]


def test_get_targets():
    targets = CLIENT.get_targets()
    target_names = [target.name for target in targets]
    assert "MY_DATASET" in target_names
    assert "OTHER_DATASET" in target_names


def test_update_target():
    CLIENT.update_target(TARGET_UPDATE_JOB)
    targets = CLIENT.get_targets()
    assert len(targets) == 2
    target = targets[0]
    assert target.action == ["ADD"]
    assert target.status == "queued"

    CLIENT.update_target(NEW_TARGET_JOB)
    targets = CLIENT.get_targets()
    assert len(targets) == 3
    target_names = [target.name for target in targets]
    assert "NEW_DATASET" in target_names
    assert "OTHER_DATASET" in target_names
    assert "MY_DATASET" in target_names


def test_update_targets_bump():
    CLIENT.update_bump_targets(BUMP_JOB)
    targets = CLIENT.get_targets()
    assert len(targets) == 4
    target_names = [target.name for target in targets]
    assert "MY_DATASET" in target_names
    assert "OTHER_DATASET" in target_names
    assert "FRESH_DATASET" in target_names
    assert "FRESH_DATASET2" in target_names
    assert all(
        [
            target.action == ["RELEASED", "2.0.0"]
            or target.action == ["REMOVED", "2.0.0"]
            for target in targets
            if target.name != "OTHER_DATASET"
        ]
    )
    assert all(
        [
            target.action == ["SET_STATUS", "PENDING_RELEASE"]
            for target in targets
            if target.name == "OTHER_DATASET"
        ]
    )
