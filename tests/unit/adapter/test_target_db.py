# pylint: disable=unused-argument
from pytest_mock import MockFixture
from testcontainers.mongodb import MongoDbContainer

from job_service.adapter import target_db
from job_service.model.target import Target
from job_service.model.job import (
    DataStructureUpdate,
    DatastoreVersion,
    Job,
    JobParameters,
)


USER_INFO_DICT = {
    "userId": "123-123-123",
    "firstName": "Data",
    "lastName": "Admin",
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
DB_CLIENT = mongo.get_connection_client()


def teardown_module():
    mongo.stop()


def setup_function():
    DB_CLIENT.jobdb.drop_collection("targets")
    DB_CLIENT.jobdb.inprogress.create_index("name", unique=True)


def test_get_targets(mocker: MockFixture):
    DB_CLIENT.jobdb.targets.insert_one(TARGET_LIST[0].dict(by_alias=True))
    assert DB_CLIENT.jobdb.targets.count_documents({}) == 1

    mocker.patch.object(
        target_db, "targets_collection", DB_CLIENT.jobdb.targets
    )
    assert target_db.get_targets() == [TARGET_LIST[0]]


def test_update_target(mocker: MockFixture):
    DB_CLIENT.jobdb.targets.insert_one(TARGET_LIST[0].dict(by_alias=True))
    assert DB_CLIENT.jobdb.targets.count_documents({}) == 1

    mocker.patch.object(
        target_db, "targets_collection", DB_CLIENT.jobdb.targets
    )
    target_db.update_target(TARGET_UPDATE_JOB)
    assert DB_CLIENT.jobdb.targets.count_documents({}) == 1
    updated_target = target_db.get_targets()[0]
    assert updated_target.action == ["ADD"]
    assert updated_target.status == "queued"

    target_db.update_target(NEW_TARGET_JOB)
    assert DB_CLIENT.jobdb.targets.count_documents({}) == 2
    targets = target_db.get_targets()
    updated_target = next(
        (target for target in targets if target.name == "NEW_DATASET"), None
    )
    assert updated_target.action == ["ADD"]


def test_update_targets_bump(mocker: MockFixture):
    DB_CLIENT.jobdb.targets.insert_one(TARGET_LIST[0].dict(by_alias=True))
    DB_CLIENT.jobdb.targets.insert_one(TARGET_LIST[1].dict(by_alias=True))
    assert DB_CLIENT.jobdb.targets.count_documents({}) == 2

    mocker.patch.object(
        target_db, "targets_collection", DB_CLIENT.jobdb.targets
    )

    target_db.update_bump_targets(BUMP_JOB)
    assert DB_CLIENT.jobdb.targets.count_documents({}) == 4
    targets = target_db.get_targets()
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
