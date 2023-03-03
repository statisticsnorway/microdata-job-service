# pylint: disable=unused-argument
from testcontainers.mongodb import MongoDbContainer

from job_service.adapter import target_db
from job_service.model.job import Job
from job_service.model.target import Target


USER_INFO_DICT = {
    'userId': '123-123-123',
    'firstName': 'Data',
    'lastName': 'Admin'
}
TARGET_LIST = [
    Target(
        name="MY_DATASET",
        last_updated_at='2022-05-18T11:40:22.519222',
        status='completed',
        action=['SET_STATUS', 'PENDING_RELEASE'],
        last_updated_by=USER_INFO_DICT
    ),
    Target(
        name="OTHER_DATASET",
        last_updated_at='2022-05-18T11:40:22.519222',
        status='completed',
        action=['SET_STATUS', 'PENDING_RELEASE'],
        last_updated_by=USER_INFO_DICT
    )
]
TARGET_UPDATE_JOB = Job(
    job_id='123-123-123-123',
    status='queued',
    parameters={
        'target': 'MY_DATASET',
        'operation': 'ADD'
    },
    created_at='2022-05-18T11:40:22.519222',
    created_by=USER_INFO_DICT
)
NEW_TARGET_JOB = Job(
    job_id='123-123-123-123',
    status='queued',
    parameters={
        'target': 'NEW_DATASET',
        'operation': 'ADD'
    },
    created_at='2022-05-18T11:40:22.519222',
    created_by=USER_INFO_DICT
)


mongo = MongoDbContainer("mongo:5")
DB_CLIENT = None


def setup_module():
    # pylint: disable=global-statement
    global DB_CLIENT
    mongo.start()
    DB_CLIENT = mongo.get_connection_client()


def teardown_module():
    mongo.stop()


def test_get_targets(mocker):
    DB_CLIENT.jobdb.drop_collection('targets')
    DB_CLIENT.jobdb.targets.insert_one(TARGET_LIST[0].dict(by_alias=True))
    assert DB_CLIENT.jobdb.targets.count_documents({}) == 1

    mocker.patch.object(
        target_db, 'targets_collection',
        DB_CLIENT.jobdb.targets
    )
    assert target_db.get_targets() == [TARGET_LIST[0]]


def test_update_target(mocker):
    DB_CLIENT.jobdb.drop_collection('targets')
    DB_CLIENT.jobdb.targets.insert_one(TARGET_LIST[0].dict(by_alias=True))
    assert DB_CLIENT.jobdb.targets.count_documents({}) == 1

    mocker.patch.object(
        target_db, 'targets_collection',
        DB_CLIENT.jobdb.targets
    )
    target_db.update_target(TARGET_UPDATE_JOB)
    assert DB_CLIENT.jobdb.targets.count_documents({}) == 1
    updated_target = target_db.get_targets()[0]
    assert updated_target.action == ['ADD']
    assert updated_target.status == 'queued'

    target_db.update_target(NEW_TARGET_JOB)
    assert DB_CLIENT.jobdb.targets.count_documents({}) == 2
    targets = target_db.get_targets()
    updated_target = next(
        (target for target in targets if target.name == 'NEW_DATASET'),
        None
    )
    assert updated_target.action == ['ADD']
