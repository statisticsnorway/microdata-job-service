# pylint: disable=unused-argument
from pytest_mock import MockFixture
from testcontainers.mongodb import MongoDbContainer

from job_service.adapter import maintenance_db

DOCUMENT_DICT = {
    "id": "upgrade_in_progress",
    "flag": False
}

mongo = MongoDbContainer("mongo:5.0")
mongo.start()
DB_CLIENT = mongo.get_connection_client()


def teardown_module():
    mongo.stop()


def setup_function():
    DB_CLIENT.jobdb.drop_collection("maintenance")


def test_set_upgrade_in_progress(mocker: MockFixture):
    mocker.patch.object(
        maintenance_db, "maintenance", DB_CLIENT.jobdb.maintenance
    )

    maintenance_db.set_upgrade_in_progress(True)
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 1
    assert maintenance_db.is_upgrade_in_progress() == True

    maintenance_db.set_upgrade_in_progress(False)
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 1
    assert maintenance_db.is_upgrade_in_progress() == False
