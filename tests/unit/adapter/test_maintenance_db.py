# pylint: disable=unused-argument
from pytest_mock import MockFixture
from testcontainers.mongodb import MongoDbContainer

from job_service.adapter import maintenance_db

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

    maintenance_db.set_status({"msg": "we upgrade chill", "pause": 1})
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 1

    maintenance_db.set_status({"msg": "finished upgrading", "pause": 0})
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 2

    maintenance_db.set_status({"msg": "we need to upgrade again", "pause": 1})
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 3

    lst = DB_CLIENT.jobdb.maintenance.find()
    latest = max(lst, key=lambda x: x['timestamp'])

    document = maintenance_db.get_status()

    assert document["_id"] == latest["_id"]
    assert document["msg"] == "we need to upgrade again"
    assert document["pause"] == 1
