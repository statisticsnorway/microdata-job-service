# pylint: disable=unused-argument
from pytest_mock import MockFixture
from testcontainers.mongodb import MongoDbContainer

from job_service.model.request import MaintenanceStatusRequest
from job_service.adapter import maintenance_db

mongo = MongoDbContainer("mongo:5.0")
mongo.start()
DB_CLIENT = mongo.get_connection_client()


def teardown_module():
    mongo.stop()


def setup_function():
    DB_CLIENT.jobdb.drop_collection("maintenance")


def test_set_and_get_status(mocker: MockFixture):
    mocker.patch.object(
        maintenance_db, "maintenance", DB_CLIENT.jobdb.maintenance
    )

    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="we upgrade chill", pause=True)
    )
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 1
    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="finished upgrading", pause=False)
    )
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 2
    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="we need to upgrade again", pause=True)
    )
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 3

    lst = DB_CLIENT.jobdb.maintenance.find()
    latest = max(lst, key=lambda x: x["timestamp"])

    document = maintenance_db.get_latest_status()

    assert document["_id"] == latest["_id"]
    assert document["msg"] == "we need to upgrade again"
    assert document["pause"] == 1


def test_get_history(mocker: MockFixture):
    mocker.patch.object(
        maintenance_db, "maintenance", DB_CLIENT.jobdb.maintenance
    )
    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="first", pause=True)
    )
    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="second", pause=False)
    )
    maintenance_db.set_status(MaintenanceStatusRequest(msg="last", pause=True))

    documents = maintenance_db.get_history()

    assert documents[0]["msg"] == "last"
    assert documents[1]["msg"] == "second"
    assert documents[2]["msg"] == "first"
