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
    DB_CLIENT.maintenance_db.drop_collection("maintenance")


def test_initialize_after_get_latest_status(mocker: MockFixture):
    mocker.patch.object(
        maintenance_db, "maintenance", DB_CLIENT.maintenance_db.maintenance
    )
    latest_status = maintenance_db.get_latest_status()

    assert (
        latest_status["msg"]
        == "Initial status inserted by job service at startup."
    )
    assert latest_status["paused"] == 0
    assert "timestamp" in latest_status.keys()


def test_initialize_after_get_history(mocker: MockFixture):
    mocker.patch.object(
        maintenance_db, "maintenance", DB_CLIENT.maintenance_db.maintenance
    )
    statuses = maintenance_db.get_history()

    assert (
        statuses[0]["msg"]
        == "Initial status inserted by job service at startup."
    )
    assert statuses[0]["paused"] == 0
    assert "timestamp" in statuses[0].keys()


def test_set_status(mocker: MockFixture):
    mocker.patch.object(
        maintenance_db, "maintenance", DB_CLIENT.maintenance_db.maintenance
    )

    status = maintenance_db.set_status(
        MaintenanceStatusRequest(msg="we upgrade chill", paused=True)
    )
    assert status["msg"] == "we upgrade chill"
    assert status["paused"]
    assert "timestamp" in status
    assert "_id" not in status


def test_set_and_get_status(mocker: MockFixture):
    mocker.patch.object(
        maintenance_db, "maintenance", DB_CLIENT.maintenance_db.maintenance
    )

    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="we upgrade chill", paused=True)
    )
    assert DB_CLIENT.maintenance_db.maintenance.count_documents({}) == 1
    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="finished upgrading", paused=False)
    )
    assert DB_CLIENT.maintenance_db.maintenance.count_documents({}) == 2
    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="we need to upgrade again", paused=True)
    )
    assert DB_CLIENT.maintenance_db.maintenance.count_documents({}) == 3

    latest_status = maintenance_db.get_latest_status()

    assert latest_status["msg"] == "we need to upgrade again"
    assert latest_status["paused"]
    assert "timestamp" in latest_status.keys()


def test_get_history(mocker: MockFixture):
    mocker.patch.object(
        maintenance_db, "maintenance", DB_CLIENT.maintenance_db.maintenance
    )

    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="first", paused=True)
    )
    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="second", paused=False)
    )
    maintenance_db.set_status(
        MaintenanceStatusRequest(msg="last", paused=True)
    )

    documents = maintenance_db.get_history()

    assert documents[0]["msg"] == "last"
    assert documents[1]["msg"] == "second"
    assert documents[2]["msg"] == "first"
