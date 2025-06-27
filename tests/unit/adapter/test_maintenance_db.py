# pylint: disable=unused-argument
from pytest_mock import MockFixture
from testcontainers.mongodb import MongoDbContainer

from job_service.model.request import MaintenanceStatusRequest
from job_service.adapter.db import CLIENT

mongo = MongoDbContainer("mongo:5.0")
mongo.start()
DB_CLIENT = mongo.get_connection_client()


def teardown_module():
    mongo.stop()


def setup_function():
    DB_CLIENT.jobdb.drop_collection("maintenance")


def test_initialize_after_get_latest_status(mocker: MockFixture):
    mocker.patch.object(CLIENT, "maintenance", DB_CLIENT.jobdb.maintenance)
    latest_status = CLIENT.get_latest_maintenance_status()

    assert (
        latest_status["msg"]
        == "Initial status inserted by job service at startup."
    )
    assert latest_status["paused"] == 0
    assert "timestamp" in latest_status.keys()


def test_initialize_after_get_history(mocker: MockFixture):
    mocker.patch.object(CLIENT, "maintenance", DB_CLIENT.jobdb.maintenance)
    statuses = CLIENT.get_maintenance_history()

    assert (
        statuses[0]["msg"]
        == "Initial status inserted by job service at startup."
    )
    assert statuses[0]["paused"] == 0
    assert "timestamp" in statuses[0].keys()


def test_set_status(mocker: MockFixture):
    mocker.patch.object(CLIENT, "maintenance", DB_CLIENT.jobdb.maintenance)

    status = CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="we upgrade chill", paused=True)
    )
    assert status["msg"] == "we upgrade chill"
    assert status["paused"]
    assert "timestamp" in status
    assert "_id" not in status


def test_set_and_get_status(mocker: MockFixture):
    mocker.patch.object(CLIENT, "maintenance", DB_CLIENT.jobdb.maintenance)

    CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="we upgrade chill", paused=True)
    )
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 1
    CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="finished upgrading", paused=False)
    )
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 2
    CLIENT.set_maintenance_status(
        MaintenanceStatusRequest(msg="we need to upgrade again", paused=True)
    )
    assert DB_CLIENT.jobdb.maintenance.count_documents({}) == 3

    latest_status = CLIENT.get_latest_maintenance_status()

    assert latest_status["msg"] == "we need to upgrade again"
    assert latest_status["paused"]
    assert "timestamp" in latest_status.keys()


def test_get_history(mocker: MockFixture):
    mocker.patch.object(CLIENT, "maintenance", DB_CLIENT.jobdb.maintenance)

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
