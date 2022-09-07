# pylint: disable=unused-argument
from datetime import datetime

import pytest

from pymongo.results import UpdateResult

from job_service.adapter import job_db
from job_service.exceptions import NotFoundException
from job_service.model.job import Job
from job_service.model.request import (
    GetJobRequest, NewJobRequest, UpdateJobRequest
)


JOB_ID = "123-123-123-123"
NON_EXISTING_JOB_ID = "abc-abc-abc-abc"
JOB = {
    '_id': 'MONGO_DB_ID',
    'jobId': '123-123-123-123',
    'status': 'completed',
    'parameters': {
        'operation': 'ADD',
        'target': 'MY_DATASET'
    },
    'logs': [
        {'at': datetime.now(), 'message': 'example log'}
    ],
    'created_at': '2022-05-18T11:40:22.519222'
}


class MockedInProgressCollection:
    def find_one(self, query):
        if query['jobId'] == NON_EXISTING_JOB_ID:
            return None
        else:
            return JOB

    def find(self, query):
        return [JOB]

    def update_one(self, query, update, *, upsert=False):
        return UpdateResult(
            {"upserted": "mocked_upserted_id"},
            acknowledged=True
        )

    def delete_one(self, query):
        return None


class MockedCompletedCollection:
    def find_one(self, query):
        return None

    def find(self, query):
        return [None]

    def insert_one(self, inserted):
        return None

    def update_one(self, query, update, *, upsert=False):
        return UpdateResult(
            {"upserted": "mocked_upserted_id"},
            acknowledged=True
        )


def test_get_job(mocker):
    mocker.patch.object(
        job_db, 'in_progress', MockedInProgressCollection()
    )
    mocker.patch.object(
        job_db, 'completed', MockedCompletedCollection()
    )
    assert job_db.get_job(JOB_ID) == Job(**JOB)

    with pytest.raises(NotFoundException) as e:
        job_db.get_job(NON_EXISTING_JOB_ID)
    assert "No job found for jobId:" in str(e)


def test_get_jobs(mocker):
    mocker.patch.object(
        job_db, 'in_progress', MockedInProgressCollection()
    )
    mocker.patch.object(
        job_db, 'completed', MockedCompletedCollection()
    )
    get_job_request = GetJobRequest(status='queued')
    assert job_db.get_jobs(get_job_request) == [Job(**JOB)]


def test_new_job(mocker):
    mocker.patch.object(
        job_db, 'in_progress', MockedInProgressCollection()
    )
    mocker.patch.object(
        job_db, 'completed', MockedCompletedCollection()
    )
    assert job_db.new_job(
        NewJobRequest(
            operation='ADD',
            target='NEW_DATASET'
        )
    ) is not None


def test_update_job(mocker):
    mocker.patch.object(
        job_db, 'in_progress', MockedInProgressCollection()
    )
    mocker.patch.object(
        job_db, 'completed', MockedCompletedCollection()
    )
    spy = mocker.spy(MockedInProgressCollection, 'update_one')
    job_db.update_job("123-123-123-123", UpdateJobRequest(status='initiated'))
    assert spy.call_count == 1

    job_db.update_job(
        "123-123-123-123",
        UpdateJobRequest(status='initiated', log='update log')
    )
    assert spy.call_count == 3


def test_new_job_different_created_at():
    job1 = NewJobRequest(
        operation='ADD',
        target='NEW_DATASET'
    ).generate_job_from_request("abc")

    job2 = NewJobRequest(
        operation='ADD',
        target='NEW_DATASET'
    ).generate_job_from_request("def")

    assert job1.created_at != job2.created_at


def test_update_job_completed(mocker):
    mocker.patch.object(
        job_db, 'in_progress', MockedInProgressCollection()
    )
    mocker.patch.object(
        job_db, 'completed', MockedCompletedCollection()
    )
    update_spy = mocker.spy(MockedCompletedCollection, 'update_one')
    insert_spy = mocker.spy(MockedInProgressCollection, 'delete_one')
    delete_spy = mocker.spy(MockedCompletedCollection, 'insert_one')
    for index, status in enumerate(["completed", "failed"]):
        job_db.update_job(
            '123-123-123-123',
            UpdateJobRequest(status=status, log="my new log")
        )
        assert insert_spy.call_count == 1 + index
        assert delete_spy.call_count == 1 + index
        assert update_spy.call_count == 2 + index * 2
