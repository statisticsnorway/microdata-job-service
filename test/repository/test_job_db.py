import pytest
from copy import deepcopy
from datetime import datetime
from pymongo.results import UpdateResult

from job_service.repository.job_db import JobDb
from job_service.exceptions.exceptions import (
    JobExistsException, NotFoundException
)


JOB_ID = "123-123-123-123"
NON_EXISTING_JOB_ID = "abc-abc-abc-abc"
JOB = {
    "_id": None,
    "jobId": JOB_ID,
    "logs": [
        {
            "at": datetime.strptime(
                '01/01/20 00:00:00', '%d/%m/%y %H:%M:%S'
            ),
            "message": "mock"
        }
    ],
    "status": "queued"
}

PROCESSED_JOB = {
    'jobId': '123-123-123-123',
    'logs': [
        {'at': '2020-01-01T00:00:00', 'message': 'mock'}
    ],
    'status': 'queued'
}


class MockedJobsCollection:
    """
    Mocked mongodb collection with basic calls
    """
    def __init__(self, collection_name: str):
        self.collection_name = collection_name

    def find(self, query):
        if self.collection_name == "completed":
            return []
        if query == {} or query.get('status') == 'queued':
            return [deepcopy(JOB)]
        else:
            return []

    def find_one(self, query):
        if self.collection_name == "completed":
            return None
        return deepcopy(JOB) if query['jobId'] == JOB_ID else None

    def update_one(self, find_query, update_query, upsert=False):
        if find_query.get('datasetName') == 'MY_DATASET':
            return UpdateResult({}, acknowledged=True)
        else:
            return UpdateResult(
                {"upserted": "mocked_upserted_id"},
                acknowledged=True
            )

    def delete_one(query):
        ...

    def insert_one(document):
        ...


@pytest.fixture(autouse=True)
def mock_mongo_client(mocker):
    mocker.patch.object(
        JobDb, '__init__', return_value=None
    )
    mocker.patch.object(
        JobDb, 'in_progress', MockedJobsCollection("in_progress")
    )
    mocker.patch.object(
        JobDb, 'completed', MockedJobsCollection("completed")
    )


def test_get_job():
    job_db = JobDb()
    assert job_db.get_job(JOB_ID) == PROCESSED_JOB

    with pytest.raises(NotFoundException) as e:
        job_db.get_job(NON_EXISTING_JOB_ID)
    assert "No job found for jobId:" in str(e)


def test_get_jobs():
    job_db = JobDb()
    assert job_db.get_jobs(status='queued') == [PROCESSED_JOB]
    assert job_db.get_jobs(status='not-a-status') == []


def test_new_job():
    job_db = JobDb()
    assert job_db.new_job('ADD_DATA', 'queued', 'NEW_DATASET') is not None

    with pytest.raises(JobExistsException) as e:
        job_db.new_job('ADD_DATA', 'queued', 'MY_DATASET')
    assert "MY_DATASET already in progress" in str(e)


def test_update_job(mocker):
    job_db = JobDb()
    spy = mocker.patch.object(MockedJobsCollection, 'update_one')
    job_db.update_job("123-123-123-123", status='queued')
    assert spy.call_count == 1

    job_db.update_job("123-123-123-123", status='queued', log="my new log")
    assert spy.call_count == 3


def test_update_job_completed(mocker):
    for status in ["done", "failed"]:
        job_db = JobDb()
        update_spy = mocker.patch.object(MockedJobsCollection, 'update_one')
        insert_spy = mocker.patch.object(MockedJobsCollection, 'insert_one')
        delete_spy = mocker.patch.object(MockedJobsCollection, 'delete_one')
        job_db.update_job("123-123-123-123", status=status, log="my new log")
        assert insert_spy.call_count == 1
        assert delete_spy.call_count == 1
        assert update_spy.call_count == 2
