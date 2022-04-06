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
    "status": "queued",
    "logs": [
        {
            "at": datetime.strptime(
                '01/01/20 00:00:00', '%d/%m/%y %H:%M:%S'
            ),
            "message": "mock"
        }
    ]
}

PROCESSED_JOB = {
    'jobId': '123-123-123-123',
    'logs': [
        {'at': '2020/01/01, 00:00:00', 'message': 'mock'}
    ],
    'status': 'queued'
}


class MockedJobsCollection:
    """
    Mocked mongodb collection with basic calls
    """
    def find(self, query):
        if query == {} or query.get('status') == 'queued':
            return [deepcopy(JOB)]
        else:
            return []

    def find_one(self, query):
        return deepcopy(JOB) if query['jobId'] == JOB_ID else None

    def update_one(self, find_query, update_query, upsert=False):
        if find_query.get('datasetName') == 'MY_DATASET':
            return UpdateResult({}, acknowledged=True)
        else:
            return UpdateResult(
                {"upserted": "mocked_upserted_id"},
                acknowledged=True
            )

    def insert_one(object):
        ...


class MockedMongoClient:
    """
    Mocked MongoClient with one jobs collection
    """
    jobs = MockedJobsCollection()


@pytest.fixture(autouse=True)
def mock_mongo_client(mocker):
    mocker.patch.object(
        JobDb, '__init__', return_value=None
    )
    mocker.patch.object(
        JobDb, 'db', MockedMongoClient
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
    assert "MY_DATASET already imported" in str(e)


def test_update_job(mocker):
    job_db = JobDb()
    spy = mocker.patch.object(MockedJobsCollection, 'update_one')
    job_db.update_job("123-123-123-123", status='queued')
    assert spy.call_count == 2

    job_db.update_job("123-123-123-123", status='queued', log="my new log")
    assert spy.call_count == 5
