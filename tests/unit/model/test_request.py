import json

import pytest
from pydantic import ValidationError

from job_service.model.job import Job, UserInfo
from job_service.model.request import NewJobsRequest, NewJobRequest


RESOURCE_DIR = "tests/resources/model"
VALID_JOB_REQUESTS_PATH = f"{RESOURCE_DIR}/valid_job_requests.json"
with open(VALID_JOB_REQUESTS_PATH, "r", encoding="utf-8") as f:
    VALID_JOB_REQUESTS = json.load(f)
INVALID_JOB_REQUESTS_PATH = f"{RESOURCE_DIR}/invalid_job_requests.json"
with open(INVALID_JOB_REQUESTS_PATH, "r", encoding="utf-8") as f:
    INVALID_JOB_REQUESTS = json.load(f)
JOB_ID = "123-123-123-123"
USER_INFO_DICT = {
    "userId": "123-123-123",
    "firstName": "Data",
    "lastName": "Admin",
}
USER_INFO = UserInfo(**USER_INFO_DICT)


def test_new_jobs_requests():
    new_jobs_request = NewJobsRequest(**VALID_JOB_REQUESTS)
    for job_request in new_jobs_request.jobs:
        assert job_request.target is not None
        assert job_request.operation is not None
        assert isinstance(
            job_request.generate_job_from_request(JOB_ID, USER_INFO), Job
        )


def test_invalid_new_jobs_requests():
    for job in INVALID_JOB_REQUESTS["jobs"]:
        with pytest.raises((ValidationError, TypeError)):
            NewJobRequest(**job)
