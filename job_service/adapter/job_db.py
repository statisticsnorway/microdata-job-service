from typing import List
import uuid
from datetime import datetime

import pymongo
from pymongo.results import UpdateResult
from pymongo.errors import DuplicateKeyError
from job_service.model.request import (
    GetJobRequest, NewJobRequest, UpdateJobRequest
)
from job_service.config import environment
from job_service.exceptions import (
    JobExistsException, NotFoundException
)
from job_service.model.job import Job


MONGODB_URL = environment.get('MONGODB_URL')
MONGODB_USER = environment.get('MONGODB_USER')
MONGODB_PASSWORD = environment.get('MONGODB_PASSWORD')

client = pymongo.MongoClient(
    MONGODB_URL,
    username=MONGODB_USER,
    password=MONGODB_PASSWORD,
    authSource='admin'
)
db = client.jobdb
completed = db.completed
in_progress = db.inprogress


def get_job(job_id: str) -> Job:
    """
    Returns job with matching job_id from database.
    Raises NotFoundException if no such job is found.
    """
    results = [
        in_progress.find_one({"jobId": job_id}),
        completed.find_one({"jobId": job_id})
    ]
    results = [result for result in results if result is not None]
    if not results:
        raise NotFoundException(f"No job found for jobId: {job_id}")
    return Job(**results[0])


def get_jobs(query: GetJobRequest) -> List[Job]:
    """
    Returns list of jobs with matching status from database.
    """
    find_query = query.to_mongo_query()
    jobs = list(in_progress.find(find_query))
    if not query.ignoreCompleted:
        jobs = jobs + list(completed.find(find_query))
    return [Job(**job) for job in jobs if job is not None]


def new_job(new_job_request: NewJobRequest) -> str:
    """
    Creates a new job for supplied command, status and dataset_name, and
    returns job_id of created job.
    Raises JobExistsException if job already exists in database.
    """
    job_id = str(uuid.uuid4())
    job = new_job_request.generate_job_from_request(job_id)
    update_result = None
    try:
        update_result: UpdateResult = in_progress.update_one(
            {"target": job.parameters.target},
            {"$setOnInsert": job},
            upsert=True
        )
    except DuplicateKeyError:
        raise JobExistsException(  # pylint: disable=raise-missing-from
            f'Job with target {job.target} already in progress'
        )
    if update_result.upserted_id is None:
        raise JobExistsException(
            f'Job with target {job.target} already in progress'
        )
    return job_id


def update_job(job_id: str, body: UpdateJobRequest) -> None:
    """
    Updates job with supplied job_id with new status.
    Appends additional log if supplied.
    """
    now = datetime.now()
    find_query = {"jobId": job_id}
    update_status_query = {
        "$set": {"status": body.status},
        "$push": {
            "log": {
                "at": now,
                "message": f"Set status: {body.status}"
            }
        }
    }
    add_log_query = {
        "$push": {
            "logs": {"at": now, "message": body.log}
        }
    }
    job = in_progress.find_one(find_query)
    if job is None:
        raise NotFoundException(f"Could not find job with id {job_id}")

    if body.status in ["completed", "failed"]:
        in_progress.delete_one(find_query)
        completed.insert_one(job)
        completed.update_one(find_query, update_status_query)
        if body.log is not None:
            completed.update_one(find_query, add_log_query)
    else:
        in_progress.update_one(find_query, update_status_query)
        if body.log is not None:
            in_progress.update_one(find_query, add_log_query)
