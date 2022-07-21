from typing import List
import uuid
from datetime import datetime
import logging
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
db = client.jobDB
completed = db.completed
in_progress = db.inprogress

logger = logging.getLogger()


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
    logger.info(str(find_query))
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
    logger.info(f'inserting new job {job}')
    try:
        update_result: UpdateResult = in_progress.update_one(
            {"target": job.parameters.target},
            {"$setOnInsert": job.dict(by_alias=True)},
            upsert=True
        )
    except DuplicateKeyError as e:
        raise JobExistsException(
            f'Job with target {job.target} already in progress'
        ) from e
    except Exception as e:
        raise e
    if update_result.upserted_id is None:
        logger.error(f'Job with target {job.target} already in progress')
        raise JobExistsException(
            f'Job with target {job.target} already in progress'
        )
    logger.info(f'Successfully inserted new job: {job}')
    return job_id


def update_job(job_id: str, body: UpdateJobRequest) -> None:
    """
    Updates job with supplied job_id with new status.
    Appends additional log if supplied.
    """
    now = datetime.now()
    find_query = {"jobId": job_id}
    job = in_progress.find_one(find_query)
    if job is None:
        raise NotFoundException(f"Could not find job with id {job_id}")

    update_query = {
        "$set": {},
        "$push": {
            "log": {
                "at": now,
                "message": ""
            }
        }
    }
    if body.status is not None:
        update_query["$set"] = {"status": body.status}
        update_query["$push"]["log"]["message"] = (
            f"Set status: {body.status}"
        )
    elif body.description is not None:
        update_query["$set"] = {"parameters.description": body.description}
        update_query["$push"]["log"]["message"] = (
            "Added update description"
        )
    log_update_query = {"$push": {"log": {"at": now, "message": body.log}}}
    if body.status in ["completed", "failed"]:
        logger.info(
            f'Job status updated to {body.status}. '
            'moving job to completed collection'
        )
        in_progress.delete_one(find_query)
        completed.insert_one(job)
        completed.update_one(find_query, update_query)
        if body.log is not None:
            completed.update_one(find_query, log_update_query)
    else:
        in_progress.update_one(find_query, update_query)
        if body.log is not None:
            in_progress.update_one(find_query, log_update_query)
