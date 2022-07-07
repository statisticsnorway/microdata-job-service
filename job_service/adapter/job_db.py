import os
from typing import List
import uuid
from datetime import datetime

from pymongo import MongoClient
from pymongo.results import UpdateResult
from pymongo.errors import DuplicateKeyError
from job_service.model.request import GetJobRequest

from job_service.exceptions.exceptions import (
    JobExistsException, NotFoundException
)
from job_service.model.job import Job


MONGODB_URL = os.environ["MONGODB_URL"]
MONGODB_USER = os.environ["MONGODB_USER"]
MONGODB_PASSWORD = os.environ["MONGODB_PASSWORD"]
client = MongoClient(
    MONGODB_URL,
    username=MONGODB_USER,
    password=MONGODB_PASSWORD,
    authSource="admin"
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

    job = results[0]
    del job["_id"]
    __stringify_datetime_logs(job["logs"])
    return Job(**job)


def get_jobs(query: GetJobRequest) -> List[Job]:
    """
    Returns list of jobs with matching status from database.
    """
    find_query = query.to_mongo_query()
    jobs = [list(in_progress.find(find_query))]
    if not query.ignoreCompleted:
        jobs = jobs + [list(completed.find(find_query))]
    for job in jobs:
        del job["_id"]
        __stringify_datetime_logs(job["logs"])
    return [Job(**job) for job in jobs]


def new_job(
        operation: str,
        status: str,
        dataset_name: str = None,
        log: str = None
) -> str:
    """
    Creates a new job for supplied command, status and dataset_name, and
    returns job_id of created job.
    Raises JobExistsException if job already exists in database.
    """
    job_id = str(uuid.uuid4())
    now = datetime.now()
    job = {
        "jobId": job_id,
        "command": operation,
        "status": status,
        "createdAt": now,
        "logs": [
            {"at": now, "message": f"Set status: {status}"}
        ]
    }
    if dataset_name is not None:
        job["datasetName"] = dataset_name
    if log is not None:
        job["logs"].append({"at": now, "message": log})

    if operation == "ADD_OR_CHANGE_DATA":
        # Atomic insert of new import job
        # Only inserts if no job is found for same datasetname
        # with a currently active status
        update_result = None
        try:
            update_result: UpdateResult = in_progress.update_one(
                {"datasetName": dataset_name},
                {"$setOnInsert": job},
                upsert=True
            )
        except DuplicateKeyError:
            raise JobExistsException(f"{dataset_name} already in progress")

        if update_result.upserted_id is None:
            # Raise exception if active job already existed
            raise JobExistsException(f"{dataset_name} already in progress")
        else:
            return job_id
    else:
        completed.insert_one(job)
        return job_id


def update_job(job_id: str, status: str, log: str = None) -> None:
    """
    Updates job with supplied job_id with new status.
    Appends additional log if supplied.
    """
    now = datetime.now()
    find_query = {"jobId": job_id}
    update_status_query = {
        "$set": {"status": status},
        "$push": {
            "log": {"at": now, "message": f"Set status: {status}"}
        }
    }
    add_log_query = {
        "$push": {
            "logs": {"at": now, "message": log}
        }
    }
    job = in_progress.find_one(find_query)
    if job is None:
        raise NotFoundException(f"Could not find job with id {job_id}")

    if status in ["done", "failed"]:
        in_progress.delete_one(find_query)
        completed.insert_one(job)
        completed.update_one(find_query, update_status_query)
        if log is not None:
            completed.update_one(find_query, add_log_query)
    else:
        in_progress.update_one(find_query, update_status_query)
        if log is not None:
            in_progress.update_one(find_query, add_log_query)


def __stringify_datetime_logs(logs):
    for log in logs:
        log["at"] = log["at"].isoformat()
