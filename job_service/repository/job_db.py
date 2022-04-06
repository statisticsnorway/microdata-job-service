import os
import uuid
from datetime import datetime

from pymongo import MongoClient
from pymongo.results import UpdateResult

from job_service.exceptions.exceptions import (
    JobExistsException, NotFoundException
)


MONGODB_URL = os.environ["MONGODB_URL"]
MONGODB_USER = os.environ["MONGODB_USER"]
MONGODB_PASSWORD = os.environ["MONGODB_PASSWORD"]


class JobDb:
    db = None

    def __init__(self):
        client = MongoClient(
            MONGODB_URL,
            username=MONGODB_USER,
            password=MONGODB_PASSWORD
        )
        self.db = client.jobdb

    def get_job(self, job_id: str) -> dict:
        """
        Returns job with matching job_id from database.
        Raises NotFoundException if no such job is found.
        """
        job = self.db.jobs.find_one({"jobId": job_id})
        if job is None:
            raise NotFoundException(f"No job found for jobId: {job_id}")

        del job["_id"]
        self.__stringify_datetime_logs(job["logs"])
        return job

    def get_jobs(self, status: str = None) -> list:
        """
        Returns list of jobs with matching status from database.
        """
        db_query = {} if status is None else {"status": status}
        jobs = list(self.db.jobs.find(db_query))
        for job in jobs:
            del job["_id"]
            self.__stringify_datetime_logs(job["logs"])
        return jobs

    def new_job(self, command: str, status: str,
                dataset_name: str = None) -> str:
        """
        Creates a new job for supplied command, status and dataset_name, and
        returns job_id of created job.
        Raises JobExistsException if job already exists in database.
        """
        job_id = str(uuid.uuid4())
        now = datetime.now()
        job = {
            "jobId": job_id,
            "command": command,
            "status": status,
            "createdAt": now,
            "logs": [
                {"at": now, "message": "Set status: {status}"}
            ]
        }
        if dataset_name is not None:
            job["datasetName"] = dataset_name
        if command == "ADD_DATA":
            # Atomic insert of new import job
            # Only inserts if no job is found for same datasetname
            # with a currently active status
            update_result: UpdateResult = self.db.jobs.update_one(
                {
                    "datasetName": dataset_name,
                    "status": {"$nin": ["done", "failed"]}
                },
                {"$setOnInsert": job},
                upsert=True
            )

            if update_result.upserted_id is None:
                # Raise exception if active job already existed
                raise JobExistsException(f"{dataset_name} already imported")
            else:
                return job_id
        else:
            self.db.jobs.insert_one(job)
            return job_id

    def update_job(self, job_id: str, status: str, log: str = None) -> None:
        """
        Updates job with supplied job_id with new status.
        Appends additional log if supplied.
        """
        self.db.jobs.update_one(
            {"jobId": job_id},
            {"$set": {"status": status}}
        )
        now = datetime.now()
        self.db.jobs.update_one(
            {
                "jobId": job_id
            },
            {
                "$push": {
                    "log": {"at": now, "message": f"Set status: {status}"}
                }
            }
        )
        if log is not None:
            self.db.jobs.update_one(
                {
                    "jobId": job_id
                },
                {
                    "$push": {
                        "logs": {"at": now, "message": log}
                    }
                }
            )

    def __stringify_datetime_logs(self, logs):
        for log in logs:
            log["at"] = log["at"].strftime("%Y/%m/%d, %H:%M:%S")
