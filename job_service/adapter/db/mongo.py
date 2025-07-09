import logging
import uuid
from datetime import datetime, UTC

import pymongo
from pymongo.errors import DuplicateKeyError
from pymongo.results import UpdateResult
from pymongo.collection import Collection

from job_service.config import environment, secrets
from job_service.exceptions import JobExistsException, NotFoundException
from job_service.model.job import Job, UserInfo
from job_service.model.target import Target
from job_service.model.request import (
    GetJobRequest,
    NewJobRequest,
    UpdateJobRequest,
    MaintenanceStatusRequest,
)

logger = logging.getLogger()


class MongoDbClient:
    completed: Collection
    in_progress: Collection
    targets: Collection
    maintenance: Collection

    def __init__(self):
        MONGODB_URL = environment.get("MONGODB_URL")
        MONGODB_USER = secrets.get("DB_USER")
        MONGODB_PASSWORD = secrets.get("DB_PASSWORD")

        client = pymongo.MongoClient(
            MONGODB_URL,
            username=MONGODB_USER,
            password=MONGODB_PASSWORD,
            authSource="admin",
        )
        db = client.jobDB
        self.completed = db.completed
        self.in_progress = db.inprogress
        self.targets = db.targets
        self.maintenance = db.maintenance

    def get_job(self, job_id: str | int) -> Job:
        """
        Returns job with matching job_id from database.
        Raises NotFoundException if no such job is found.
        """
        results = [
            self.in_progress.find_one({"jobId": job_id}),
            self.completed.find_one({"jobId": job_id}),
        ]
        results = [result for result in results if result is not None]
        if not results:
            raise NotFoundException(f"No job found for jobId: {job_id}")
        return Job(**results[0])

    def get_jobs(self, query: GetJobRequest) -> list[Job]:
        """
        Returns list of jobs with matching status from database.
        """
        find_query = query.to_mongo_query()
        logger.debug(str(find_query))
        jobs = list(self.in_progress.find(find_query))
        if not query.ignoreCompleted:
            jobs = jobs + list(self.completed.find(find_query))
        return [Job(**job) for job in jobs if job is not None]

    def get_jobs_for_target(self, name: str) -> list[Job]:
        """
        Returns list of jobs with matching target name for database.
        Including datastore bump jobs that include the name in
        datastructureUpdates.
        """
        find_query = {
            "$or": [
                {"parameters.bumpManifesto.dataStructureUpdates.name": name},
                {"parameters.target": name},
            ]
        }
        in_progress_jobs = list(self.in_progress.find(find_query))
        completed_jobs = list(self.completed.find(find_query))
        return [Job(**job) for job in in_progress_jobs + completed_jobs]

    def new_job(
        self, new_job_request: NewJobRequest, user_info: UserInfo
    ) -> Job:
        """
        Creates a new job for supplied command, status and dataset_name, and
        returns job_id of created job.
        Raises JobExistsException if job already exists in database.
        """
        job_id = str(uuid.uuid4())
        job = new_job_request.generate_job_from_request(job_id, user_info)
        update_result: UpdateResult | None = None
        logger.info(f"Inserting new job {job}")
        try:
            update_result = self.in_progress.update_one(
                {"parameters.target": job.parameters.target},
                {
                    "$setOnInsert": job.model_dump(
                        exclude_none=True, by_alias=True
                    )
                },
                upsert=True,
            )
        except DuplicateKeyError as e:
            raise JobExistsException(
                f"Job with target {job.parameters.target} already in progress"
            ) from e
        except Exception as e:
            raise e
        if update_result.upserted_id is None:
            logger.error(
                f"Job with target {job.parameters.target} already in progress"
            )
            raise JobExistsException(
                f"Job with target {job.parameters.target} already in progress"
            )
        logger.info(f"Successfully inserted new job: {job}")
        return job

    def update_job(self, job_id: str, body: UpdateJobRequest) -> Job:
        """
        Updates job with supplied job_id with new status.
        Appends additional log if supplied.
        """
        now = datetime.now(UTC).replace(tzinfo=None)
        find_query = {"jobId": job_id}
        job: Job | None = self.in_progress.find_one(find_query)
        if job is None:
            raise NotFoundException(f"Could not find job with id {job_id}")

        update_query = {
            "$set": {},
            "$push": {"log": {"at": now, "message": ""}},
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
                f"Job status updated to {body.status}. "
                "moving job to completed collection"
            )
            self.in_progress.delete_one(find_query)
            self.completed.insert_one(job)
            self.completed.update_one(find_query, update_query)
            if body.log is not None:
                self.completed.update_one(find_query, log_update_query)
            return Job(**self.completed.find_one(find_query))
        else:
            self.in_progress.update_one(find_query, update_query)
            if body.log is not None:
                self.in_progress.update_one(find_query, log_update_query)
            return Job(**self.in_progress.find_one(find_query))

    def set_maintenance_status(
        self, status_request: MaintenanceStatusRequest
    ) -> dict:
        document = None
        try:
            document = {
                "msg": status_request.msg,
                "paused": status_request.paused,
                "timestamp": str(datetime.now()),
            }
            doc_id = self.maintenance.insert_one(document).inserted_id
            return self.maintenance.find_one(
                {"_id": doc_id}, projection={"_id": False}
            )
        except Exception as e:
            logger.error(
                f"Exception occured while setting maintenance status: {document}"
            )
            raise e

    def get_latest_maintenance_status(self) -> dict:
        cursor = (
            self.maintenance.find({}, {"_id": 0})
            .sort([("timestamp", -1)])
            .limit(1)
        )
        documents = list(cursor)
        if len(documents) == 0:
            return self.initialize_maintenance()
        return documents[0]

    def get_maintenance_history(self) -> list:
        cursor = self.maintenance.find({}, {"_id": 0}).sort(
            [("timestamp", -1)]
        )
        documents = list(cursor)
        if len(documents) == 0:
            return [self.initialize_maintenance()]
        return documents

    def initialize_maintenance(self) -> dict:
        logger.info("initializing")
        return self.set_maintenance_status(
            MaintenanceStatusRequest(
                msg="Initial status inserted by job service at startup.",
                paused=False,
            )
        )

    def get_targets(self) -> list[Target]:
        targets = self.targets.find({}, {"_id": 0})
        return [Target(**target) for target in targets if target is not None]

    def update_target(self, job: Job) -> None:
        self.targets.update_one(
            {"name": job.parameters.target},
            {
                "$set": {
                    "lastUpdatedAt": datetime.now().isoformat(),
                    "status": job.status,
                    "lastUpdatedBy": job.created_by.model_dump(
                        exclude_none=True, by_alias=True
                    ),
                    "action": job.get_action(),
                }
            },
            upsert=True,
        )

    def update_bump_targets(self, job: Job) -> None:
        updates = [
            update
            for update in job.parameters.bump_manifesto.data_structure_updates
            if update.release_status != "DRAFT"
        ]
        for update in updates:
            operation = (
                "RELEASED"
                if update.release_status == "PENDING_RELEASE"
                else "REMOVED"
            )
            version = job.parameters.bump_to_version
            self.targets.update_one(
                {"name": update.name},
                {
                    "$set": {
                        "lastUpdatedAt": datetime.now().isoformat(),
                        "status": job.status,
                        "lastUpdatedBy": job.created_by.model_dump(
                            exclude_none=True, by_alias=True
                        ),
                        "action": [operation, version],
                    }
                },
                upsert=True,
            )
