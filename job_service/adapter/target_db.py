import logging
from typing import List
from datetime import datetime

import pymongo

from job_service.config import environment, secrets
from job_service.model.job import Job
from job_service.model.target import Target


MONGODB_URL = environment.get("MONGODB_URL")
MONGODB_USER = secrets.get("MONGODB_USER")
MONGODB_PASSWORD = secrets.get("MONGODB_PASSWORD")

client = pymongo.MongoClient(
    MONGODB_URL,
    username=MONGODB_USER,
    password=MONGODB_PASSWORD,
    authSource="admin",
)
db = client.jobDB
targets_collection = db.targets

logger = logging.getLogger()


def get_targets() -> List[Target]:
    """
    Returns list of all targets present in the targets database collection.
    """
    targets = targets_collection.find({}, {"_id": 0})
    return [Target(**target) for target in targets if target is not None]


def update_target(job: Job) -> None:
    """
    Updates a target with the supplied job state. If no such
    target exists in the collection, a new target is created
    from the supplied job state.
    """
    targets_collection.update_one(
        {"name": job.parameters.target},
        {
            "$set": {
                "lastUpdatedAt": datetime.now().isoformat(),
                "status": job.status,
                "lastUpdatedBy": job.created_by.dict(by_alias=True),
                "action": job.get_action(),
            }
        },
        upsert=True,
    )


def update_bump_targets(job: Job) -> None:
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
        targets_collection.update_one(
            {"name": update.name},
            {
                "$set": {
                    "lastUpdatedAt": datetime.now().isoformat(),
                    "status": job.status,
                    "lastUpdatedBy": job.created_by.dict(by_alias=True),
                    "action": [operation, version],
                }
            },
            upsert=True,
        )
