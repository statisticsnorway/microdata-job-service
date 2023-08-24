import logging

import pymongo

from job_service.config import environment, secrets
from job_service.exceptions import NotFoundException

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
maintenance = db.maintenance

logger = logging.getLogger()


def set_upgrade_in_progress(flag: bool):
    try:
        maintenance.update_one(
            {"id": "upgrade_in_progress"},
            {"$set":
                {
                    "flag": flag
                }
            },
            upsert=True,
        )
    except Exception as e:
        logger.error(f"Exception occured while updating upgrade_in_progress with value: {flag}")
        raise e
    logger.info(f"Successfully updated upgrade_in_progress with value: {flag}")


def is_upgrade_in_progress() -> bool:
    result = maintenance.find_one({"id": "upgrade_in_progress"})
    if not result:
        raise NotFoundException(f"No upgrade_in_progress found in collection jobDB.maintenance")
    return result['flag']
