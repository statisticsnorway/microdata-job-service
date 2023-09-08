import logging
from datetime import datetime

import pymongo

from job_service.config import environment, secrets
from job_service.model.request import MaintenanceStatusRequest

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


def set_status(status_request: MaintenanceStatusRequest):
    try:
        document = {
            "msg": status_request.msg,
            "paused": status_request.paused,
            "timestamp": str(datetime.now()),
        }
        doc_id = maintenance.insert_one(document)
        return {"_id": doc_id, **document}
    except Exception as e:
        logger.error(
            f"Exception occured while setting maintenance status: {document}"
        )
        raise e

def get_latest_status():
    cursor = (
        maintenance.find({}, {"_id": 0}).sort([("timestamp", -1)]).limit(1)
    )
    documents = list(cursor)
    if len(documents) == 0:
        return initialize()
    return documents[0]


def get_history():
    cursor = maintenance.find({}, {"_id": 0}).sort([("timestamp", -1)])
    documents = list(cursor)
    if len(documents) == 0:
        return [initialize()]
    return documents

def initialize():
    logger.info("initializing")
    return set_status(
        MaintenanceStatusRequest(
            msg="Initial status inserted by job service at startup.",
            paused=False,
        )
    )