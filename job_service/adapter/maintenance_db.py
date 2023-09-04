import logging
from datetime import datetime

import pymongo

from job_service.config import environment, secrets
from job_service.exceptions import NotFoundException
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
            "pause": status_request.pause,
            "timestamp": str(datetime.now()),
        }
        doc_id = maintenance.insert_one(document)
    except Exception as e:
        logger.error(
            f"Exception occured while setting maintenance status: {document}"
        )
        raise e
    logger.info(
        f"Successfully set maintenance status {document} with id {str(doc_id)}"
    )


def get_latest_status():
    documents = maintenance.find().sort([("timestamp", -1)]).limit(1)
    if not documents:
        raise NotFoundException(
            "No documents found in collection jobDB.maintenance"
        )
    return documents[0]


def get_history():
    documents = maintenance.find().sort([("timestamp", -1)])
    if not documents:
        raise NotFoundException(
            "No documents found in collection jobDB.maintenance"
        )
    return documents
