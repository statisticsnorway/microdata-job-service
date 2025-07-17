import logging

from fastapi import APIRouter
from job_service.adapter.db import CLIENT
from job_service.model.request import MaintenanceStatusRequest

logger = logging.getLogger()

maintenance_api = APIRouter()


@maintenance_api.post("/maintenance-status")
def set_status(maintenance_status_request: MaintenanceStatusRequest):
    logger.info(
        f"POST /maintenance-status with request body: {maintenance_status_request}"
    )
    new_status = CLIENT.set_maintenance_status(maintenance_status_request)
    return new_status


@maintenance_api.get("/maintenance-status")
def get_status():
    document = CLIENT.get_latest_maintenance_status()
    if "paused" in document and document["paused"]:
        logger.info(
            f"GET /maintenance-status, paused: {document['paused']}, msg: {document['msg']}"
        )
    return document


@maintenance_api.get("/maintenance-history")
def get_history():
    logger.info("GET /maintenance-history")
    documents = CLIENT.get_maintenance_history()
    return documents
