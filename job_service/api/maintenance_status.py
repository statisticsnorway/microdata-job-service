import logging

from fastapi import APIRouter, Depends
from job_service.adapter import db
from job_service.model.camelcase_model import CamelModel

logger = logging.getLogger()

router = APIRouter()


class MaintenanceStatusRequest(CamelModel, extra="forbid"):
    msg: str
    paused: bool


@router.post("/maintenance-status")
def set_status(
    maintenance_status_request: MaintenanceStatusRequest,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    logger.info(
        f"POST /maintenance-status with request body: {maintenance_status_request}"
    )
    new_status = database_client.set_maintenance_status(
        maintenance_status_request
    )
    return new_status


@router.get("/maintenance-status")
def get_status(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    document = database_client.get_latest_maintenance_status()
    if "paused" in document and document["paused"]:
        logger.info(
            f"GET /maintenance-status, paused: {document['paused']}, msg: {document['msg']}"
        )
    return document


@router.get("/maintenance-history")
def get_history(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    logger.info("GET /maintenance-history")
    documents = database_client.get_maintenance_history()
    return documents
