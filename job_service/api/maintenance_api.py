import logging

from flask import Blueprint, jsonify, request

from job_service.adapter.db import CLIENT, swap_client
from job_service.model.request import MaintenanceStatusRequest

logger = logging.getLogger()

maintenance_api = Blueprint("maintenance_api", __name__)


@maintenance_api.post("/maintenance-status")
def set_status():
    validated_body = MaintenanceStatusRequest(**request.json)
    logger.info(
        f"POST /maintenance-status with request body: {validated_body}"
    )
    new_status = CLIENT.set_maintenance_status(validated_body)
    return jsonify(new_status), 200


@maintenance_api.get("/maintenance-status")
def get_status():
    document = CLIENT.get_latest_maintenance_status()
    if "paused" in document and document["paused"]:
        logger.info(
            f"GET /maintenance-status, paused: {document['paused']}, msg: {document['msg']}"
        )
    return jsonify(document), 200


@maintenance_api.get("/maintenance-history")
def get_history():
    logger.info("GET /maintenance-history")
    documents = CLIENT.get_maintenance_history()
    return jsonify(documents), 200
