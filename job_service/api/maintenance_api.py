import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from job_service.adapter import maintenance_db
from job_service.model.request import MaintenanceStatusRequest

logger = logging.getLogger()

maintenance_api = Blueprint("maintenance_api", __name__)


@maintenance_api.route("/maintenance-status", methods=["POST"])
@validate()
def set_status(body: MaintenanceStatusRequest):
    logger.info(f"POST /maintenance-status with request body: {body}")
    maintenance_db.set_status(body)
    response = {
        "status": "SUCCESS",
        "msg": body.dict()["msg"],
        "pause": body.dict()["pause"],
    }
    return jsonify(response), 200


@maintenance_api.route("/maintenance-status", methods=["GET"])
def get_status():
    logger.info("GET /maintenance-status")
    document = maintenance_db.get_latest_status()
    return jsonify(document), 200


@maintenance_api.route("/maintenance-history", methods=["GET"])
def get_history():
    logger.info("GET /maintenance-history")
    documents = maintenance_db.get_history()
    return jsonify(documents), 200
