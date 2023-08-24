import logging

from flask import Blueprint, jsonify

from job_service.adapter import maintenance_db

logger = logging.getLogger()

maintenance_api = Blueprint("maintenance_api", __name__)


@maintenance_api.route("/prepare-for-upgrade", methods=["POST"])
def prepare_for_upgrade():
    maintenance_db.set_upgrade_in_progress(True)
    logger.info(f"POST /prepare-for-upgrade has set upgrade status = True")
    return jsonify({"status": "SUCCESS", "msg": "PREPARE_FOR_UPGRADE"}), 200


@maintenance_api.route("/upgrade-done", methods=["POST"])
def upgrade_done():
    maintenance_db.set_upgrade_in_progress(False)
    logger.info(f"POST /upgrade-done has set upgrade status = False")
    return jsonify({"status": "SUCCESS", "msg": "UPGRADE_DONE"}), 200
