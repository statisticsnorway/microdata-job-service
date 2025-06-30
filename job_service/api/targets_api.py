import logging

from flask import Blueprint, jsonify

from job_service.adapter.db import CLIENT


logger = logging.getLogger()
targets_api = Blueprint("targets_api", __name__)


@targets_api.get("/targets")
def get_targets():
    logger.debug("GET /targets")
    targets = CLIENT.get_targets()
    return jsonify(
        [
            target.model_dump(exclude_none=True, by_alias=True)
            for target in targets
        ]
    )


@targets_api.get("/targets/<name>/jobs")
def get_target_jobs(name: str):
    logger.info(f"GET /targets/{name}")
    jobs = CLIENT.get_jobs_for_target(name)
    return [job.model_dump(exclude_none=True, by_alias=True) for job in jobs]
