import logging

from flask import Blueprint, jsonify, request
from flask_pydantic import validate

from job_service.api import auth
from job_service.adapter import job_db, target_db
from job_service.model.request import (
    NewJobsRequest,
    UpdateJobRequest,
    GetJobRequest,
)
from job_service.config import environment

logger = logging.getLogger()

job_api = Blueprint("job_api", __name__)


@job_api.route("/jobs", methods=["GET"])
@validate()
def get_jobs(query: GetJobRequest):
    logger.debug(f"GET /jobs with query: {query}")
    jobs = job_db.get_jobs(query)
    return jsonify([job.dict(by_alias=True) for job in jobs])


@job_api.route("/jobs", methods=["POST"])
@validate()
def new_job(body: NewJobsRequest):
    logger.info(f"POST /jobs with request body: {body}")
    user_info = auth.authorize_user(
        request.cookies.get("authorization"), request.cookies.get("user-info")
    )
    response_list = []
    for job_request in body.jobs:
        try:
            if (
                job_request.target == "DATASTORE"
                and job_request.operation == "BUMP"
                and environment.get("BUMP_ENABLED") is False
            ):
                raise Exception("Bumping the datastore is disabled")
            else:
                job = job_db.new_job(job_request, user_info)
                response_list.append(
                    {
                        "status": "queued",
                        "msg": "CREATED",
                        "job_id": job.job_id,
                    }
                )
            target_db.update_target(job)
        except Exception as e:
            logger.exception(e)
            response_list.append(
                {"status": "FAILED", "msg": f"FAILED: {str(e)}"}
            )
    return jsonify(response_list), 200


@job_api.route("/jobs/<job_id>", methods=["GET"])
@validate()
def get_job(job_id: str):
    logger.info(f"GET /jobs/{job_id}")
    job = job_db.get_job(job_id)
    return job.dict(by_alias=True)


@job_api.route("/jobs/<job_id>", methods=["PUT"])
@validate()
def update_job(body: UpdateJobRequest, job_id: str):
    logger.info(
        f"PUT /jobs/{job_id} with request body: {body.dict(by_alias=True)}"
    )
    job = job_db.update_job(job_id, body)
    target_db.update_target(job)
    if job.parameters.target == "DATASTORE" and job.status == "completed":
        target_db.update_bump_targets(job)
    return {"message": f"Updated job with jobId {job_id}"}


def fail_bump_job_if_configured(job):
    if (
        job.parameters.target == "DATASTORE"
        and job.parameters.operation == "BUMP"
        and environment.get("BUMP_ENABLED") is False
    ):
        update_job(
            UpdateJobRequest(
                status="failed",
                log="Bumping the datastore is disabled",
            ),
            job.job_id,
        )
