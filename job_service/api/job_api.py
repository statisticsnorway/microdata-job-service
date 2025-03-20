import logging

from flask import Blueprint, jsonify, request
from job_service.adapter import job_db, target_db
from job_service.api import auth
from job_service.config import environment
from job_service.exceptions import BumpingDisabledException
from job_service.model.request import (
    NewJobsRequest,
    UpdateJobRequest,
    GetJobRequest,
)

logger = logging.getLogger()

job_api = Blueprint("job_api", __name__)


@job_api.get("/jobs")
def get_jobs():
    query_validated = GetJobRequest(**request.json)
    logger.debug(f"GET /jobs with query: {query_validated}")
    jobs = job_db.get_jobs(query_validated)
    return jsonify(
        [job.model_dump(exclude_none=True, by_alias=True) for job in jobs]
    )


@job_api.post("/jobs")
def new_job():
    body_validated = NewJobsRequest(**request.json)
    logger.info(f"POST /jobs with request body: {body_validated}")
    user_info = auth.authorize_user(
        request.cookies.get("authorization"), request.cookies.get("user-info")
    )
    response_list = []
    for job_request in body_validated.jobs:
        try:
            if (
                job_request.target == "DATASTORE"
                and job_request.operation == "BUMP"
                and environment.get("BUMP_ENABLED") is False
            ):
                raise BumpingDisabledException(
                    "Bumping the datastore is disabled"
                )
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
        except BumpingDisabledException as e:
            logger.exception(e)
            response_list.append(
                {
                    "status": "FAILED",
                    "msg": "FAILED: Bumping the datastore is disabled",
                }
            )
        except Exception as e:
            logger.exception(e)
            response_list.append({"status": "FAILED", "msg": "FAILED"})
    return jsonify(response_list), 200


@job_api.get("/jobs/<job_id>")
def get_job(job_id: str):
    logger.info(f"GET /jobs/{job_id}")
    job = job_db.get_job(job_id)
    return job.model_dump(exclude_none=True, by_alias=True)


@job_api.put("/jobs/<job_id>")
def update_job(job_id: str):
    body_validated = UpdateJobRequest(**request.json)
    logger.info(
        f"PUT /jobs/{job_id} with request body: "
        f"{body_validated.model_dump(exclude_none=True, by_alias=True)}"
    )
    job = job_db.update_job(job_id, body_validated)
    target_db.update_target(job)
    if job.parameters.target == "DATASTORE" and job.status == "completed":
        target_db.update_bump_targets(job)
    return {"message": f"Updated job with jobId {job_id}"}
