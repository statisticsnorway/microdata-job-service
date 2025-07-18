import logging

from flask import Blueprint, jsonify, request
from job_service.adapter.db import CLIENT
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
    operation_arg = request.args.get("operation") or None
    validated_query = GetJobRequest(
        status=request.args.get("status"),
        operation=operation_arg.split(",")
        if operation_arg is not None
        else None,
        ignoreCompleted=request.args.get("ignoreCompleted"),
    )
    logger.debug(f"GET /jobs with query: {validated_query}")
    jobs = CLIENT.get_jobs(validated_query)
    return jsonify(
        [job.model_dump(exclude_none=True, by_alias=True) for job in jobs]
    )


@job_api.post("/jobs")
def new_job():
    validated_body = NewJobsRequest(**request.json)
    logger.info(f"POST /jobs with request body: {validated_body}")
    user_info = auth.authorize_user(
        request.cookies.get("authorization"), request.cookies.get("user-info")
    )
    response_list = []
    for job_request in validated_body.jobs:
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
                job = CLIENT.new_job(job_request, user_info)
                response_list.append(
                    {
                        "status": "queued",
                        "msg": "CREATED",
                        "job_id": job.job_id,
                    }
                )
            CLIENT.update_target(job)
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
    job = CLIENT.get_job(job_id)
    return job.model_dump(exclude_none=True, by_alias=True)


@job_api.put("/jobs/<job_id>")
def update_job(job_id: str):
    validated_body = UpdateJobRequest(**request.json)
    logger.info(
        f"PUT /jobs/{job_id} with request body: "
        f"{validated_body.model_dump(exclude_none=True, by_alias=True)}"
    )
    job = CLIENT.update_job(job_id, validated_body)
    CLIENT.update_target(job)
    if job.parameters.target == "DATASTORE" and job.status == "completed":
        CLIENT.update_bump_targets(job)
    return {"message": f"Updated job with jobId {job_id}"}
