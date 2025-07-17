import logging
from typing import Optional

from fastapi import APIRouter, Query, Cookie

from job_service.adapter.db import CLIENT
from job_service.api import auth
from job_service.config import environment
from job_service.exceptions import BumpingDisabledException
from job_service.model.job import JobStatus, Operation
from job_service.model.request import (
    NewJobsRequest,
    UpdateJobRequest,
    GetJobRequest,
)

logger = logging.getLogger()

job_api = APIRouter()


@job_api.get("/jobs")
def get_jobs(
    status: Optional[str] = Query(None),
    operation: Optional[str] = Query(None),
    ignoreCompleted: Optional[bool] = Query(None),
):
    validated_query = GetJobRequest(
        status=JobStatus(status),
        operation=[Operation(op) for op in operation.split(",")]
        if operation is not None
        else None,
        ignoreCompleted=ignoreCompleted,
    )
    logger.debug(f"GET /jobs with query: {validated_query}")
    return [
        job.model_dump(exclude_none=True, by_alias=True)
        for job in CLIENT.get_jobs(validated_query)
    ]


@job_api.post("/jobs")
def new_job(
    validated_body: NewJobsRequest,
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None),
):
    logger.info(f"POST /jobs with request body: {validated_body}")
    parsed_user_info = auth.authorize_user(authorization, user_info)
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
                job = CLIENT.new_job(job_request, parsed_user_info)
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
    return response_list


@job_api.get("/jobs/{job_id}")
def get_job(job_id: str):
    logger.info(f"GET /jobs/{job_id}")
    return CLIENT.get_job(job_id).model_dump(exclude_none=True, by_alias=True)


@job_api.put("/jobs/{job_id}")
def update_job(job_id: str, validated_body: UpdateJobRequest):
    logger.info(
        f"PUT /jobs/{job_id} with request body: "
        f"{validated_body.model_dump(exclude_none=True, by_alias=True)}"
    )
    job = CLIENT.update_job(job_id, validated_body)
    CLIENT.update_target(job)
    if job.parameters.target == "DATASTORE" and job.status == "completed":
        CLIENT.update_bump_targets(job)
    return {"message": f"Updated job with jobId {job_id}"}
