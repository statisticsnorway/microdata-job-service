import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from job_service.adapter import job_db
from job_service.model.request import (
    NewJobRequest, UpdateJobRequest, GetJobRequest
)


logger = logging.getLogger()

job_api = Blueprint('job_api', __name__)


@job_api.route('/jobs', methods=['GET'])
@validate()
def get_jobs(query: GetJobRequest):
    logger.info(f'GET /jobs with query: {query}')
    jobs = job_db.get_jobs(
        status=query.status,
        operations=query.operation,
        ignore_completed=query.ignoreCompleted
    )
    return jsonify([job.dict() for job in jobs])


@job_api.route('/jobs/<job_id>', methods=['GET'])
@validate()
def get_job(job_id: str):
    logger.info(f'GET /jobs/{job_id}')
    job = job_db.get_job(job_id)
    return job.dict()


@job_api.route('/jobs', methods=['POST'])
@validate()
def new_job(body: NewJobRequest):
    logger.info(f'POST /jobs with request body: {body}')
    job_id = job_db.new_job(body.operation, body.status, body.datasetName)
    return {"jobId": job_id}


@job_api.route('/jobs/<job_id>', methods=['PUT'])
@validate()
def update_job(body: UpdateJobRequest, job_id: str):
    logger.info(f'PUT /jobs/{job_id} with request body: {body}')
    job_db.update_job(job_id, body)
    return {'message': f'Updated job with jobId {job_id}'}
