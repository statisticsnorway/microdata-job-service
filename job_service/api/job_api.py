import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from job_service.adapter import job_db
from job_service.model.job import UserInfo
from job_service.model.request import (
    NewJobsRequest, UpdateJobRequest, GetJobRequest
)


logger = logging.getLogger()

job_api = Blueprint('job_api', __name__)


@job_api.route('/jobs', methods=['GET'])
@validate()
def get_jobs(query: GetJobRequest):
    logger.debug(f'GET /jobs with query: {query}')
    jobs = job_db.get_jobs(query)
    return jsonify([job.dict(by_alias=True) for job in jobs])


@job_api.route('/jobs', methods=['POST'])
@validate()
def new_job(body: NewJobsRequest):
    logger.info(f'POST /jobs with request body: {body}')
    response_list = []
    user_info = UserInfo(
        userId='123-123-123',
        firstName='Data',
        lastName='Admin'
    )
    for job in body.jobs:
        try:
            job_id = job_db.new_job(job, user_info)
            response_list.append({
                'status': 'queued',
                'msg': 'CREATED',
                'job_id': job_id
            })
        except Exception:
            response_list.append({'status': 'FAILED', 'msg': 'FAILED'})
    return jsonify(response_list), 200


@job_api.route('/jobs/<job_id>', methods=['GET'])
@validate()
def get_job(job_id: str):
    logger.info(f'GET /jobs/{job_id}')
    job = job_db.get_job(job_id)
    return job.dict(by_alias=True)


@job_api.route('/jobs/<job_id>', methods=['PUT'])
@validate()
def update_job(body: UpdateJobRequest, job_id: str):
    logger.info(
        f'PUT /jobs/{job_id} with request body: {body.dict(by_alias=True)}'
    )
    job_db.update_job(job_id, body)
    return {'message': f'Updated job with jobId {job_id}'}
