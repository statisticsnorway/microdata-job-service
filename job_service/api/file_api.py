import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from job_service.api.request_models import ImportRequest
from job_service.exceptions.exceptions import JobExistsException
from job_service.repository.job_db import JobDb
from job_service.repository import local_storage

logger = logging.getLogger()

db = JobDb()
file_api = Blueprint('file_api', __name__)


@file_api.route('/files', methods=['GET'])
def get_files():
    logger.info('GET /files')
    datasets = local_storage.get_input_datasets()
    return jsonify(datasets)


@file_api.route('/files/import', methods=['POST'])
@validate()
def import_dataset_files(body: ImportRequest):
    logger.info(f'POST /files/import with request body {body}')
    response = []
    existing_datasets = []
    for dataset_name in body.datasetList:
        if not local_storage.has_input_dataset(dataset_name):
            response.append({
                "status": "ERROR",
                "dataset": dataset_name,
                "message": "Dataset files not available"
            })
        else:
            existing_datasets.append(dataset_name)
    for dataset_name in existing_datasets:
        try:
            job_id = db.new_job('ADD_DATA', 'queued', dataset_name)
            response.append({
                "status": "OK",
                "dataset": dataset_name,
                "message": f"Dataset import job queued with jobId: {job_id}"
            })
        except JobExistsException as e:
            response.append({
                "status": "ERROR",
                "dataset": dataset_name,
                "message": str(e)
            })
    return jsonify(response)
