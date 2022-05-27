import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from job_service.api.request_models import ImportRequest
from job_service.exceptions.exceptions import JobExistsException
from job_service.repository.job_db import JobDb
from job_service.repository import local_storage

logger = logging.getLogger()

db = JobDb()
importable_datasets_api = Blueprint('importable_datasets_api', __name__)


@importable_datasets_api.route('/importable_datasets', methods=['GET'])
def get_importable_datasets():
    logger.info('GET /importable-datasets')
    datasets = local_storage.get_importable_datasets()
    return jsonify(datasets)


@importable_datasets_api.route('/importable-datasets', methods=['POST'])
@validate()
def import_datasets(body: ImportRequest):
    logger.info(f'POST /importable-datasets/import with request body {body}')
    response = []
    existing_datasets = []
    for dataset_name in body.datasetList:
        if not local_storage.has_importable_dataset(dataset_name):
            response.append({
                "status": "ERROR",
                "dataset": dataset_name,
                "message": "Importable datasets not available"
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
