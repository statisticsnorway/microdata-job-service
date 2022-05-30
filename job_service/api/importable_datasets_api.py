import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from job_service.api.request_models import ImportRequest
from job_service.exceptions.exceptions import (
    JobExistsException, NoSuchImportableDataset
)
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
    logger.info(f'POST /importable-datasets with request body {body}')
    response = []
    for dataset in body.importableDatasets:
        dataset_name = dataset.datasetName
        dataset_operation = dataset.operation
        try:
            operation = local_storage.get_importable_dataset_operation(
                dataset_name
            )
            assert dataset_operation == operation
            job_id = db.new_job(operation, 'queued', dataset_name)
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
        except NoSuchImportableDataset as e:
            response.append({
                "status": "ERROR",
                "dataset": dataset_name,
                "message": str(e)
            })
        except Exception as e:
            logger.error(f"Unexpected error when importing dataset: {str(e)}")
            response.append({
                "status": "ERROR",
                "dataset": dataset_name,
                "message": "Unexpected error when importing dataset"
            })
    return jsonify(response)
