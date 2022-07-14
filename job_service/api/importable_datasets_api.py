import logging

from flask import Blueprint, jsonify

from job_service.adapter import local_storage


logger = logging.getLogger()

importable_datasets_api = Blueprint('importable_datasets_api', __name__)


@importable_datasets_api.route('/importable_datasets', methods=['GET'])
def get_importable_datasets():
    logger.info('GET /importable-datasets')
    datasets = local_storage.get_importable_datasets()
    return jsonify([dataset.dict(by_alias=True) for dataset in datasets])
