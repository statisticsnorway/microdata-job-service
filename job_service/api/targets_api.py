import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from job_service.adapter import target_db


logger = logging.getLogger()
targets_api = Blueprint('targets_api', __name__)


@targets_api.route('/targets', methods=['GET'])
@validate()
def get_targets():
    logger.debug('GET /targets')
    targets = target_db.get_targets()
    return jsonify([target.dict(by_alias=True) for target in targets])


@targets_api.route('/targets/<name>', methods=['GET'])
@validate()
def get_target(name: str):
    logger.info(f'GET /jobs/{name}')
    target = target_db.get_target(name)
    return target.dict(by_alias=True)
