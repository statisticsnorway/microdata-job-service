from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Extra, root_validator

from job_service.exceptions.exceptions import BadRequestException


class OperationEnum(str, Enum):
    ADD_OR_CHANGE_DATA = 'ADD_OR_CHANGE_DATA'
    PATCH_METADATA = 'PATCH_METADATA'
    REMOVE_DATASET = 'REMOVE'
    BUMP_VERSION = 'BUMP_VERSION'
    SET_STATUS = 'SET_STATUS'


class StatusEnum(str, Enum):
    QUEUED = 'queued'
    INITIATED = 'initiated'
    VALIDATING = 'validating'
    TRANSFORMING = 'transforming'
    ENRICHING = 'enriching'
    PSEUDONYMIZING = 'pseudonymizing'
    CONVERTING = 'converting'
    IMPORTING = 'importing'
    FAILED = 'failed'
    DONE = 'done'


class NewJobRequest(BaseModel, extra=Extra.forbid):
    operation: OperationEnum
    status: StatusEnum
    datasetName: Optional[str]

    @root_validator(skip_on_failure=True)
    def check_command_type(cls, values):
        operation = values['operation']
        if operation in ['SET_STATUS', 'ADD_OR_CHANGE_DATA', 'PATCH_METADATA']:
            if values.get('datasetName') is None:
                raise BadRequestException(
                    'Must provide a datasetName when '
                    f'operation is {operation}.'
                )
        if operation not in ['PATCH_METADATA', 'ADD_OR_CHANGE_DATA']:
            if values.get('status') not in ['done', 'failed']:
                raise BadRequestException(
                    'Unable to set status of synchronous job to in progress'
                )
        return values


class UpdateJobRequest(BaseModel, extra=Extra.forbid):
    status: StatusEnum
    log: Optional[str]


class GetJobRequest(BaseModel, extra=Extra.forbid):
    status: Optional[str]


class ImportableDataset(BaseModel, extra=Extra.forbid):
    datasetName: str
    operation: str


class ImportRequest(BaseModel, extra=Extra.forbid):
    importableDatasets: List[ImportableDataset]
