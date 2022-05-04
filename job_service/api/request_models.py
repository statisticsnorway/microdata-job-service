from enum import Enum
from typing import Optional

from pydantic import BaseModel, Extra, root_validator

from job_service.exceptions.exceptions import BadRequestException


class CommandEnum(str, Enum):
    ADD_OR_CHANGE_DATA = 'ADD_OR_CHANGE_DATA'
    REMOVE_DATASET = 'REMOVE'
    CHANGE_DATASET = 'IMPORT'
    PATCH_METADATA = 'PATCH_METADATA'
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
    command: CommandEnum
    status: StatusEnum
    datasetName: Optional[str]

    @root_validator(skip_on_failure=True)
    def check_command_type(cls, values):
        command = values['command']
        if command in ['ADD_OR_CHANGE_DATA', 'REMOVE', 'PATCH_METADATA']:
            if values.get('datasetName') is None:
                raise BadRequestException(
                    f'Must provide a datasetName when command is {command}.'
                )
        if command != 'ADD_OR_CHANGE_DATA':
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


class ImportRequest(BaseModel, extra=Extra.forbid):
    datasetList: list
