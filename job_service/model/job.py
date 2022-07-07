from typing import List, Optional, Union

from pydantic import Extra, ValidationError, root_validator
from job_service.api.request_models import Operation

from job_service.model.camelcase_model import CamelModel
from job_service.model.enum import JobStatus, ReleaseStatus


class DataStructureUpdate(CamelModel, extra=Extra.forbid):
    name: str
    description: str
    operation: str
    release_status: str


class DatastoreVersion(CamelModel):
    version: str
    description: str
    release_time: int
    language_code: str
    update_type: Union[str, None]
    data_structure_updates: List[DataStructureUpdate]


class JobParameters(CamelModel, use_enum_values=True):
    dataset_name: str
    bump_manifesto: Optional[dict]
    description: Optional[str]
    release_status: Optional[ReleaseStatus]

    @root_validator(skip_on_failure=True)
    @classmethod
    def remove_none_values(cls, values):
        return {
            key: value for key, value in values.items()
            if value is not None
        }


class Job(CamelModel, extra=Extra.forbid, use_enum_values=True):
    job_id: str
    operation: Operation
    status: JobStatus
    parameters: JobParameters

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_job_type(cls, values):
        operation: Operation = values['operation']
        parameters: JobParameters = values['parameters']
        if operation == Operation.BUMP:
            if (
                parameters.bump_manifesto is None or
                parameters.description is None
            ):
                raise ValidationError(
                    'No supplied bump manifesto for BUMP operation'
                )
        elif operation == Operation.SET_STATUS:
            if (
                parameters.dataset_name is None or
                parameters.release_status is None
            ):
                raise ValidationError(
                    'Missing parameters for SET STATUS operation'
                )
        else:
            if parameters.dataset_name is None:
                raise ValidationError(
                    f'Missing parameter for {operation} operation'
                )
        return values
