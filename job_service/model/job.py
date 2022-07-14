import datetime
from typing import List, Optional, Union

from pydantic import Extra, ValidationError, root_validator
from job_service.model.request import Operation

from job_service.model.camelcase_model import CamelModel
from job_service.model.enums import JobStatus, ReleaseStatus


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
    operation: Operation
    target: str
    bump_manifesto: Optional[dict]
    description: Optional[str]
    release_status: Optional[ReleaseStatus]

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_job_type(cls, values):
        operation: Operation = values['operation']
        if (
            operation == Operation.BUMP
            and (
                values['bump_manifesto'] is None or
                values['description'] is None
            )
        ):
            raise ValidationError(
                'No supplied bump manifesto for BUMP operation'
            )
        if (
            operation == Operation.REMOVE
            and values['descripton'] is None
        ):
            raise ValidationError(
                'Missing parameters for REMOVE operation'
            )
        if (
            operation == Operation.SET_STATUS
            and values['release_status'] is None
        ):
            raise ValidationError(
                'Missing parameters for SET STATUS operation'
            )
        return values


class Log(CamelModel, extra=Extra.forbid):
    at: datetime.datetime
    message: str

    def dict(self, **kwargs):   # pylint: disable=unused-argument
        return {'at': self.at.isoformat(), 'message': self.message}


class Job(CamelModel, use_enum_values=True):
    job_id: str
    status: JobStatus
    parameters: JobParameters
    logs: Optional[List[Log]] = []

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_job_type(cls, values):
        if values['logs'] is None:
            values['logs'] = [{
                'at': datetime.datetime.now(),
                'message': 'Job generated and queued'
            }]
        return values
