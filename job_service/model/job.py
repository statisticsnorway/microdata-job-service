from datetime import datetime
from typing import List, Optional, Union

from pydantic import Extra, root_validator

from job_service.model.camelcase_model import CamelModel
from job_service.model.enums import JobStatus, ReleaseStatus, Operation


class UserInfo(CamelModel, extra=Extra.forbid):
    user_id: str
    first_name: str
    last_name: str


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
    bump_manifesto: Optional[DatastoreVersion]
    description: Optional[str]
    release_status: Optional[ReleaseStatus]
    bump_from_version: Optional[str]
    bump_to_version: Optional[str]

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_job_type(cls, values):
        operation: Operation = values.get('operation')
        if (
            operation == Operation.BUMP
            and (
                values.get('bump_manifesto') is None or
                values.get('description') is None or
                values.get('bump_from_version') is None or
                values.get('bump_to_version') is None or
                values.get('target') != 'DATASTORE'
            )
        ):
            raise ValueError(
                'Invalid or missing arguments for BUMP operation'
            )
        elif (
            operation == Operation.REMOVE
            and values.get('description') is None
        ):
            raise ValueError(
                'Missing parameters for REMOVE operation'
            )
        elif (
            operation == Operation.SET_STATUS
            and values.get('release_status') is None
        ):
            raise ValueError(
                'Missing parameters for SET STATUS operation'
            )
        else:
            return {
                key: value for key, value in values.items()
                if value is not None
            }


class Log(CamelModel, extra=Extra.forbid):
    at: datetime
    message: str

    def dict(self, **kwargs):   # pylint: disable=unused-argument
        return {'at': self.at.isoformat(), 'message': self.message}


class Job(CamelModel, use_enum_values=True):
    job_id: str
    status: JobStatus
    parameters: JobParameters
    log: Optional[List[Log]] = []
    created_at: str
    created_by: UserInfo

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_job_type(cls, values):
        if values['log'] is None:
            values['log'] = [{
                'at': datetime.datetime.now(),
                'message': 'Job generated and queued'
            }]
        return values

    def get_action(self) -> list[str]:
        match(self.parameters.operation):
            case 'SET_STATUS':
                return [
                    self.parameters.operation,
                    self.parameters.release_status
                ]
            case 'BUMP':
                return [
                    self.parameters.operation,
                    self.parameters.bump_from_version,
                    self.parameters.bump_to_version
                ]
            case _:
                return [self.parameters.operation]
