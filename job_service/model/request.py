from typing import List, Optional

from pydantic import Extra, root_validator

from job_service.exceptions import BadQueryException, BadRequestException
from job_service.model.camelcase_model import CamelModel
from job_service.model.enums import JobStatus, Operation, ReleaseStatus
from job_service.model.job import DatastoreVersion, Job, JobParameters


class NewJobRequest(CamelModel, extra=Extra.forbid):
    operation: Operation
    target: str
    release_status: Optional[ReleaseStatus]
    description: Optional[str]
    bump_manifesto: Optional[DatastoreVersion]

    @root_validator(skip_on_failure=True)
    def check_command_type(cls, values):  # pylint: disable=no-self-argument
        operation = values['operation']
        if operation in ['REMOVE', 'BUMP']:
            if values.get('description') is None:
                raise BadRequestException(
                    'Must provide a description when '
                    f'operation is {operation}.'
                )
        if operation == 'SET_STATUS':
            if values.get('releaseStatus') is None:
                raise BadRequestException(
                    'Must provide a releaseStatus when '
                    f'operation is {operation}.'
                )
        if operation == 'BUMP':
            if values.get('bumpManifesto') is None:
                raise BadRequestException(
                    'Must provide a bumpManifesto when '
                    f'operation is {operation}.'
                )
        return values

    def generate_job_from_request(self, job_id: str) -> Job:
        if self.operation == 'SET_STATUS':
            job_parameters = JobParameters(
                operation=self.operation,
                release_status=self.release_status,
                target=self.target
            )
        elif self.operation == 'REMOVE':
            job_parameters = JobParameters(
                operation=self.operation,
                description=self.description,
                target=self.target
            )
        elif self.operation == 'BUMP':
            job_parameters = JobParameters(
                operation=self.operation,
                bump_manifesto=self.bump_manifesto,
                description=self.description,
                target=self.target
            )
        else:
            job_parameters = JobParameters(
                target=self.target,
                operation=self.operation
            )
        return Job(
            job_id=job_id,
            status='queued',
            parameters=job_parameters
        )


class NewJobsRequest(CamelModel, extra=Extra.forbid):
    jobs: List[NewJobRequest]


class UpdateJobRequest(CamelModel, extra=Extra.forbid):
    status: Optional[JobStatus]
    description: Optional[str]
    log: Optional[str]


class GetJobRequest(CamelModel, extra=Extra.forbid):
    status: Optional[JobStatus]
    operation: Optional[List[Operation]]
    ignoreCompleted: Optional[bool] = False

    @root_validator(pre=True, skip_on_failure=True)
    def validate_query(cls, values):  # pylint: disable=no-self-argument
        if (
            values.get('status', None) is None and
            values.get('operation', None) is None
        ):
            raise BadRequestException(
                'Query must include either "status" or "operation"'
            )
        operation_list = None
        if values.get('operation', None) is not None:
            operation_list = values['operation'][0].split(',')
        return {
            'status': values.get('status', None),
            'operation': operation_list,
            'ignoreCompleted': values.get('ignoreCompleted', False)
        }

    def to_mongo_query(self):
        conditions = [
            (
                None if self.status is None
                else {"status": self.status}),
            (
                None if self.status is None
                else {"operation": {"$in": self.operation}})
        ]
        conditions = [
            condition for condition in conditions if condition is not None
        ]
        if len(conditions) == 1:
            return conditions[1]
        elif len(conditions) == 2:
            return {"$and": conditions}
        else:
            raise BadQueryException(
                'Unable to transform GetJobRequest to Mongo query'
            )
