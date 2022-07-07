from typing import List, Optional

from pydantic import Extra, root_validator

from job_service.exceptions import BadQueryException, BadRequestException
from job_service.model.camelcase_model import CamelModel
from job_service.model.enum import JobStatus, Operation, ReleaseStatus
from job_service.model.job import DatastoreVersion


class NewJobRequest(CamelModel, extra=Extra.forbid):
    operation: Operation
    releaseStatus: ReleaseStatus
    datasetName: Optional[str]
    description: Optional[str]
    bumpManifesto: Optional[DatastoreVersion]

    @root_validator(skip_on_failure=True)
    def check_command_type(cls, values):
        operation = values['operation']
        if operation in [
            'SET_STATUS', 'ADD', 'CHANGE_DATA',
            'PATCH_METADATA', 'REMOVE', 'DELETE_DRAFT'
        ]:
            if values.get('datasetName') is None:
                raise BadRequestException(
                    'Must provide a datasetName when '
                    f'operation is {operation}.'
                )
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


class UpdateJobRequest(CamelModel, extra=Extra.forbid):
    status: Optional[JobStatus]
    description: Optional[str]
    log: Optional[str]


class GetJobRequest(CamelModel, extra=Extra.forbid):
    status: Optional[JobStatus]
    operation: Optional[List[Operation]]
    ignoreCompleted: Optional[bool] = False

    @root_validator(pre=True, skip_on_failure=True)
    def validate_query(cls, values):
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


class ImportableDataset(CamelModel, extra=Extra.forbid):
    datasetName: str
    hasMetadata: str
    hasData: str
