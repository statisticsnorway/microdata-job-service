from typing import List, Optional

from pydantic import BaseModel, Extra, ValidationError, root_validator

from job_service.exceptions.exceptions import BadRequestException
from job_service.model.camelcase_model import CamelModel
from job_service.model.enum import JobStatus, Operation


class NewJobRequest(BaseModel, extra=Extra.forbid):
    operation: Operation
    status: JobStatus
    datasetName: Optional[str]

    @root_validator(skip_on_failure=True)
    def check_command_type(cls, values):
        operation = values['operation']
        if operation in [
            'SET_STATUS', 'ADD', 'CHANGE_DATA', 'PATCH_METADATA', 'REMOVE'
        ]:
            if values.get('datasetName') is None:
                raise BadRequestException(
                    'Must provide a datasetName when '
                    f'operation is {operation}.'
                )
        if operation not in ['PATCH_METADATA', 'ADD', 'CHANGE_DATA']:
            if values.get('status') not in ['done', 'failed']:
                raise BadRequestException(
                    'Unable to set status of synchronous job to in progress'
                )
        return values


class UpdateJobRequest(BaseModel, extra=Extra.forbid):
    status: Optional[JobStatus]
    description: Optional[str]
    log: Optional[str]


class GetJobRequest(BaseModel, extra=Extra.forbid):
    status: Optional[JobStatus]
    operation: Optional[List[Operation]]
    ignoreCompleted: Optional[bool] = False

    @root_validator(pre=True, skip_on_failure=True)
    def validate_query(cls, values):
        if (
            values.get('status', None) is None and
            values.get('operation', None) is None
        ):
            raise ValidationError(
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
            raise Exception()


class ImportableDataset(CamelModel, extra=Extra.forbid):
    datasetName: str
    hasMetadata: str
    hasData: str
