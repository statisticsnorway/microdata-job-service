from datetime import datetime
from typing import List, Optional

from pydantic import Extra, root_validator, ValidationError

from job_service.exceptions import BadQueryException
from job_service.model.camelcase_model import CamelModel
from job_service.model.enums import JobStatus, Operation, ReleaseStatus
from job_service.model.job import (
    DatastoreVersion,
    Job,
    JobParameters,
    UserInfo,
)


class NewJobRequest(CamelModel, extra=Extra.forbid):
    operation: Operation
    target: str
    release_status: Optional[ReleaseStatus]
    description: Optional[str]
    bump_manifesto: Optional[DatastoreVersion]
    bump_from_version: Optional[str]
    bump_to_version: Optional[str]

    @root_validator(skip_on_failure=True)
    def check_command_type(cls, values):  # pylint: disable=no-self-argument
        operation = values["operation"]
        if operation in ["REMOVE", "BUMP"]:
            if values.get("description") is None:
                raise ValidationError(
                    "Must provide a description when "
                    f"operation is {operation}."
                )
        if operation == "SET_STATUS":
            if values.get("release_status") is None:
                raise ValidationError(
                    "Must provide a releaseStatus when "
                    f"operation is {operation}."
                )
        if operation == "BUMP":
            if (
                values.get("bump_manifesto") is None
                or values.get("bump_from_version") is None
                or values.get("bump_to_version") is None
            ):
                raise ValidationError(
                    "Must provide a bumpManifesto, bumpFromVersion and "
                    f"bumpToVersion when operation is {operation}."
                )
        return values

    def generate_job_from_request(
        self, job_id: str, user_info: UserInfo
    ) -> Job:
        if self.operation == "SET_STATUS":
            job_parameters = JobParameters(
                operation=self.operation,
                release_status=self.release_status,
                target=self.target,
            )
        elif self.operation == "REMOVE":
            job_parameters = JobParameters(
                operation=self.operation,
                description=self.description,
                target=self.target,
            )
        elif self.operation == "BUMP":
            job_parameters = JobParameters(
                operation=self.operation,
                bump_manifesto=self.bump_manifesto,
                description=self.description,
                target=self.target,
                bump_from_version=self.bump_from_version,
                bump_to_version=self.bump_to_version,
            )
        else:
            job_parameters = JobParameters(
                target=self.target, operation=self.operation
            )
        return Job(
            job_id=job_id,
            status="queued",
            parameters=job_parameters,
            created_at=datetime.now().isoformat(),
            created_by=user_info,
        )


class NewJobsRequest(CamelModel, extra=Extra.forbid):
    jobs: List[NewJobRequest]


class UpdateJobRequest(CamelModel, extra=Extra.forbid):
    status: Optional[JobStatus]
    description: Optional[str]
    log: Optional[str]


class MaintenanceStatusRequest(CamelModel, extra=Extra.forbid):
    msg: str
    paused: bool


class GetJobRequest(CamelModel, extra=Extra.forbid, use_enum_values=True):
    status: Optional[JobStatus]
    operation: Optional[List[Operation]]
    ignoreCompleted: Optional[bool] = False

    @root_validator(pre=True, skip_on_failure=True)
    def validate_query(cls, values):  # pylint: disable=no-self-argument
        return {
            "status": values.get("status", None),
            "operation": (
                None
                if values.get("operation") is None
                else values.get("operation")[0].split(",")
            ),
            "ignoreCompleted": values.get("ignoreCompleted", False),
        }

    def to_mongo_query(self):
        conditions = [
            (None if self.status is None else {"status": self.status}),
            (
                None
                if self.operation is None
                else {"parameters.operation": {"$in": self.operation}}
            ),
        ]
        conditions = [
            condition for condition in conditions if condition is not None
        ]
        if len(conditions) == 0:
            return {}
        if len(conditions) == 1:
            return conditions[0]
        elif len(conditions) == 2:
            return {"$and": conditions}
        else:
            raise BadQueryException(
                "Unable to transform GetJobRequest to Mongo query"
            )
