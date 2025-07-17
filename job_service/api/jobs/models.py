from datetime import datetime
from typing import List, Optional

from pydantic import model_validator

from job_service.model.camelcase_model import CamelModel
from job_service.adapter.db.models import JobStatus, Operation, ReleaseStatus
from job_service.adapter.db.models import (
    DatastoreVersion,
    Job,
    JobParameters,
    UserInfo,
)


class NewJobRequest(CamelModel, extra="forbid", use_enum_values=True):
    operation: Operation
    target: str
    release_status: Optional[ReleaseStatus] = None
    description: Optional[str] = None
    bump_manifesto: Optional[DatastoreVersion] = None
    bump_from_version: Optional[str] = None
    bump_to_version: Optional[str] = None

    @model_validator(mode="after")
    def check_command_type(self: "NewJobRequest"):  # pylint: disable=no-self-argument
        operation = self.operation
        if operation in ["REMOVE", "BUMP"]:
            if self.description is None:
                raise TypeError(
                    "Must provide a description when "
                    f"operation is {operation}."
                )
        if operation == "SET_STATUS":
            if self.release_status is None:
                raise TypeError(
                    "Must provide a releaseStatus when "
                    f"operation is {operation}."
                )
        if operation == "BUMP":
            if (
                self.bump_manifesto is None
                or self.bump_from_version is None
                or self.bump_to_version is None
            ):
                raise TypeError(
                    "Must provide a bumpManifesto, bumpFromVersion and "
                    f"bumpToVersion when operation is {operation}."
                )
        return self

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


class NewJobsRequest(CamelModel, extra="forbid"):
    jobs: List[NewJobRequest]


class UpdateJobRequest(CamelModel, extra="forbid"):
    status: Optional[JobStatus] = None
    description: Optional[str] = None
    log: Optional[str] = None
