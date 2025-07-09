from datetime import datetime
from typing import List, Optional, Union

from pydantic import model_validator, field_serializer

from job_service.model.camelcase_model import CamelModel
from job_service.model.enums import JobStatus, ReleaseStatus, Operation


class UserInfo(CamelModel, extra="forbid"):
    user_id: str
    first_name: str
    last_name: str


class DataStructureUpdate(CamelModel, extra="forbid"):
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
    bump_manifesto: Optional[DatastoreVersion] = None
    description: Optional[str] = None
    release_status: Optional[ReleaseStatus] = None
    bump_from_version: Optional[str] = None
    bump_to_version: Optional[str] = None

    @model_validator(mode="after")
    def validate_job_type(self: "JobParameters"):
        operation: Operation = self.operation
        if operation == Operation.BUMP and (
            self.bump_manifesto is None
            or self.description is None
            or self.bump_from_version is None
            or self.bump_to_version is None
            or self.target != "DATASTORE"
        ):
            raise ValueError("Invalid or missing arguments for BUMP operation")

        elif operation == Operation.REMOVE and self.description is None:
            raise ValueError("Missing parameters for REMOVE operation")
        elif operation == Operation.SET_STATUS and self.release_status is None:
            raise ValueError("Missing parameters for SET STATUS operation")
        else:
            return self


class Log(CamelModel, extra="forbid"):
    at: datetime
    message: str

    @field_serializer("at")
    def serialize_dt(self, at: datetime):
        return at.isoformat()


class Job(CamelModel, use_enum_values=True):
    job_id: str | int
    status: JobStatus
    parameters: JobParameters
    log: Optional[List[Log]] = []
    created_at: str
    created_by: UserInfo

    def get_action(self) -> list[str]:
        match self.parameters.operation:
            case "SET_STATUS":
                return [
                    self.parameters.operation,
                    self.parameters.release_status,
                ]
            case "BUMP":
                return [
                    self.parameters.operation,
                    self.parameters.bump_from_version,
                    self.parameters.bump_to_version,
                ]
            case _:
                return [self.parameters.operation]
