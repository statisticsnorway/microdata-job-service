from typing import List

from job_service.model.camelcase_model import CamelModel
from job_service.model.enums import JobStatus
from job_service.model.job import UserInfo


class Target(CamelModel, use_enum_values=True, extra="forbid"):
    name: str
    last_updated_at: str
    status: JobStatus
    last_updated_by: UserInfo
    action: List[str]
