from typing import List

from pydantic import Extra

from job_service.model.enums import JobStatus
from job_service.model.camelcase_model import CamelModel


class UserInfo(CamelModel, extra=Extra.forbid):
    user_id: str
    first_name: str
    last_name: str


class Target(CamelModel, use_enum_values=True):
    name: str
    jobs: List[str]
    last_update: str
    last_updated_by: UserInfo
    last_status: JobStatus
