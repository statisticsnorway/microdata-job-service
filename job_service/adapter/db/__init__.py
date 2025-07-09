from typing import Protocol

from job_service.adapter.db.mongo import MongoDbClient
from job_service.adapter.db.sqlite import SqliteDbClient
from job_service.adapter.db.migration import _get_migration_config
from job_service.config import environment
from job_service.model.job import Job, UserInfo
from job_service.model.target import Target
from job_service.model.request import (
    GetJobRequest,
    NewJobRequest,
    UpdateJobRequest,
    MaintenanceStatusRequest,
)


class DatabaseClient(Protocol):
    def get_job(self, job_id: int | str) -> Job: ...
    def get_jobs(self, query: GetJobRequest) -> list[Job]: ...
    def get_jobs_for_target(self, name: str) -> list[Job]: ...
    def new_job(
        self, new_job_request: NewJobRequest, user_info: UserInfo
    ) -> Job: ...
    def update_job(self, job_id: str, body: UpdateJobRequest) -> Job: ...
    def set_maintenance_status(
        self, status_request: MaintenanceStatusRequest
    ) -> dict: ...
    def get_latest_maintenance_status(self) -> dict: ...
    def get_maintenance_history(self) -> list[dict]: ...
    def initialize_maintenance(self) -> dict: ...
    def get_targets(self) -> list[Target]: ...
    def update_target(self, job: Job) -> None: ...
    def update_bump_targets(self, job: Job) -> None: ...


def get_database_client() -> DatabaseClient:
    return MongoDbClient()


CLIENT = get_database_client()


def swap_client(x_api_key: str):
    global CLIENT
    migration_config = _get_migration_config()
    if migration_config.migration_api_key != x_api_key:
        raise Exception("Invalid API key")
    if isinstance(CLIENT, MongoDbClient):
        CLIENT = SqliteDbClient(environment.get("SQLITE_URL"))
    else:
        CLIENT = MongoDbClient()
