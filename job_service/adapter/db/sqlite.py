from datetime import datetime, UTC
from pathlib import Path
import logging
import json

import sqlite3

from job_service.exceptions import (
    JobAlreadyCompleteException,
    JobExistsException,
    NotFoundException,
)
from job_service.model.enums import JobStatus
from job_service.model.job import Job, UserInfo, Log
from job_service.model.target import Target
from job_service.model.request import (
    GetJobRequest,
    NewJobRequest,
    UpdateJobRequest,
    MaintenanceStatusRequest,
)

logger = logging.getLogger()

sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())
sqlite3.register_converter(
    "timestamp", lambda s: datetime.fromisoformat(s.decode())
)


class SqliteDbClient:
    db_path: Path

    def __init__(self, db_url: str):
        self.db_path = Path(db_url.replace("sqlite://", ""))
        self._ensure_schema()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self):
        conn = self._conn()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job (
                    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target TEXT,
                    datastore_id INTEGER,
                    status TEXT,
                    created_at TIMESTAMP,
                    created_by TEXT,
                    parameters TEXT,
                    FOREIGN KEY(datastore_id) REFERENCES datastore(datastore_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS maintenance (
                    maintenance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    datastore TEXT,
                    msg TEXT,
                    paused BOOLEAN,
                    timestamp TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS target (
                    name TEXT,
                    datastore TEXT,
                    status TEXT,
                    action TEXT,
                    last_updated_at TIMESTAMP,
                    last_updated_by TEXT,
                    PRIMARY KEY (name, datastore)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_log (
                    job_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER,
                    msg TEXT,
                    at TIMESTAMP,
                    FOREIGN KEY(job_id) REFERENCES job(job_id) ON DELETE CASCADE
                )
            """)
            conn.commit()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS datastore (
                    datastore_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rdn TEXT,
                    description TEXT,
                    directory TEXT,
                    name TEXT
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def _get_job_row_with_logs(
        self, cursor: sqlite3.Cursor, job_id: int | str
    ) -> sqlite3.Row | None:
        job_id = int(job_id)
        job_row = cursor.execute(
            """
            SELECT
                j.job_id,
                j.status,
                j.parameters,
                j.created_at,
                j.created_by,
                CASE 
                    WHEN jl.msg IS NULL THEN '[]'
                    ELSE json_group_array(
                        json_object('at', jl.at, 'message', jl.msg)
                    )
                END AS logs_json
            FROM job j
            LEFT JOIN job_log jl ON j.job_id = jl.job_id
            WHERE j.job_id = ?
            """,
            (job_id,),
        ).fetchone()
        return None if job_row["job_id"] is None else job_row

    def get_job(self, job_id: int | str) -> Job:
        """
        Returns job with matching job_id from database.
        Raises NotFoundException if no such job is found.
        """
        conn = self._conn()
        try:
            cursor = conn.cursor()
            job_id = int(job_id)
            job_row = self._get_job_row_with_logs(cursor, job_id)

            if not job_row:
                raise NotFoundException(f"No job found for jobId: {job_id}")

            return Job(
                job_id=job_row["job_id"],
                status=job_row["status"],
                parameters=json.loads(job_row["parameters"]),
                created_at=job_row["created_at"].isoformat(),
                created_by=json.loads(job_row["created_by"]),
                log=[
                    Log(at=row["at"], message=row["message"])
                    for row in json.loads(job_row["logs_json"])
                ],
            )
        finally:
            conn.close()

    def get_jobs(self, query: GetJobRequest) -> list[Job]:
        """
        Returns list of jobs with matching status from database.
        """
        conn = self._conn()
        try:
            cursor = conn.cursor()
            job_rows = cursor.execute(
                f"""
                SELECT
                    j.job_id,
                    j.status,
                    j.parameters,
                    j.created_at,
                    j.created_by,
                    CASE 
                        WHEN jl.msg IS NULL THEN '[]'
                        ELSE json_group_array(
                            json_object('at', jl.at, 'message', jl.msg)
                        )
                    END AS logs_json
                FROM job j
                LEFT JOIN job_log jl ON j.job_id = jl.job_id
                {query.to_sqlite_where_condition()}
                GROUP BY j.job_id; 
                """,
            ).fetchall()
            if not job_rows:
                return []
            return [
                Job(
                    job_id=job_row["job_id"],
                    status=job_row["status"],
                    parameters=json.loads(job_row["parameters"]),
                    created_at=job_row["created_at"].isoformat(),
                    created_by=json.loads(job_row["created_by"]),
                    log=[
                        Log(at=row["at"], message=row["message"])
                        for row in json.loads(job_row["logs_json"])
                    ],
                )
                for job_row in job_rows
            ]
        finally:
            conn.close()

    def get_jobs_for_target(self, name: str) -> list[Job]:
        """
        Returns list of jobs with matching target name for database.
        Including datastore bump jobs that include the name in
        datastructureUpdates.
        """
        conn = self._conn()
        try:
            cursor = conn.cursor()
            job_rows = cursor.execute(
                """
                SELECT
                    j.job_id,
                    j.status,
                    j.parameters,
                    j.created_at,
                    j.created_by,
                    CASE 
                        WHEN jl.msg IS NULL THEN '[]'
                        ELSE json_group_array(
                            json_object('at', jl.at, 'message', jl.msg)
                        )
                    END AS logs_json
                FROM job j
                LEFT JOIN job_log jl ON j.job_id = jl.job_id
                WHERE j.target = ?
                GROUP BY j.job_id; 
                """,
                (name,),
            ).fetchall()
            if not job_rows:
                return []
            return [
                Job(
                    job_id=job_row["job_id"],
                    status=job_row["status"],
                    parameters=json.loads(job_row["parameters"]),
                    created_at=job_row["created_at"].isoformat(),
                    created_by=json.loads(job_row["created_by"]),
                    log=[
                        Log(at=row["at"], message=row["message"])
                        for row in json.loads(job_row["logs_json"])
                    ],
                )
                for job_row in job_rows
            ]
        finally:
            conn.close()

    def new_job(
        self, new_job_request: NewJobRequest, user_info: UserInfo
    ) -> Job:
        """
        Creates a new job for supplied command, status and dataset_name, and
        returns job_id of created job.
        Raises JobExistsException if job already exists in database.
        """
        job = new_job_request.generate_job_from_request("", user_info)
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 1 FROM job
                WHERE target = ? AND datastore_id = ? AND status NOT IN ('completed', 'failed')
                LIMIT 1
                """,
                (job.parameters.target, 1),
            )
            in_progress_job = cursor.fetchone()
            if not in_progress_job:
                cursor.execute(
                    """
                    INSERT INTO job
                    (target, datastore_id, status, parameters, created_at, created_by)
                    VALUES
                    ( ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job.parameters.target,
                        1,
                        job.status,
                        json.dumps(job.parameters.model_dump(by_alias=True)),
                        job.created_at,
                        json.dumps(job.created_by.model_dump(by_alias=True)),
                    ),
                )
                job_id = cursor.lastrowid
                conn.commit()
                job.job_id = str(job_id)
                return job
            else:
                conn.rollback()
                raise JobExistsException(
                    f"Job already in progress for {job.parameters.target}"
                )
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def update_job(self, job_id: str, body: UpdateJobRequest) -> Job:
        """
        Updates job with supplied job_id with new status, log, or description.
        Ensures atomic, isolated update.
        """
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            job_row = self._get_job_row_with_logs(cursor, job_id)
            if job_row is None:
                raise NotFoundException(f"Could not find job with id {job_id}")
            if job_row["status"] in ["completed", "failed"]:
                raise JobAlreadyCompleteException(
                    f"Job with id {job_id} has already been completed"
                )
            if body.description is not None:
                cursor.execute(
                    "UPDATE job SET parameters = json_set(parameters, $.description, ?) WHERE job_id = ?",
                    (body.description, job_id),
                )

            if body.status is not None:
                cursor.execute(
                    "UPDATE job SET status = ? WHERE job_id = ?",
                    (body.status, job_id),
                )

            if body.log is not None:
                cursor.execute(
                    """
                    INSERT INTO job_log (job_id, msg, at)
                    VALUES (?, ?, ?)
                    """,
                    (job_id, body.log, datetime.now()),
                )
            conn.commit()
            job_row = self._get_job_row_with_logs(cursor, job_id)
            if job_row is None:
                raise Exception(
                    f"Could not find job with id {job_id} after update"
                )
            return Job(
                job_id=job_row["job_id"],
                status=job_row["status"],
                parameters=json.loads(job_row["parameters"]),
                created_at=job_row["created_at"].isoformat(),
                created_by=json.loads(job_row["created_by"]),
                log=[
                    Log(at=row["at"], message=row["message"])
                    for row in json.loads(job_row["logs_json"])
                ],
            )
        except sqlite3.Error as e:
            conn.rollback()
            raise e
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def initialize_maintenance(self) -> dict:
        """
        Inserts an initial maintenance status row if table is empty
        """
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM maintenance")
            count = cursor.fetchone()[0]
            if count == 0:
                timestamp = datetime.now().isoformat()
                cursor.execute(
                    """
                    INSERT INTO maintenance (datastore, msg, paused, timestamp)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        1,
                        "Initial status inserted by at startup.",
                        False,
                        timestamp,
                    ),
                )
                conn.commit()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT msg, paused, timestamp FROM maintenance
                WHERE datastore = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (1,),
            )
            row = cursor.fetchone()
            return {
                "msg": row["msg"],
                "paused": bool(row["paused"]),
                "timestamp": row["timestamp"],
            }
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def get_latest_maintenance_status(self) -> dict:
        """
        Retrieves the latest maintenance status, initializing if necessary
        """
        conn = self._conn()
        try:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT msg, paused, timestamp FROM maintenance
                WHERE datastore = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (1,),
            )
            row = cursor.fetchone()
            if row is None:
                return self.initialize_maintenance()

            return {
                "msg": row["msg"],
                "paused": bool(row["paused"]),
                "timestamp": row["timestamp"],
            }
        finally:
            conn.close()

    def get_maintenance_history(self) -> list:
        """
        Returns full history of maintenance entries, initializing if needed.
        """
        conn = self._conn()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT msg, paused, timestamp FROM maintenance
                WHERE datastore = ?
                ORDER BY timestamp DESC
                """,
                (1,),
            )
            rows = cursor.fetchall()
            if rows:
                return [
                    {
                        "msg": row["msg"],
                        "paused": bool(row["paused"]),
                        "timestamp": row["timestamp"],
                    }
                    for row in rows
                ]
            else:
                return [self.initialize_maintenance()]
        finally:
            conn.close()

    def set_maintenance_status(
        self, status_request: MaintenanceStatusRequest
    ) -> dict:
        """
        Inserts a new maintenance status record.
        """
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            timestamp = datetime.now().isoformat()
            cursor.execute(
                """
                INSERT INTO maintenance (datastore, msg, paused, timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (
                    1,
                    status_request.msg,
                    status_request.paused,
                    timestamp,
                ),
            )
            conn.commit()
            return self.get_latest_maintenance_status()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def get_targets(self) -> list[Target]:
        conn = self._conn()
        try:
            cursor = conn.cursor()
            target_rows = cursor.execute(
                """
                SELECT name, datastore, status, action, last_updated_at, last_updated_by
                FROM target
                WHERE datastore = ?
                """,
                (1,),
            ).fetchall()
            return [
                Target(
                    name=target_row["name"],
                    status=target_row["status"],
                    action=target_row["action"].split(","),
                    last_updated_at=target_row["last_updated_at"].isoformat(),
                    last_updated_by=UserInfo(
                        **json.loads(target_row["last_updated_by"])
                    ),
                )
                for target_row in target_rows
            ]
        finally:
            conn.close()

    def _upsert_one_target(
        self,
        cursor: sqlite3.Cursor,
        name: str,
        status: str,
        timestamp: datetime,
        created_by: str,
        action: str,
    ):
        cursor.execute(
            """
                INSERT INTO target (name, datastore, status, last_updated_at, last_updated_by, action)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(name, datastore) DO UPDATE SET
                    status = excluded.status,
                    last_updated_at = excluded.last_updated_at,
                    last_updated_by = excluded.last_updated_by,
                    action = excluded.action
                """,
            (name, 1, status, timestamp, created_by, action),
        )

    def update_target(self, job: Job) -> None:
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            self._upsert_one_target(
                cursor,
                job.parameters.target,
                job.status,
                datetime.now(),
                json.dumps(
                    job.created_by.model_dump(exclude_none=True, by_alias=True)
                ),
                ",".join(job.get_action()),
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def update_bump_targets(self, job: Job) -> None:
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            updates = [
                update
                for update in job.parameters.bump_manifesto.data_structure_updates
                if update.release_status != "DRAFT"
            ]
            version = job.parameters.bump_to_version
            created_by = json.dumps(
                job.created_by.model_dump(exclude_none=True, by_alias=True)
            )
            cursor = conn.cursor()
            for update in updates:
                operation = (
                    "RELEASED"
                    if update.release_status == "PENDING_RELEASE"
                    else "REMOVED"
                )
                self._upsert_one_target(
                    cursor,
                    update.name,
                    job.status,
                    datetime.now(),
                    created_by,
                    ",".join([operation, str(version)]),
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
