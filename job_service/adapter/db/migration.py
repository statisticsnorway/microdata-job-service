import sqlite3

from flask import json
from pydantic import BaseModel

from job_service.adapter.db.mongo import MongoDbClient
from job_service.config import environment
from job_service.model.job import Job
from job_service.model.request import GetJobRequest


class MigrationConfig(BaseModel):
    datastore_name: str
    datastore_rdn: str
    datastore_description: str
    sqlite_file_path: str
    datastore_directory: str
    migration_api_key: str


def _get_migration_config() -> MigrationConfig:
    migration_config_path = environment.get("MIGRATION_CONFIG_PATH")
    with open(migration_config_path) as f:
        return MigrationConfig.model_validate(json.load(f))


def _insert_datastore_defintion(config: MigrationConfig):
    conn = _conn(config.sqlite_file_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO datastore (name, rdn, description, directory)
        VALUES (?, ?, ?, ?)
        """,
        (
            config.datastore_name,
            config.datastore_rdn,
            config.datastore_description,
            config.datastore_directory,
        ),
    )
    conn.commit()


def _transfer_jobs(mongo_client: MongoDbClient, config: MigrationConfig):
    conn = _conn(config.sqlite_file_path)
    jobs: list[Job] = mongo_client.get_jobs(GetJobRequest())
    cursor = conn.cursor()
    for job in jobs:
        cursor.execute(
            """
            INSERT INTO job (target, datastore_id, status, created_at, created_by, parameters)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                job.parameters.target,
                1,
                job.status,
                job.created_at,
                job.created_by,
                json.dumps(job.parameters),
            ),
        )

        job_id = cursor.lastrowid
        job_logs = job.log if job.log else []
        for log in job_logs:
            cursor.execute(
                """
                INSERT INTO job_log (job_id, msg, at)
                VALUES (?, ?, ?)
                """,
                (
                    job_id,
                    log.message,
                    log.at,
                ),
            )
    conn.commit()


def _transfer_maintenance_history(
    mongo_client: MongoDbClient, config: MigrationConfig
):
    conn = _conn(config.sqlite_file_path)
    maintenance_logs = mongo_client.get_maintenance_history()
    cursor = conn.cursor()
    for maintenance_log in maintenance_logs:
        cursor.execute(
            """
            INSERT INTO maintenance (datastore, msg, paused, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (
                1,
                maintenance_log["msg"],
                maintenance_log["paused"],
                maintenance_log["timestamp"],
            ),
        )
    conn.commit()


def _transfer_targets(mongo_client: MongoDbClient, config: MigrationConfig):
    conn = _conn(config.sqlite_file_path)
    targets = mongo_client.get_targets()
    cursor = conn.cursor()
    for target in targets:
        cursor.execute(
            """
            INSERT INTO target (name, datastore, status, action, last_updated_at, last_updated_by)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                target.name,
                1,
                target.status,
                ",".join(target.action),
                target.last_updated_at,
                json.dumps(target.last_updated_by),
            ),
        )


def _conn(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        db_path,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _ensure_schema(sqlite_file_path: str):
    conn = _conn(sqlite_file_path)
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


def start_migration():
    print("Connecting to mongodb...")
    mongo_client = MongoDbClient()
    print("Reading migration config...")
    config = _get_migration_config()
    print(f"Migration config loaded: {config}")
    print(f"Ensuring table schema @ {config.sqlite_file_path}")
    _ensure_schema(config.sqlite_file_path)
    print("Inserting datastore definiton...")
    _insert_datastore_defintion(config)
    print("Inserting jobs...")
    _transfer_jobs(mongo_client, config)
    print("Inserting maintenance history...")
    _transfer_maintenance_history(mongo_client, config)
    print("Inserting targets...")
    _transfer_targets(mongo_client, config)
    print(
        f"Finished transfering mongodb collections to sqlite file @ {config.sqlite_file_path}"
    )
