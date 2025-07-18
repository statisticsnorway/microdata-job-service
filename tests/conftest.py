import os
import sqlite3

os.environ["MONGODB_URL"] = "mongodb://localhost:27017/jobdb"
os.environ["SQLITE_URL"] = "test.db"
os.environ["JWKS_URL"] = "http://jwks.test"
os.environ["SECRETS_FILE"] = "tests/resources/secrets/secrets.json"
os.environ["INPUT_DIR"] = "tests/resources/input_directory"
os.environ["DOCKER_HOST_NAME"] = "localhost"
os.environ["STACK"] = "local"
os.environ["BUMP_ENABLED"] = "true"
os.environ["COMMIT_ID"] = "abc123"
os.environ["MIGRATION_CONFIG_PATH"] = "tests/resources/migration_config.json"


def insert_datastore_defintion():
    conn = sqlite3.connect(
        os.environ["SQLITE_URL"],
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS datastore (
                datastore_id INTEGER PRIMARY KEY AUTOINCREMENT,
                rdn TEXT,
                description TEXT,
                directory TEXT,
                name TEXT
            )
        """)
    cursor.execute(
        """
        INSERT INTO datastore (name, rdn, description, directory)
        VALUES (?, ?, ?, ?)
        """,
        (
            "testdatastore",
            "no.ssb.testdatstore",
            "this is a test datastore",
            "resources/TEST_DATASTORE",
        ),
    )
    conn.commit()


insert_datastore_defintion()
