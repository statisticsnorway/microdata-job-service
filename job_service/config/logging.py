import re
import sys
import uuid
import json
import logging
import datetime
from time import perf_counter_ns

import tomlkit
from flask import request, g

from job_service.config import environment


def _get_project_meta():
    with open("pyproject.toml", encoding="utf-8") as pyproject:
        file_contents = pyproject.read()
    return tomlkit.parse(file_contents)["tool"]["poetry"]


class MicrodataJSONFormatter(logging.Formatter):
    def __init__(self):
        self.pkg_meta = _get_project_meta()
        self.host = environment.get("DOCKER_HOST_NAME")
        self.command = json.dumps(sys.argv)

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "@timestamp": datetime.datetime.fromtimestamp(
                    record.created,
                    tz=datetime.timezone.utc,
                ).isoformat(),
                "command": self.command,
                "error.stack": record.__dict__.get("exc_info"),
                "host": self.host,
                "message": record.getMessage(),
                "level": record.levelno,
                "levelName": record.levelname,
                "loggerName": record.name,
                "method": request.method,
                "responseTime": getattr(g, "response_time_ms"),
                "schemaVersion": "v3",
                "serviceName": "job-service",
                "serviceVersion": str(self.pkg_meta["version"]),
                "source_host": request.remote_addr,
                "statusCode": getattr(g, "response_status"),
                "thread": record.threadName,
                "url": request.url,
                "xRequestId": re.sub(
                    r"[^\w\-]", "", request.headers.get("X-Request-ID", "")
                ),
            }
        )


def setup_logging(app, log_level: int = logging.INFO) -> None:
    logger = logging.getLogger()
    logger.setLevel(log_level)

    formatter = MicrodataJSONFormatter()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    @app.before_request
    def before_request():
        g.start_time = perf_counter_ns()
        correlation_id = request.headers.get("X-Request-ID", None)
        if correlation_id is None:
            g.correlation_id = "job-service-" + str(uuid.uuid1())
        else:
            g.correlation_id = correlation_id
        g.method = request.method
        g.url = request.url
        g.remote_host = request.remote_addr

    @app.after_request
    def after_request(response):
        g.response_time_ms = int(
            (perf_counter_ns() - g.start_time) / 1_000_000
        )
        g.response_status = response.status_code
        response.headers["X-Request-ID"] = g.correlation_id
        return response
