import re
import sys
import uuid
import json
import logging
import datetime
from time import perf_counter_ns

from flask import request, g

from job_service.config import environment


class MicrodataJSONFormatter(logging.Formatter):
    def __init__(self):
        self.host = environment.get("DOCKER_HOST_NAME")
        self.command = json.dumps(sys.argv)
        self.commit_id = environment.get("COMMIT_ID")

    def format(self, record: logging.LogRecord) -> str:
        response_time_ms = getattr(g, "response_time_ms")
        response_status = getattr(g, "response_status")
        x_request_id = getattr(g, "correlation_id")
        request_method = request.method
        request_remote_addr = request.remote_addr
        request_url = request.url

        return json.dumps(
            {
                "@timestamp": datetime.datetime.fromtimestamp(
                    record.created,
                    tz=datetime.timezone.utc,
                ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                + "Z",
                "command": self.command,
                "error.stack": record.__dict__.get("exc_info"),
                "host": self.host,
                "message": record.getMessage(),
                "level": record.levelno,
                "levelName": record.levelname,
                "loggerName": record.name,
                "method": request_method,
                "responseTime": response_time_ms,
                "schemaVersion": "v3",
                "serviceName": "job-service",
                "serviceVersion": self.commit_id,
                "source_host": request_remote_addr,
                "statusCode": response_status,
                "thread": record.threadName,
                "url": request_url,
                "xRequestId": re.sub(r"[^\w\-]", "", x_request_id),
            }
        )


def setup_logging(app, log_level: int = logging.INFO) -> None:
    logger = logging.getLogger()
    logger.setLevel(log_level)
    formatter = MicrodataJSONFormatter()
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    @app.before_request
    def before_request():
        g.response_time_ms = 0
        g.response_status = ""
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
        logger.info("responded")
        return response
