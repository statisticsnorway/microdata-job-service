import json
import sys

import json_logging

from job_service.config import environment

commit_id = environment.get("COMMIT_ID")
service_name = "job-service"
host = environment.get("DOCKER_HOST_NAME")
command = json.dumps(sys.argv)


class CustomJSONLog(json_logging.JSONLogWebFormatter):
    """
    Customized application logger
    """

    def _format_log_object(self, record, request_util):
        json_log_object = super(CustomJSONLog, self)._format_log_object(
            record, request_util
        )
        return create_microdata_json_log(json_log_object, record)


class CustomJSONRequestLogFormatter(json_logging.JSONRequestLogFormatter):
    """
    Customized request logger
    """

    def _format_log_object(self, record, request_util):
        json_log_object = super(
            CustomJSONRequestLogFormatter, self
        )._format_log_object(record, request_util)
        return create_microdata_json_log(json_log_object, record)


def create_microdata_json_log(json_log_object, record):
    return {
        "@timestamp": json_log_object["written_at"],
        "command": command,
        "error.stack": json_log_object.get("exc_info"),
        "host": host,
        "message": record.getMessage(),
        "level": record.levelno,
        "levelName": record.levelname,
        "loggerName": record.name,
        "method": json_log_object.get("method"),
        "responseTime": json_log_object.get("response_time_ms"),
        "schemaVersion": "v3",
        "serviceName": service_name,
        "serviceVersion": commit_id,
        "source_host": json_log_object.get("remote_host"),
        "statusCode": json_log_object.get("response_status"),
        "thread": record.threadName,
        "url": json_log_object.get("request"),
        "xRequestId": json_log_object.get("correlation_id"),
    }
