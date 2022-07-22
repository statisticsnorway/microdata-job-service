import json
import platform
import sys

import json_logging
import tomlkit


def _get_project_meta():
    with open('pyproject.toml', encoding='utf-8') as pyproject:
        file_contents = pyproject.read()

    return tomlkit.parse(file_contents)['tool']['poetry']


pkg_meta = _get_project_meta()
service_name = "job-service"
host = platform.node()
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
        json_log_object = (
            super(CustomJSONRequestLogFormatter, self)._format_log_object(
                record, request_util
            )
        )
        return create_microdata_json_log(json_log_object, record)


def create_microdata_json_log(json_log_object, record):
    return {
        "@timestamp": json_log_object['written_at'],
        "command": command,
        "error.stack": json_log_object.get('exc_info'),
        "host": host,
        "message": record.getMessage(),
        "level": record.levelno,
        "levelName": record.levelname,
        "loggerName": record.name,
        "method": json_log_object.get('method'),
        "responseTime": json_log_object.get('response_time_ms'),
        "schemaVersion": "v3",
        "serviceName": service_name,
        "serviceVersion": str(pkg_meta['version']),
        "source_host": json_log_object.get('remote_host'),
        "statusCode": json_log_object.get('response_status'),
        "thread": record.threadName,
        "url": json_log_object.get('request'),
        "xRequestId": json_log_object.get('correlation_id')
    }
