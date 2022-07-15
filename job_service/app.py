import uuid
import logging
import json_logging

from flask import Flask

from job_service.api.job_api import job_api
from job_service.api.importable_datasets_api import importable_datasets_api
from job_service.exceptions import (
    JobExistsException, NotFoundException, BadRequestException
)
from job_service.config.logging import (
    CustomJSONLog, CustomJSONRequestLogFormatter
)


def init_json_logging():
    json_logging.CREATE_CORRELATION_ID_IF_NOT_EXISTS = True
    json_logging.CORRELATION_ID_GENERATOR = (
        lambda: "job-service-" + str(uuid.uuid1())
    )
    json_logging.init_flask(enable_json=True, custom_formatter=CustomJSONLog)
    json_logging.init_request_instrument(
        app, custom_formatter=CustomJSONRequestLogFormatter
    )


logging.getLogger("json_logging").setLevel(logging.WARNING)

app = Flask(__name__)
app.register_blueprint(job_api)
app.register_blueprint(importable_datasets_api)


@app.errorhandler(NotFoundException)
def handle_not_found(e):
    return {"message": str(e)}, 404


@app.errorhandler(BadRequestException)
def handle_bad_request(e):
    return {"message": str(e)}, 400


@app.errorhandler(JobExistsException)
def handle_job_exists(e):
    return {"message": str(e)}, 400


@app.errorhandler(Exception)
def handle_unknown_error(e):  # pylint: disable=unused-argument
    return {"message": "Internal Server Error"}, 500
