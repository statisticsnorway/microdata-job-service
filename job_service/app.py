import sys
import uuid
import logging
import json_logging

from flask import Flask
from pydantic import ValidationError

from job_service.api.job_api import job_api
from job_service.api.targets_api import targets_api
from job_service.api.importable_datasets_api import importable_datasets_api
from job_service.exceptions import (
    JobExistsException, NotFoundException
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


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

app = Flask(__name__)
app.register_blueprint(job_api)
app.register_blueprint(importable_datasets_api)
app.register_blueprint(targets_api)

init_json_logging()


@app.errorhandler(NotFoundException)
def handle_not_found(e):
    logger.exception(e)
    return {"message": str(e)}, 404


@app.errorhandler(ValidationError)
def handle_bad_request(e):
    logger.exception(e)
    return {"message": str(e)}, 400


@app.errorhandler(JobExistsException)
def handle_job_exists(e):
    logger.exception(e)
    return {"message": str(e)}, 400


@app.errorhandler(Exception)
def handle_unknown_error(e):
    logger.exception(e)
    return {"message": "Internal Server Error"}, 500


# this is needed to run the application in IDE
if __name__ == "__main__":
    app.run(port=8000, host="0.0.0.0")
