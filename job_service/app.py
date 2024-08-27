import logging

from flask import Flask
from pydantic import ValidationError
from werkzeug.exceptions import HTTPException

from job_service.api.job_api import job_api
from job_service.api.targets_api import targets_api
from job_service.api.importable_datasets_api import importable_datasets_api
from job_service.api.maintenance_api import maintenance_api
from job_service.exceptions import (
    AuthError,
    JobExistsException,
    NotFoundException,
    NameValidationError,
)
from job_service.config.logging import setup_logging


logger = logging.getLogger()

app = Flask(__name__)
app.register_blueprint(job_api)
app.register_blueprint(importable_datasets_api)
app.register_blueprint(targets_api)
app.register_blueprint(maintenance_api)

setup_logging(app)


@app.errorhandler(NotFoundException)
def handle_not_found(e):
    logger.warning(e, exc_info=True)
    return {"message": str(e)}, 404


@app.errorhandler(ValidationError)
def handle_bad_request(e):
    logger.warning(e, exc_info=True)
    return {"message": str(e)}, 400


@app.errorhandler(JobExistsException)
def handle_job_exists(e):
    logger.warning(e, exc_info=True)
    return {"message": str(e)}, 400


@app.errorhandler(AuthError)
def handle_auth_error(e):
    logger.warning(e, exc_info=True)
    return {"message": str(e)}, 401


@app.errorhandler(Exception)
def handle_unknown_error(e):
    logger.exception(e)
    return {"message": "Internal Server Error"}, 500


@app.errorhandler(NameValidationError)
def handle_invalid_name(e):
    logger.warning(e, exc_info=True)
    return {"message": str(e)}, 400


@app.errorhandler(HTTPException)
def handle_http_exception(e: HTTPException):
    if str(e.code).startswith("4"):
        logger.warning(e, exc_info=True)
        error_message = str(e.description)
    else:
        logger.exception(e)
        error_message = "Internal Server Error"
    return {"message": f"{error_message}"}, e.code


# this is needed to run the application in IDE
if __name__ == "__main__":
    app.run(port=8000, host="0.0.0.0")
