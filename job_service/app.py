import logging

from starlette.status import HTTP_400_BAD_REQUEST
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from starlette.responses import JSONResponse

from job_service.api import jobs
from job_service.api import targets
from job_service.api import importable_datasets
from job_service.api import maintenance_status
from job_service.api import observability
from job_service.exceptions import (
    AuthError,
    JobExistsException,
    NotFoundException,
    NameValidationError,
)
from job_service.config.logging import setup_logging


logger = logging.getLogger()

app = FastAPI()
app.include_router(jobs.router)
app.include_router(importable_datasets.router)
app.include_router(targets.router)
app.include_router(maintenance_status.router)
app.include_router(observability.router)

setup_logging(app)


@app.exception_handler(NotFoundException)
def handle_not_found(_req: Request, e: NotFoundException):
    logger.warning(e, exc_info=True)
    return JSONResponse(status_code=404, content={"message": str(e)})


@app.exception_handler(ValidationError)
def handle_bad_request(_req: Request, e: ValidationError):
    logger.warning(e, exc_info=True)
    return JSONResponse(status_code=400, content={"message": str(e)})


@app.exception_handler(JobExistsException)
def handle_job_exists(_req: Request, e: JobExistsException):
    logger.warning(e, exc_info=True)
    return JSONResponse(status_code=400, content={"message": str(e)})


@app.exception_handler(AuthError)
def handle_auth_error(_req: Request, e: AuthError):
    logger.warning(e, exc_info=True)
    return JSONResponse(status_code=401, content={"message": str(e)})


@app.exception_handler(Exception)
def handle_unknown_error(_req: Request, e: Exception):
    logger.exception(e)
    return JSONResponse(
        status_code=500, content={"message": "Internal Server Error"}
    )


@app.exception_handler(NameValidationError)
def handle_invalid_name(_req: Request, e: NameValidationError):
    logger.warning(e, exc_info=True)
    return JSONResponse(status_code=400, content={"message": str(e)})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    _req: Request, e: RequestValidationError
):
    logger.warning("Validation error: %s", e, exc_info=True)
    return JSONResponse(
        status_code=HTTP_400_BAD_REQUEST,
        content={"message": "Bad Request", "details": e.errors()},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
