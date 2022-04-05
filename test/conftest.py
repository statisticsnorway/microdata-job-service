import pytest

from job_service.app import app


# Set up test application for all tests in session
@pytest.fixture(scope="session")
def flask_app():
    client = app.test_client()
    ctx = app.test_request_context()
    ctx.push()
    yield client
    ctx.pop()
