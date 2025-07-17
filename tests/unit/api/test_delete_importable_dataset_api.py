import os
from fastapi.testclient import TestClient

from job_service.app import app

client = TestClient(app)


def test_delete_importable_datasets_api():
    open(
        "tests/resources/input_directory/DATASET_THAT_SHOULD_BE_DELETED.tar",
        "x",
    )
    response = client.delete(
        "/importable-datasets/DATASET_THAT_SHOULD_BE_DELETED",
    )
    assert response.status_code == 200
    assert not os.path.exists(
        "tests/resources/input_directory/DATASET_THAT_SHOULD_BE_DELETED.tar"
    )


def test_delete_nonexisting_dataset():
    response = client.delete(
        "/importable-datasets/NONEXISTING_DATASET",
    )
    assert response.status_code == 404


def test_delete_invalid_name_dataset():
    response = client.delete("/importable-datasets/INVALID_NAME_DATASET++")
    assert response.status_code == 400
