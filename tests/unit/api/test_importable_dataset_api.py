import os
import shutil

from fastapi.testclient import TestClient

from job_service.app import app

client = TestClient(app)


def teardown_module():
    os.remove("tests/resources/input_directory/DATASET_WITH_INVAL&D_NAM+E.tar")


def test_get_files():
    response = client.get("/importable-datasets")
    assert response.status_code == 200
    assert len(response.json()) == 4
    expected_datasets = [
        {
            "datasetName": "MY_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "YOUR_DATASET",
            "hasData": False,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "OTHER_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "YET_ANOTHER_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": True,
        },
    ]
    for dataset in expected_datasets:
        assert dataset in response.json()


def test_get_invalid_name_files():
    shutil.copyfile(
        "tests/resources/input_directory/MY_DATASET.tar",
        "tests/resources/input_directory/DATASET_WITH_INVAL&D_NAM+E.tar",
    )
    response = client.get("/importable-datasets")
    assert response.status_code == 200
    assert len(response.json()) == 4
    expected_datasets = [
        {
            "datasetName": "MY_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "YOUR_DATASET",
            "hasData": False,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "OTHER_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "YET_ANOTHER_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": True,
        },
    ]
    for dataset in expected_datasets:
        assert dataset in response.json()
