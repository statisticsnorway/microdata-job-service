from flask import url_for
import os
import shutil


def teardown_module():
    os.remove("tests/resources/input_directory/DATASET_WITH_INVAL&D_NAM+E.tar")


def test_get_files(flask_app):
    response = flask_app.get(
        url_for("importable_datasets_api.get_importable_datasets")
    )
    assert response.status_code == 200
    assert len(response.json) == 4
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
        assert dataset in response.json


def test_get_invalid_name_files(flask_app):
    shutil.copyfile(
        "tests/resources/input_directory/MY_DATASET.tar",
        "tests/resources/input_directory/DATASET_WITH_INVAL&D_NAM+E.tar",
    )
    response = flask_app.get(
        url_for("importable_datasets_api.get_importable_datasets")
    )
    assert response.status_code == 200
    assert len(response.json) == 4
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
        assert dataset in response.json
