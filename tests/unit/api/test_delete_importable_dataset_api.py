from flask import url_for
import os

def test_delete_importable_datasets_api(flask_app):
    f = open("tests/resources/input_directory/DATASET_THAT_SHOULD_BE_DELETED", "x")
    response = flask_app.delete(
        url_for("importable_datasets_api.delete_importable_datasets", dataset_name="DATASET_THAT_SHOULD_BE_DELETED")
    )
    assert response.status_code == 200
    assert os.path.exists("tests/resources/input_directory/DATASET_THAT_SHOULD_BE_DELETED") is False

def test_delete_nonexisting_dataset(flask_app):
    response = flask_app.delete(
        url_for("importable_datasets_api.delete_importable_datasets", dataset_name="NONEXISTING_DATASET")
    )
    assert response.status_code == 404