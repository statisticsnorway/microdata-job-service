from flask import url_for
import os

def test_delete_importable_datasets_api(flask_app):
    open(
        "tests/resources/input_directory/DATASET_THAT_SHOULD_BE_DELETED.tar",
        "x",
    )
    response = flask_app.delete(
        url_for(
            "importable_datasets_api.delete_importable_datasets",
            dataset_name="DATASET_THAT_SHOULD_BE_DELETED",
        )
    )
    assert response.status_code == 200
    assert not os.path.exists(
        "tests/resources/input_directory/DATASET_THAT_SHOULD_BE_DELETED.tar"
    )


def test_delete_nonexisting_dataset(flask_app):
    response = flask_app.delete(
        url_for(
            "importable_datasets_api.delete_importable_datasets",
            dataset_name="NONEXISTING_DATASET",
        )
    )
    assert response.status_code == 404


def test_delete_invalid_name_dataset(flask_app):
    response = flask_app.delete(
        url_for(
            "importable_datasets_api.delete_importable_datasets",
            dataset_name="INVALID_NAME_DATASET++",
        )
    )
    assert response.status_code == 400
