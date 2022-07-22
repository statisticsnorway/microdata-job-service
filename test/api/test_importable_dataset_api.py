from flask import url_for


def test_get_files(flask_app):
    response = flask_app.get(
        url_for('importable_datasets_api.get_importable_datasets')
    )
    assert response.status_code == 200
    assert len(response.json) == 3
    expected_datasets = [
        {'datasetName': 'MY_DATASET', 'hasData': True, 'hasMetadata': True},
        {'datasetName': 'YOUR_DATASET', 'hasData': False, 'hasMetadata': True},
        {'datasetName': 'OTHER_DATASET', 'hasData': True, 'hasMetadata': True},
    ]
    for dataset in expected_datasets:
        assert dataset in response.json
