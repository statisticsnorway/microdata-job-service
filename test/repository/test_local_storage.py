import pytest
from job_service.exceptions.exceptions import NoSuchImportableDataset
from job_service.repository import local_storage

expected_datasets = [
    {'operation': 'ADD_OR_CHANGE_DATA', 'datasetName': 'MY_DATASET'},
    {'operation': 'PATCH_METADATA', 'datasetName': 'YOUR_DATASET'},
    {'operation': 'ADD_OR_CHANGE_DATA', 'datasetName': 'OTHER_DATASET'}
]


def test_get_importable_datasets():
    actual_datasets = local_storage.get_importable_datasets()
    assert len(actual_datasets) == 3
    for dataset in expected_datasets:
        assert dataset in actual_datasets


def test_get_importable_dataset_operation():
    for dataset in expected_datasets:
        operation = local_storage.get_importable_dataset_operation(
            dataset['datasetName']
        )
        assert operation == dataset['operation']

    with pytest.raises(NoSuchImportableDataset):
        local_storage.get_importable_dataset_operation(
            'NON_EXISTING_DATASET'
        )
