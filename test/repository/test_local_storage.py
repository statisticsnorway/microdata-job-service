from job_service.repository import local_storage

expected_input_datasets = [{'command': 'ADD_OR_CHANGE_DATA', 'datasetName': 'MY_DATASET'},
                           {'command': 'PATCH_METADATA', 'datasetName': 'YOUR_DATASET'},
                           {'command': 'ADD_OR_CHANGE_DATA', 'datasetName': 'OTHER_DATASET'}]


def test_get_input_datasets():
    assert local_storage.get_input_datasets() == expected_input_datasets


def test_has_input_dataset():
    for dataset in expected_input_datasets:
        has_dataset = local_storage.has_input_dataset(dataset['datasetName'])
        assert has_dataset

    has_dataset, command = local_storage.has_input_dataset('NON_EXISTING_DATASET')
    assert (False, None) == (has_dataset, command)
