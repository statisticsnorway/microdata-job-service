from job_service.repository import local_storage

expected_input_datasets = ['MY_DATASET', 'OTHER_DATASET']


def test_get_input_datasets():
    assert local_storage.get_input_datasets() == expected_input_datasets


def test_has_input_dataset():
    for dataset in expected_input_datasets:
        assert local_storage.has_input_dataset(dataset)
    assert not local_storage.has_input_dataset('NON_EXISTING_DATASET')
