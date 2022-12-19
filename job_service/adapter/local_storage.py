import os
from typing import List

from job_service.model.importable_dataset import ImportableDataset
from job_service.config import environment


INPUT_DIR = environment.get('INPUT_DIR')


def get_importable_datasets() -> List[ImportableDataset]:
    """
    Returns names of all valid datasets in input directory.
    """
    importable_datasets = []
    for dataset_name in os.listdir(INPUT_DIR):
        csv_file_exists = os.path.exists(
            f'{INPUT_DIR}/{dataset_name}/{dataset_name}.csv'
        )
        json_file_exists = os.path.exists(
            f'{INPUT_DIR}/{dataset_name}/{dataset_name}.json'
        )
        if not json_file_exists:
            continue
        importable_datasets.append(
            ImportableDataset(
                dataset_name=dataset_name,
                has_data=csv_file_exists,
                has_metadata=json_file_exists
            )
        )
    return importable_datasets
