import os
from typing import List

from job_service.model.importable_dataset import ImportableDataset
from job_service.config import environment


INPUT_DIR = environment.get('INPUT_DIR')


def get_importable_datasets() -> List[ImportableDataset]:
    """
    Returns names of all valid datasets in input directory.
    """
    return [
        importable_dataset for importable_dataset in
        [
            ImportableDataset(
                dataset_name=dataset_name,
                has_data=os.path.exists(
                    f'{INPUT_DIR}/{dataset_name}/{dataset_name}.csv'
                ),
                has_metadata=os.path.exists(
                    f'{INPUT_DIR}/{dataset_name}/{dataset_name}.json'
                )
            )
            for dataset_name in os.listdir(INPUT_DIR)
        ]
        if importable_dataset.has_metadata
    ]
