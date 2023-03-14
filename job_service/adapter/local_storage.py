import os
from pathlib import Path
from typing import List

from job_service.model.importable_dataset import ImportableDataset
from job_service.config import environment

INPUT_DIR = Path(environment.get('INPUT_DIR'))
ARCHIVE_DIR = INPUT_DIR / 'archive'


def get_importable_datasets() -> List[ImportableDataset]:
    """
    Returns names of all valid datasets in input directory.
    """
    datasets = [
        importable_dataset for importable_dataset in
        [
            ImportableDataset(
                dataset_name=dataset_name,
                has_data=os.path.exists(
                    INPUT_DIR / f'{dataset_name}/{dataset_name}.csv'
                ),
                has_metadata=os.path.exists(
                    INPUT_DIR / f'{dataset_name}/{dataset_name}.json'
                )
            )
            for dataset_name in os.listdir(INPUT_DIR)
        ]
        if importable_dataset.has_metadata
    ]

    if ARCHIVE_DIR.exists():
        datasets = datasets + [
            archived_dataset for archived_dataset in
            [
                ImportableDataset(
                    dataset_name=dataset_name,
                    has_data=os.path.exists(
                        ARCHIVE_DIR / f'{dataset_name}/{dataset_name}.csv'
                    ),
                    has_metadata=os.path.exists(
                        ARCHIVE_DIR / f'{dataset_name}/{dataset_name}.json'
                    ),
                    is_archived=True
                )
                for dataset_name in os.listdir(ARCHIVE_DIR)
            ]
            if archived_dataset.has_metadata
        ]

    return datasets
