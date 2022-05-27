import os

from typing import Tuple

from job_service.api.request_models import CommandEnum

INPUT_DIR = os.environ['INPUT_DIR']


def get_importable_datasets() -> list[str]:
    """
    Returns names of all valid datasets in input directory.
    """
    valid_datasets = []
    for dataset_name in os.listdir(INPUT_DIR):
        has_dataset, command = has_importable_dataset(dataset_name)
        if has_dataset:
            valid_datasets.append(
                {"datasetName": dataset_name, "command": command}
            )
    return valid_datasets


def has_importable_dataset(dataset_name) -> Tuple[bool, str]:
    """
    Returns true if dataset with dataset_name exists in input directory,
    and false if not.
    """
    csv_file_exists = os.path.exists(
        f'{INPUT_DIR}/{dataset_name}/{dataset_name}.csv'
    )
    json_file_exists = os.path.exists(
        f'{INPUT_DIR}/{dataset_name}/{dataset_name}.json'
    )

    if csv_file_exists and json_file_exists:
        command = CommandEnum.ADD_OR_CHANGE_DATA.value
    elif json_file_exists:
        command = CommandEnum.PATCH_METADATA.value
    else:
        command = None

    return (csv_file_exists and json_file_exists) or json_file_exists, command
