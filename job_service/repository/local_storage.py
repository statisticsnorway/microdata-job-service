import os

from job_service.exceptions.exceptions import NoSuchImportableDataset
from job_service.api.request_models import OperationEnum

INPUT_DIR = os.environ['INPUT_DIR']


def get_importable_datasets() -> list[str]:
    """
    Returns names of all valid datasets in input directory.
    """
    valid_datasets = []
    for dataset_name in os.listdir(INPUT_DIR):
        try:
            operation = get_importable_dataset_operation(dataset_name)
            valid_datasets.append(
                {"datasetName": dataset_name, "operation": operation}
            )
        except NoSuchImportableDataset:
            continue
    return valid_datasets


def get_importable_dataset_operation(dataset_name) -> str:
    """
    Returns the operation of the importable dataset. PATCH_METADATA
    if only .json file exists, and ADD_OR_CHANGE_DATA if both
    .csv file and .json file exists.
    Raises a NoSuchImportableDataset error if importable dataset
    does not exist.
    """
    csv_file_exists = os.path.exists(
        f'{INPUT_DIR}/{dataset_name}/{dataset_name}.csv'
    )
    json_file_exists = os.path.exists(
        f'{INPUT_DIR}/{dataset_name}/{dataset_name}.json'
    )

    if csv_file_exists and json_file_exists:
        return OperationEnum.ADD_OR_CHANGE_DATA.value
    elif json_file_exists:
        return OperationEnum.PATCH_METADATA.value
    else:
        raise NoSuchImportableDataset(
            f'{dataset_name} does not exist in the input directory'
        )
