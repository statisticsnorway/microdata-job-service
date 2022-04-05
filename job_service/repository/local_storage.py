import os


INPUT_DIR = os.environ['INPUT_DIR']


def get_input_datasets() -> list[str]:
    """
    Returns names of all valid datasets in input directory.
    """
    valid_datasets = []
    for dataset_name in os.listdir(INPUT_DIR):
        if has_input_dataset(dataset_name):
            valid_datasets.append(dataset_name)
    return valid_datasets


def has_input_dataset(dataset_name) -> bool:
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
    return csv_file_exists and json_file_exists
