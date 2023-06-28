import os
import tarfile
from pathlib import Path
from typing import List

from job_service.model.importable_dataset import ImportableDataset
from job_service.config import environment

INPUT_DIR = Path(environment.get("INPUT_DIR"))
ARCHIVE_DIR = INPUT_DIR / "archive"


def _has_data(tar, dataset_name):
    chunks_count = len(
        [
            name
            for name in tar.getnames()
            if name.startswith("chunks/") and name.endswith(".csv.encr")
        ]
    )
    data_files = [
        f"{dataset_name}.symkey.encr",
        f"{dataset_name}.md5",
        "chunks",
    ] + [f"chunks/{i}.csv.encr" for i in range(1, chunks_count + 1)]
    return _dataset_files_exists(tar, data_files)


def _has_metadata(tar, dataset_name):
    return _dataset_files_exists(tar, [f"{dataset_name}.json"])


def _dataset_files_exists(tar, file_names):
    tar_files = tar.getnames()
    return all(file_name in tar_files for file_name in file_names)


def get_datasets_in_directory(dir_path, is_archived=False):
    datasets = []

    for item in os.listdir(dir_path):
        item_path = dir_path / item
        dataset_name, ext = os.path.splitext(item)

        if ext == ".tar" and tarfile.is_tarfile(item_path):
            tar = tarfile.open(item_path)
            importable_dataset = ImportableDataset(
                dataset_name=dataset_name,
                has_data=_has_data(tar, dataset_name),
                has_metadata=_has_metadata(tar, dataset_name),
                is_archived=is_archived,
            )
            if importable_dataset.has_metadata:
                datasets.append(importable_dataset)

    return datasets


def get_importable_datasets() -> List[ImportableDataset]:
    """
    Returns names of all valid datasets in input directory.
    """
    datasets = get_datasets_in_directory(INPUT_DIR)
    if ARCHIVE_DIR.exists():
        datasets += get_datasets_in_directory(ARCHIVE_DIR, is_archived=True)
    return datasets
