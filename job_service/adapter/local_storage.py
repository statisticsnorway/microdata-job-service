import logging
import os
import tarfile
from pathlib import Path
from tarfile import ReadError
from typing import List

from job_service.config import environment
from job_service.model.importable_dataset import ImportableDataset
from job_service.exceptions import NotFoundException


logger = logging.getLogger()

INPUT_DIR = Path(environment.get("INPUT_DIR"))
ARCHIVE_DIR = INPUT_DIR / "archive"


def _has_data(tar: tarfile.TarFile) -> bool:
    return "chunks" in tar.getnames()


def _has_metadata(tar: tarfile.TarFile, dataset_name: str) -> bool:
    return f"{dataset_name}.json" in tar.getnames()


def get_datasets_in_directory(
    dir_path: Path, is_archived: bool = False
) -> List[ImportableDataset]:
    datasets = []

    for item in os.listdir(dir_path):
        item_path = dir_path / item
        dataset_name, ext = os.path.splitext(item)
        try:
            if ext == ".tar" and tarfile.is_tarfile(item_path):
                tar = tarfile.open(item_path)
                importable_dataset = ImportableDataset(
                    dataset_name=dataset_name,
                    has_data=_has_data(tar),
                    has_metadata=_has_metadata(tar, dataset_name),
                    is_archived=is_archived,
                )
                if importable_dataset.has_metadata:
                    datasets.append(importable_dataset)
        except ReadError as e:
            logger.warning(
                f"Couldn't read tarfile for {dataset_name}: {str(e)}"
            )
            continue
    return datasets


def get_importable_datasets() -> List[ImportableDataset]:
    """
    Returns names of all valid datasets in input directory.
    """
    datasets = get_datasets_in_directory(INPUT_DIR)
    if ARCHIVE_DIR.exists():
        datasets += get_datasets_in_directory(ARCHIVE_DIR, is_archived=True)
    return datasets

def delete_importable_datasets(dataset_name):
    #if not os.path.isfile(f"{INPUT_DIR}/{dataset_name}"):
        #raise NotFoundException(f"404 - File {dataset_name} not found")
    #os.remove(f"{INPUT_DIR}/{dataset_name}")
    try:
        os.remove(f"{INPUT_DIR}/{dataset_name}")
    except (FileNotFoundError, OSError) as e:
        raise NotFoundException(f"File {dataset_name} not found: {str(e)}")