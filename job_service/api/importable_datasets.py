import logging

from fastapi import APIRouter
from job_service.adapter.local_storage import input_directory


logger = logging.getLogger()

router = APIRouter()


@router.get("/importable-datasets")
def get_importable_datasets():
    logger.info("GET /importable-datasets")
    datasets = input_directory.get_importable_datasets()
    return [
        dataset.model_dump(exclude_none=True, by_alias=True)
        for dataset in datasets
    ]


@router.delete("/importable-datasets/{dataset_name}")
def delete_importable_datasets(dataset_name: str):
    logger.info(f"DELETE /importable-datasets/{dataset_name}")
    input_directory.delete_importable_datasets(dataset_name)
    return {"message": f"OK, {dataset_name} deleted"}
