import logging

from fastapi import APIRouter
from job_service.adapter import local_storage


logger = logging.getLogger()

importable_datasets_api = APIRouter()


@importable_datasets_api.get("/importable-datasets")
def get_importable_datasets():
    logger.info("GET /importable-datasets")
    datasets = local_storage.get_importable_datasets()
    return [
        dataset.model_dump(exclude_none=True, by_alias=True)
        for dataset in datasets
    ]


@importable_datasets_api.delete("/importable-datasets/{dataset_name}")
def delete_importable_datasets(dataset_name: str):
    logger.info(f"DELETE /importable-datasets/{dataset_name}")
    local_storage.delete_importable_datasets(dataset_name)
    return {"message": f"OK, {dataset_name} deleted"}
