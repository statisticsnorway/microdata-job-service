from pydantic import Extra
from job_service.model.camelcase_model import CamelModel


class ImportableDataset(CamelModel, extra=Extra.forbid):
    dataset_name: str
    has_metadata: bool
    has_data: bool
