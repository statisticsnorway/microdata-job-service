from job_service.model.camelcase_model import CamelModel


class ImportableDataset(CamelModel, extra="forbid"):
    dataset_name: str
    has_metadata: bool
    has_data: bool
    is_archived: bool = False
