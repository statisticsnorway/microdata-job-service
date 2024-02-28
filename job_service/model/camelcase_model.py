from pydantic import BaseModel, ConfigDict
from humps import camelize


def to_camel(string):
    return camelize(string)


class CamelModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
