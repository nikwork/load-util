import yaml
from typing import List, Optional
from pydantic import BaseModel, StrictStr


class Attribute(BaseModel):
    name: StrictStr
    df_type: Optional[StrictStr] = None


class Source(BaseModel):
    name: StrictStr
    type: StrictStr
    data_item: StrictStr
    dataset: StrictStr
    format: StrictStr
    read_blocksize: StrictStr
    container: StrictStr
    required_attributes: Optional[List[Attribute]]


class Destination(BaseModel):
    name: str
    container: StrictStr


class Pipeline(BaseModel):
    name: StrictStr
    description: Optional[StrictStr]
    pipeline_subdomain: Optional[StrictStr]
    work_container: StrictStr
    sources: Optional[List[Source]]
    destination: Destination


class ModelUtils:

    def read_pipeline_config(self, file_path: str) -> Pipeline:
        with open(file_path, 'r') as stream:
            config = yaml.safe_load(stream)
            print(config)
        return Pipeline(**config)

    def get_src_list(self, pipeline: Pipeline) -> List[Source]:
        return [src for src in pipeline.sources]

    def get_src_column_list(self, source: Source) -> str:
        return [attr.name for attr in source.required_attributes]

    def get_src_dtypes(self, source: Source) -> dict:
        src_dtypes = dict()
        for attr in filter(lambda attr: attr.df_type is not None,
                           source.required_attributes):
            src_dtypes[attr.name] = attr.df_type
        return src_dtypes
