import os
import uuid
from loadutil.models.pipeline import Pipeline


class Batch:

    def __init__(self, pipeline: Pipeline) -> None:
        self._pipeline_config = Pipeline
        self._batch_id = uuid.uuid4()
        self._work_container = os.path.normpath(
            "/".join([
                    pipeline.work_container,
                    str(self._batch_id)
                ])
            )

    @property
    def pipeline_config(self):
        return self._pipeline_config

    @property
    def batch_id(self):
        return self._batch_id

    @property
    def work_container(self):
        return self._work_container
