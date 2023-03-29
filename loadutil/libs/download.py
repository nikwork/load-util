import os
from zipfile import ZipFile
from kaggle.api.kaggle_api_extended import KaggleApi


class DownloadKaggle:

    def __init__(self) -> None:
        self._api = KaggleApi()
        self._api.authenticate()

    @property
    def api(self):
        return self._api

    def download_file(self, dataset_name: str,
                      file_name: str, path: str) -> None:
        self.api.dataset_download_file(dataset=dataset_name,
                                       file_name=file_name,
                                       path=path
                                       )
        arch_path = os.path.normpath('/'.join([path, file_name])) + '.zip'
        with ZipFile(arch_path) as zf:
            zf.extractall(path=path)
        try:
            os.remove(arch_path)
        except OSError:
            pass

    def download_dataset(self, dataset_name: str,
                         path: str, unzip: bool) -> None:
        self.api.dataset_download_files(dataset=dataset_name,
                                        path=path,
                                        unzip=unzip
                                        )
