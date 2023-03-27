import os
import argparse
from loadutil.models.pipeline import ModelUtils
from loadutil.libs.batch import Batch
from loadutil.libs.download import DownloadKaggle
import dask.dataframe as dd


parser = argparse.ArgumentParser(prog='run_pipeline',
                                 description='Run ETL pipeline.')

parser.add_argument('--cf', type=str,
                    help='Path to pipeline config YAML file.')
args = parser.parse_args()
print(args.cf)

model_utils = ModelUtils()
download_kaggle = DownloadKaggle()

pipline_config = model_utils.read_pipeline_config(args.cf)
print('-'*72)

for src in model_utils.get_src_list(pipline_config):
    download_kaggle.download_file(dataset_name=src.dataset,
                                  file_name=src.data_item,
                                  path=pipline_config.work_container
                                  )

batch = Batch(pipline_config)


ddf = dd.read_csv('demo_data/wrk_data/olist_order_payments_dataset.csv', blocksize='1MB')
for src in model_utils.get_src_list(pipline_config):
    src_file_path = os.path.normpath(
                        '/'.join([pipline_config.work_container, src.data_item])
                        )
    print(src_file_path)
    types = model_utils.get_src_dtypes(src)

    ddf = dd.read_csv(src_file_path, blocksize='1MB', usecols=model_utils.get_src_column_list(src), dtype=types)

    print(types)
    ddf.compute()

    parquet_folderpath = os.path.normpath(
                    '/'.join([pipline_config.work_container, src.name])
                    )

    ddf.to_parquet(
        parquet_folderpath,
        engine="pyarrow",
        write_metadata_file=False,
        compression="snappy",
    )

    print(ddf.dtypes)
