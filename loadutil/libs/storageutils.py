import os
import dask.dataframe as dd
from loadutil.models.pipeline import Source
from loadutil.models.pipeline import ModelUtils
import pandas as pd


class StorageUtils:

    def csv_srv_to_parquet(self, work_container: str, source: Source) -> str:
        model_utils = ModelUtils()
        src_file_path = os.path.normpath(
                            '/'.join([work_container, source.data_item])
                            )

        types = model_utils.get_src_dtypes(source)

        ddf = dd.read_csv(src_file_path,
                          blocksize=source.read_blocksize,
                          usecols=model_utils.get_src_column_list(source),
                          dtype=types)

        ddf.compute()

        parquet_folderpath = os.path.normpath(
                        '/'.join([work_container, source.name])
                        )

        ddf.to_parquet(
            parquet_folderpath,
            engine="pyarrow",
            write_metadata_file=False,
            compression="snappy"
        )

    def map_attr_to_timestamp64(self, ddf, col_name, format):
        meta = ('timestamp', 'datetime64[ns]')
        ddf[col_name] = ddf[col_name].map_partitions(pd.to_datetime,
                                                     format=format,
                                                     meta=meta)
        return ddf

    def read_parquet_with_sort_by_div(self, path: str, index: str,
                                      engine='pyarrow'):
        ddf = dd.read_parquet(path, engine=engine)
        ddf = ddf.sort_values(by=index)
        dask_divisions = ddf.set_index(index, sorted=True).divisions
        unique_divisions = list(dict.fromkeys(list(dask_divisions)))
        ddf = ddf.set_index(
            index,
            divisions=unique_divisions)
        # print(sorted_olist_order_items_dataset_ddf.divisions)
        return ddf

    def get_filter_values(self, path: str, columns, set_index=False, index=None, engine='pyarrow'):
        ddf = dd.read_parquet(path, engine=engine)
        ddf = ddf[columns].drop_duplicates().to_frame()
        if set_index:
            ddf = ddf.set_index(index)
        return ddf

    def create_flat_columns_list(self, columns):
        res_cols = []
        for col in columns:
            mot_empy_items = tuple(filter(lambda c: c != '' and c is not None, col))
            res_cols.append('_'.join(mot_empy_items))
        return res_cols
