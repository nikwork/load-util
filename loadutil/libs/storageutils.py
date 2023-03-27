import os
import dask.dataframe as dd
from loadutil.models.pipeline import Source

class StorageUtils:
    def csv_srv_to_parquet(work_container: str, read_blocksize: str, source: Source) -> str:
        src_file_path = os.path.normpath(
                                '/'.join([work_container, source.data_item])
                                )
        ddf = dd.read_csv(src_file_path, blocksize=read_blocksize)
        print(ddf.dtypes)
        ddf = dd.read_csv(filename, blocksize="500MB",
                             dtype=fColTypes, header=None, sep='|',names=fCSVCols)

        ddf.to_parquet(
            "data/something5",
            engine="pyarrow",
            write_metadata_file=False,
            compression="snappy"
        )

ddf = dd.read_csv(
   "s3://coiled-datasets/nyc-parking-tickets/csv/*.csv",
   usecols=cols,
   dtype={'House Number': 'object',
      'Issuer Command': 'object',
      'Issuer Squad': 'object',
      'Time First Observed': 'object',
      'Unregistered Vehicle?': 'float64',
      'Violation Description': 'object',
      'Violation Legal Code': 'object',
      'Violation Location': 'float64',
      'Violation Post Code': 'object'}
)