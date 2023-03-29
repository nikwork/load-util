import argparse
from loadutil.models.pipeline import ModelUtils
from loadutil.libs.batch import Batch
from loadutil.libs.download import DownloadKaggle
from loadutil.libs.storageutils import StorageUtils
from datetime import timedelta
import pyarrow.parquet as pq


# CLI params

parser = argparse.ArgumentParser(prog='run_pipeline',
                                 description='Run ETL pipeline.')

parser.add_argument('--cf', type=str,
                    help='Path to pipeline config YAML file.')
args = parser.parse_args()
print(args.cf)

# Load utils

model_utils = ModelUtils()
download_kaggle = DownloadKaggle()
storage_utils = StorageUtils()

# Read config

pipline_config = model_utils.read_pipeline_config(args.cf)
print('-'*72)


# Download files from config

for source in model_utils.get_src_list(pipline_config):
    download_kaggle.download_file(dataset_name=source.dataset,
                                  file_name=source.data_item,
                                  path=pipline_config.work_container
                                  )
    print('loaded: ' + source.data_item)

batch = Batch(pipline_config)

# To parquet with some casts

for source in model_utils.get_src_list(pipline_config):
    storage_utils.csv_srv_to_parquet(pipline_config.work_container, source)
    print('saved as parquet: ' + source.data_item)


# Prepare for joins

olist_order_items_dataset_ddf = storage_utils.read_parquet_with_sort_by_div(
        path='demo_data/wrk_data/olist_order_items_dataset/*.parquet',
        index="order_id"
    )

olist_orders_dataset_ddf = storage_utils.read_parquet_with_sort_by_div(
        path='demo_data/wrk_data/olist_orders_dataset/*.parquet',
        index="order_id"
    )

filter_ddf = storage_utils.get_filter_values(
        'demo_data/wrk_data/olist_order_payments_dataset/*.parquet',
        'order_id',
        set_index=True, index='order_id'
    )

# Cast to datetime

for col_name in ['order_purchase_timestamp',
                    'order_approved_at',
                    'order_delivered_carrier_date',
                    'order_delivered_customer_date',
                    'order_estimated_delivery_date']:
    olist_orders_dataset_ddf = storage_utils.map_attr_to_timestamp64(
                                            ddf=olist_orders_dataset_ddf,
                                            col_name=col_name,
                                            format='%Y-%m-%d %H:%M:%S')

print('Data types casted')

# 112650
merged_order_items_dataset_ddf = olist_order_items_dataset_ddf.merge(
    olist_orders_dataset_ddf,
    how="left",
    left_index=True,
    right_index=True
)
print(merged_order_items_dataset_ddf.shape[0].compute())


# 112647 (filterring better to up in actions)
merged_filtered_order_items_dataset_ddf = merged_order_items_dataset_ddf.merge(
    filter_ddf,
    how="inner",
    left_index=True,
    right_index=True
)
print(merged_filtered_order_items_dataset_ddf.shape[0].compute())

merged_filtered_order_items_dataset_ddf = merged_filtered_order_items_dataset_ddf.assign(
        end_of_week=lambda x: x['order_approved_at'] + timedelta(days=6)
    )

print(merged_filtered_order_items_dataset_ddf.dtypes)

print('Detailed data mart is ready')

print('-'*72)
print('-'*72)

# Agg calc

merged_filtered_order_items_dataset_ddf = merged_filtered_order_items_dataset_ddf.groupby(['product_id', 'end_of_week']).agg({'price': ['sum']})
merged_filtered_order_items_dataset_ddf = merged_filtered_order_items_dataset_ddf.reset_index()
print(merged_filtered_order_items_dataset_ddf.compute().head())
print(merged_filtered_order_items_dataset_ddf.columns.values)
merged_filtered_order_items_dataset_ddf.columns = storage_utils.create_flat_columns_list(
    merged_filtered_order_items_dataset_ddf.columns.values
)

print(merged_filtered_order_items_dataset_ddf.columns.values)
print(merged_filtered_order_items_dataset_ddf.dtypes)

print('Agg data mart is ready')
print('-'*72)

merged_filtered_order_items_dataset_ddf.to_parquet(
            pipline_config.destination.container,
            engine="pyarrow",
            write_metadata_file=False,
            compression="snappy",
            write_index=False,
            partition_on=['product_id']
)

# Test read with Pandas

nyc_payroll_parquet = pq.ParquetDataset(pipline_config.destination.container).read_pandas().to_pandas()
print(nyc_payroll_parquet.head())
print(nyc_payroll_parquet.info())
print(nyc_payroll_parquet.shape)
