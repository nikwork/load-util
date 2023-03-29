# Load util
## Prerequisites to run
1. Python 3.10.10
2. requirements.txt
3. ~/.kaggle/kaggle.json file to download data sets
## Run command
python run_pipeline.py --cf=loadutil/piplines/brzi-public-list.yml
## Output 
Partitoned data mart (partitoned by product: it produces large number of folders with small files. Such approach is better for very granular aggs, which produse a lot of data.)

Agg datamart has only few fields (due to demo goals):
- product_id             object
- end_of_week    datetime64[ns]
- price_sum             float64