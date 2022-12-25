from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import os


from common.operators.get_urls import get_urls
from common.operators.get_weather import get_weather_data
from common.operators.download_files import download_files
from common.operators.load_postgres import load_postgres
from common.operators.create_yellow_primary_key import yellow_key_gen
from common.operators.create_green_primary_key import green_key_gen
from common.operators.create_fhvhv_primary_key import fhvhv_key_gen
from common.operators.create_fhv_primary_key import fhv_key_gen

default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# Variables

LINK = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# For GCS Only

BUCKET_NAME = "levant-data-lake_nyc-taxi-dwh"

# For Local (Postgres) DB Only

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")

# DAG Documentation

year = "{{ execution_date.strftime('%Y') }}"
month =  "{{ execution_date.strftime('%m') }}"


doc_md = """
### NYC Taxi Dag
#### Purpose
   This dag scrapes the NYC Taxi datapage for downloadable parquet files and loads them into postgres each month.
"""

with DAG(
    "nyc_taxi_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    doc_md=doc_md,
    start_date=datetime(2022, 8, 1),
    catchup=True,
    concurrency=6,
) as dag:

    get_url = PythonOperator(
        task_id="get_url",
        python_callable=get_urls,
        op_kwargs={
            "url": LINK,
            "regex": f'([a-z]*_[a-z]*)_({year}-{month})(\.[a-z]*)'
        },
    )

    download_from_url = PythonOperator(
        task_id="download_files", python_callable=download_files
    )

    create_yellow_key = PythonOperator(
        task_id="create_yellow_key",
        python_callable=yellow_key_gen,
        op_kwargs={
            "year": "{{ execution_date.strftime('%Y') }}",
            "month": "{{ execution_date.strftime('%m') }}",
        },
    )

    create_green_key = PythonOperator(
        task_id="create_green_key",
        python_callable=green_key_gen,
        op_kwargs={
            "year": "{{ execution_date.strftime('%Y') }}",
            "month": "{{ execution_date.strftime('%m') }}",
        },
    )
    create_green_key.doc_md = """
    ### Description
    Function that reads a parquet file containing green taxi data from the NYC Taxi dataset.
    The function creates a primary key value to uniquely identify each record of the parquet file.
    The function also creates a load date field which indicates when this data was ingested by this DAG.

    The function accepts a dictionary from an upstream task which is pulled via Airflow XCOM.

    ### Inputs
        ti -- Task Instance (required for Airflow XCOM)
        year -- the year of the execution date
        month -- the month of the execution date

    ### Example Inputs (passed via airflow)

        source_dest_paths: {'yellow_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/yellow_tripdata/yellow_tripdata_2021-04.parquet', 'dest': 'yellow_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'green_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/green_tripdata/green_tripdata_2021-04.parquet', 'dest': 'green_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'fhv_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/fhv_tripdata/fhv_tripdata_2021-04.parquet', 'dest': 'fhv_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'fhvhv_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/fhvhv_tripdata/fhvhv_tripdata_2021-04.parquet', 'dest': 'fhvhv_tripdata', 'date': '2021-04', 'ext': '.parquet'}}

    ### Example Outputs (run in place, returns nothing)

        None       
        
    """

    create_fhvhv_key = PythonOperator(
        task_id="create_fhvhv_key",
        python_callable=fhvhv_key_gen,
        op_kwargs={
            "year": "{{ execution_date.strftime('%Y') }}",
            "month": "{{ execution_date.strftime('%m') }}",
        },
    )

    create_fhv_key = PythonOperator(
        task_id="create_fhv_key",
        python_callable=fhv_key_gen,
        op_kwargs={
            "year": "{{ execution_date.strftime('%Y') }}",
            "month": "{{ execution_date.strftime('%m') }}",
        },
    )

    load_db = PythonOperator(
        task_id="load_postgres",
        python_callable=load_postgres,
        op_kwargs={
            "host": PG_HOST,
            "db": PG_DATABASE,
            "user": PG_USER,
            "pw": PG_PASSWORD,
            "port": PG_PORT,
        },
    )

    # Weather tasks turned off due to API Limits

    """get_weather = PythonOperator(
        task_id='get_weather',
        python_callable = get_weather_data,
        op_kwargs = {
            "year" : "{{ execution_date.strftime(\'%Y\') }}",
            "month" : "{{ execution_date.strftime(\'%m\') }}"
        }
    )

    upload_weather = PythonOperator(
        task_id='upload_weather_gcs',
        python_callable = upload_blob,
        op_kwargs = {
            "bucket_name" : BUCKET_NAME,
            "source_file_name" : 'weather_nyc_{{ execution_date.strftime(\'%Y\') }}_{{ execution_date.strftime(\'%m\') }}.json',
            "destination_blob_name" : "weather/weather_nyc_{{ execution_date.strftime(\'%Y\') }}_{{ execution_date.strftime(\'%m\') }}.json"
        }
    )"""

    (
        get_url
        >> download_from_url
        >> [create_yellow_key, create_green_key, create_fhv_key, create_fhvhv_key]
        >> load_db
    )

    # Weather tasks turned off due to API Limits

    # get_weather >> upload_weather
