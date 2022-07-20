import os
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
import re


from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq

from curl_taxi_data import download_taxi_data

LINK = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


def download_data_dag(
    dag,
    url,
    destination_path
):
    with dag:
        download_dataset_task = PythonOperator(
            task_id="download_dataset_task",
            python_callable=download_taxi_data,
            op_kwargs={
                "url" : url,
                "year" : "{{ execution_date.strftime(\'%Y\') }}",
                "month" : "{{ execution_date.strftime(\'%m\') }}",
                "destination" : destination_path
            }
        )

        download_dataset_task 



TAXI_DEST_FILE = AIRFLOW_HOME + "/nyc_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

levant_nyc_taxi_data = DAG(
    dag_id="download_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['levant-de'],
)

download_data_dag(
    dag=levant_nyc_taxi_data,
    url = LINK,
    destination_path=TAXI_DEST_FILE
)

