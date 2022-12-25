from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import os


from common.operators.get_urls import get_urls
from common.operators.download_files import download_files
from common.operators.load_postgres import load_postgres

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

doc_md = """
### NYC Taxi Zones  Dag
#### Purpose
   This dag scrapes the NYC Taxi datapage for a downloadable csv file containing metadata
   to support to NYC Taxi Data Warehouse. The data is loaded into postgres.
"""
with DAG(
    "nyc_taxi_zone_dag",
    default_args=default_args,
    doc_md=doc_md,
    catchup=False,
    start_date=datetime(2022, 8, 1),
    schedule_interval=None,
    concurrency=1,
) as dag:

    get_url = PythonOperator(
        task_id="get_url",
        python_callable=get_urls,
        op_kwargs={
            "url": LINK,
            "regex": "(taxi)\+_(zone_lookup)(.csv)"
        },
    )

    download_from_url = PythonOperator(
        task_id="download_files", python_callable=download_files
    )

    (
            get_url >> download_from_url

            )
