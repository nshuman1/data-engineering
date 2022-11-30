from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime


from get_urls import get_urls
from get_weather import get_weather_data
from download_files import download_files
from load_postgres import load_postgres
from create_yellow_primary_key import yellow_key_gen
from create_green_primary_key import green_key_gen
from create_fhvhv_primary_key import fhvhv_key_gen
from create_fhv_primary_key import fhv_key_gen

default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# Variables

LINK = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
BUCKET_NAME = 'levant-data-lake_nyc-taxi-dwh' 


with DAG('nyc_taxi_dag', 
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    start_date=datetime(2020, 8, 1),
    catchup=True,
    concurrency=6) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3',
        do_xcom_push=False
    )

    get_url = PythonOperator(
            task_id='get_url',
            python_callable = get_urls,
            op_kwargs = {
                "url" : LINK,
                "year" : "{{ execution_date.strftime(\'%Y\') }}",
                "month" : "{{ execution_date.strftime(\'%m\') }}"
            }
    )
     

    download_from_url = PythonOperator(
        task_id='download_files',
        python_callable = download_files
    )

    create_yellow_key = PythonOperator(
        task_id='create_yellow_key',
        python_callable= yellow_key_gen,
        op_kwargs = {
            "year" : "{{ execution_date.strftime(\'%Y\') }}",
            "month" : "{{ execution_date.strftime(\'%m\') }}"
        }

    )

    create_green_key = PythonOperator(
        task_id='create_green_key',
        python_callable= green_key_gen,
        op_kwargs = {
            "year" : "{{ execution_date.strftime(\'%Y\') }}",
            "month" : "{{ execution_date.strftime(\'%m\') }}"
        }

    )

    create_fhvhv_key = PythonOperator(
        task_id='create_fhvhv_key',
        python_callable= fhvhv_key_gen,
        op_kwargs = {
            "year" : "{{ execution_date.strftime(\'%Y\') }}",
            "month" : "{{ execution_date.strftime(\'%m\') }}"
        }

    )

    create_fhv_key = PythonOperator(
        task_id='create_fhv_key',
        python_callable= fhv_key_gen,
        op_kwargs = {
            "year" : "{{ execution_date.strftime(\'%Y\') }}",
            "month" : "{{ execution_date.strftime(\'%m\') }}"
        }

    )

    load_db = PythonOperator(
        task_id = 'load_postgres',
        python_callable = load_postgres
    )

    # get_weather = PythonOperator(
    #     task_id='get_weather',
    #     python_callable = get_weather_data,
    #     op_kwargs = {
    #         "year" : "{{ execution_date.strftime(\'%Y\') }}",
    #         "month" : "{{ execution_date.strftime(\'%m\') }}"
    #     }
    # )

    # upload_weather = PythonOperator(
    #     task_id='upload_weather_gcs',
    #     python_callable = upload_blob,
    #     op_kwargs = {
    #         "bucket_name" : BUCKET_NAME,
    #         "source_file_name" : 'weather_nyc_{{ execution_date.strftime(\'%Y\') }}_{{ execution_date.strftime(\'%m\') }}.json',
    #         "destination_blob_name" : "weather/weather_nyc_{{ execution_date.strftime(\'%Y\') }}_{{ execution_date.strftime(\'%m\') }}.json"
    #     }
    # )

    downloading_data >> get_url >> download_from_url >> [create_yellow_key, create_green_key, create_fhv_key, create_fhvhv_key ] >> load_db

    # get_weather >> upload_weather
    
