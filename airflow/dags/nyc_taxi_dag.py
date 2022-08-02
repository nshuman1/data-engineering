from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime


from get_urls import get_urls
from upload_to_gcs import process_xcom_for_gcs_upload
from upload_to_gcs import upload_blob
from get_weather import get_weather_data
from download_files import download_files


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
    start_date=datetime(2022, 4, 1),
    catchup=True) as dag:

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
        python_callable=download_files
    )

    local_to_gcs = PythonOperator(
        task_id = 'local_to_gcs',
        python_callable = process_xcom_for_gcs_upload,
        op_kwargs = {
            "bucket_name" : BUCKET_NAME
        }
    )

    get_weather = PythonOperator(
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
    )

    downloading_data >> get_url >> download_from_url >> local_to_gcs

    get_weather >> upload_weather
    
