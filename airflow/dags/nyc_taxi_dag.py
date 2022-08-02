from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from random import uniform
from datetime import datetime
import os
import urllib.request

from get_urls import get_urls
from upload_to_gcs import process_xcom_for_gcs_upload
from upload_to_gcs import upload_blob
from get_weather import get_weather_data

from collections import defaultdict



default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

LINK = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
BUCKET_NAME = 'levant-data-lake_nyc-taxi-dwh'

# def _get_urls(ti, url, year, month):
#     urls = []
#     page = requests.get(url)
#     soup = BeautifulSoup(page.text, 'html.parser')

#     for link in soup.find_all('a'):
#         urls.append(link.get('href'))

#     dataset_regex = re.compile('.tripdata_' + year + '-' + month + '.[a-z]*')
#     url_regex = re.compile('([a-z]*_[a-z]*)_(\d{4}-\d{2})(.parquet)')

#     links=list(filter(dataset_regex.search, urls))
#     to_pass = defaultdict()

#     for download_url in links:
#         result = re.search(url_regex, download_url)
#         dir, date, ext = result.groups()
#         to_pass[dir] = [dir, date, ext, download_url]

#     ti.xcom_push(key='download_info', value = to_pass)
        

def _download_files(ti):
    '''
    Reads a dictionary containing a subject as a key, and a list stored as the value pair containing the following:
        list[0] = ******@@@@@*****  -- TO-DO: populate this description based on whether this function will be for NY taxi only
        list[1] = YYYY-MM
        list[2] = file extension
        list[4] = url to download file
    Utilizes unpacked list values to set a path and filename before downloading the files and saving them to the local machine.

    '''

    
    download_info = ti.xcom_pull(key='download_info', task_ids='get_url')
    source_dest_gcs = defaultdict()    

    for k,v in download_info.items():
        taxi_type = v[0]
        year_month = v[1]
        file_ext = v[2]
        url = v[3]

        parent_dir = os.path.abspath(os.path.join(os.path.join(os.path.dirname(__file__), '..'), taxi_type))
        file_name = f'{taxi_type}_{year_month}{file_ext}'
        dest = os.path.join(parent_dir, file_name)
        os.makedirs(parent_dir, exist_ok=True)
            
        urllib.request.urlretrieve(url, dest)
        source_dest_gcs[file_name] = { 'source' : dest, 'dest' : f'{taxi_type}/{taxi_type}_{year_month}{file_ext}' }

    ti.xcom_push(key='gcs_path', value = source_dest_gcs)



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
     

    download_files = PythonOperator(
        task_id='download_files',
        python_callable=_download_files
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

    downloading_data >> get_url >> download_files >> local_to_gcs

    get_weather >> upload_weather
    
