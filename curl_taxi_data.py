import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
import re
import sys
import os
import datetime

date = datetime.date.today()
def download_taxi_data(url, dest_file):

    urls = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href'))

# For testing:
# year = '2022'
# month = '02'

# Parameterize year and month, regex pattern is \d{4}-\d{2}, airflow pass {{ execution_date.strftime(\'%Y-%m\') }}

    regex = re.compile(f"/yellow_tripdata_{date.strftime('%Y-%m')}.[a-z]*")

    links=list(filter(regex.search, urls))

    for download_url in links:
        os.system(f"curl -O {download_url} > {dest_file}")
        print(f'Downloaded {download_url}.')
    

# To test script run this command $ python test.py 2022 02
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
TAXI_DEST_FILE = AIRFLOW_HOME + f"/nyc_taxi_{date.strftime('%Y-%m')}.parquet"
link = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

download_taxi_data(link, TAXI_DEST_FILE)