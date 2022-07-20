import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
import re
import os



def download_taxi_data(url, year, month, destination):

    urls = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href'))

# For testing:
    #year = '2022'
    #month = '02'

# Parameterize year and month, regex pattern is \d{4}-\d{2}, airflow pass {{ execution_date.strftime(\'%Y-%m\') }}

    dataset_regex = re.compile('.tripdata_' + year + '-' + month + '.[a-z]*')
    url_regex = re.compile('([a-z]*_[a-z]*)_(\d{4}-\d{2})(.parquet)')

    links=list(filter(dataset_regex.search, urls))

    for download_url in links:
        result = re.search(url_regex, download_url)
        dir, date, ext = result.groups()
        #target = airflow_home+dir+"/"+dir+date+ext
        os.system(f"curl -sSLf {download_url} > {destination}")
        print(f'Downloaded {download_url} at destination: {destination}.')


# To test script run this command $ python test.py 2022 02

