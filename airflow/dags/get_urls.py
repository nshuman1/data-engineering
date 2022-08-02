# get_urls.py

# TO-DO: per airflow docs, imports should be contained within function

import requests
from bs4 import BeautifulSoup
import re
from collections import defaultdict

def get_urls(ti, url: str, year: str, month: str) -> None:
    """
    Function to scrape NYC Taxi Data webpage for data associated to downloadable parquet files.
    Returns None type, relevant dataset information is pushed via XCOM.

    ti -- Task Instance (required for Airflow XCOM)
    """


    

    urls = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    for link in soup.find_all('a'):
        urls.append(link.get('href'))

    dataset_regex = re.compile('.tripdata_' + year + '-' + month + '.[a-z]*')
    url_regex = re.compile('([a-z]*_[a-z]*)_(\d{4}-\d{2})(.parquet)')

    links=list(filter(dataset_regex.search, urls))
    to_pass = defaultdict()

    for download_url in links:
        result = re.search(url_regex, download_url)
        dir, date, ext = result.groups()
        to_pass[dir] = [dir, date, ext, download_url]

    ti.xcom_push(key='download_info', value = to_pass)

    return None
    
   