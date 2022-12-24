# get_urls.py


def get_urls(ti, url: str, year: str, month: str) -> None:
    """
    Function to scrape NYC Taxi data page for data associated to downloadable parquet files.
    The function constructs a dictionary which is pushed to downstream tasks via Airflow XCOM.

    Inputs:
        ti -- Task Instance (required for Airflow XCOM)
        url -- The url for the NYC Taxi data page
        year -- The desired search year on the NYC Taxi data page
        month -- The desired search month on the NYC Taxi data page

    Example Inputs:
        ti: None (passed via airflow)
        year: 2022
        month: 01
        url: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

    Example Outputs (The function returns a None type but pushes the dictionary via airflow xcom push,
    This section is to demonstrate an example of the constructed dictionary):

        download_info: {'yellow_tripdata': ['yellow_tripdata', '2022-04', '.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-04.parquet'],
        'green_tripdata': ['green_tripdata', '2022-04', '.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet'],                                                'fhv_tripdata': ['fhv_tripdata', '2022-04', '.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2022-04.parquet'],
        'fhvhv_tripdata': ['fhvhv_tripdata', '2022-04', '.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-04.parquet']}

    """
    import requests
    from bs4 import
