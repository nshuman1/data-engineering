def download_files(ti) -> None:
    """
     Function that uses downloads files to a target destination and stores the destination path for use
     further downstream in DAG.

    The function accepts a dictionary from an upstream task which is pulled via Airflow XCOM.

    Inputs:
        ti -- Task Instance (required for Airflow XCOM)

    Example Inputs (passed via airflow):
        download_info: {'yellow_tripdata': ['yellow_tripdata', '2022-04', '.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-04.parquet'],
        'green_tripdata': ['green_tripdata', '2022-04', '.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet'],
        'fhv_tripdata': ['fhv_tripdata', '2022-04', '.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2022-04.parquet'],
        'fhvhv_tripdata': ['fhvhv_tripdata', '2022-04', '.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-04.parquet']}


    Example Outputs (The function returns a None type but pushes a dictionary containing destination paths for each
    downloaded file via airflow xcom push.)

    This section is to demonstrate an example of the constructed dictionary:

        source_dest_paths: {'yellow_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/yellow_tripdata/yellow_tripdata_2021-04.parquet', 'dest': 'yellow_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'green_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/green_tripdata/green_tripdata_2021-04.parquet', 'dest': 'green_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'fhv_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/fhv_tripdata/fhv_tripdata_2021-04.parquet', 'dest': 'fhv_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'fhvhv_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/fhvhv_tripdata/fhvhv_tripdata_2021-04.parquet', 'dest': 'fhvhv_tripdata', 'date': '2021-04', 'ext': '.parquet'}}


    """

    import os
    import urllib.request
    from collections import defaultdict

    download_info = ti.xcom_pull(key="download_info", task_ids="get_url")
    source_dest_paths = defaultdict()

    for k, v in download_info.items():
        taxi_type = v[0]
        year_month = v[1]
        file_ext = v[2]
        url = v[3]

        parent_dir = os.path.abspath(
            os.path.join(
                os.path.join(os.path.join(os.path.join(os.path.dirname(__file__), ".."), ".."), ".."), f"data/{taxi_type}"
            )
        )
        file_name = f"{taxi_type}_{year_month}{file_ext}"
        dest = os.path.join(parent_dir, file_name)
        os.makedirs(parent_dir, exist_ok=True)

        urllib.request.urlretrieve(url, dest)

        source_dest_paths[file_name] = {
            "source": dest,
            "dest": taxi_type,
            "date": year_month,
            "ext": file_ext,
        }

    ti.xcom_push(key="source_dest_paths", value=source_dest_paths)

    return None
