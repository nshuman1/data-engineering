def yellow_key_gen(ti, year: str, month: str) -> None:
    """
    Function that reads a parquet file containing yellow taxi data from the NYC Taxi dataset.
    The function creates a primary key value to uniquely identify each record of the parquet file.
    The function also creates a load date field which indicates when this data was ingested by this DAG.

    The function accepts a dictionary from an upstream task which is pulled via Airflow XCOM.

    Inputs:
        ti -- Task Instance (required for Airflow XCOM)
        year -- the year of the execution date
        month -- the month of the execution date

    Example Inputs (passed via airflow):

        source_dest_paths: {'yellow_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/yellow_tripdata/yellow_tripdata_2021-04.parquet', 'dest': 'yellow_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'green_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/green_tripdata/green_tripdata_2021-04.parquet', 'dest': 'green_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'fhv_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/fhv_tripdata/fhv_tripdata_2021-04.parquet', 'dest': 'fhv_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'fhvhv_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/fhvhv_tripdata/fhvhv_tripdata_2021-04.parquet', 'dest': 'fhvhv_tripdata', 'date': '2021-04', 'ext': '.parquet'}}

    Example Outputs (run in place, returns nothing):

    """

    import calendar
    import pandas as pd
    import pyarrow.parquet as pq

    path = ti.xcom_pull(key="source_dest_paths", task_ids="download_files")

    yellow_path = {}

    for _, j in enumerate((list(path.values()))):

        if dict(j)["dest"] == "yellow_tripdata":
            yellow_path = dict(j)
            break

    filename = yellow_path["source"]
    dest = yellow_path["dest"]

    df = pq.read_table(filename)
    df = df.to_pandas()

    int_year = int(year)
    int_month = int(month)

    date_str = year + month + str(calendar.monthrange(int_year, int_month)[1])

    df["load_date"] = pd.to_datetime(date_str)

    df["taxi_type"] = dest

    df["primary_key"] = (
        df["tpep_dropoff_datetime"].map(str)
        + df["tpep_pickup_datetime"].map(str)
        + df["PULocationID"].map(str)
        + df["DOLocationID"].map(str)
        + df["payment_type"].map(str)
        + df["total_amount"].map(str)
    )

    df["primary_key"] = df["primary_key"].apply(lambda x: x.replace(" ", ""))
    df["primary_key"] = df["primary_key"].apply(lambda x: x.replace(":", ""))
    df["primary_key"] = df["primary_key"].apply(lambda x: x.replace(".", ""))

    df.to_parquet(filename)

    del df

    return None
