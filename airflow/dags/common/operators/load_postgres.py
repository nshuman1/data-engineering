def load_postgres(ti, host, db, user, pw, port):
    """
    Function that reads a parquet file into a pyarrow dataset. The dataset is then split into batches with a size of 10,000.
    Each batch is then converted to a pandas dataframe via the pyarrow dataset pandas api. The pandas to_sql api call is then invoked to load the data
    via SQL to a postgres database. If the table exists in postgres, the function will append, otherwise creating a new table.

    The function accepts a dictionary from an upstream task which is pulled via Airflow XCOM.

    Inputs:
        ti -- Task Instance (required for Airflow XCOM)
        host (str) -- Database host name 
        db (str) -- Database name
        user (str) -- Database username
        pw (str) -- Database user password
        port (str) -- Database port

    Example Inputs (passed via airflow):

        source_dest_paths: {'yellow_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/yellow_tripdata/yellow_tripdata_2021-04.parquet', 'dest': 'yellow_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'green_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/green_tripdata/green_tripdata_2021-04.parquet', 'dest': 'green_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'fhv_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/fhv_tripdata/fhv_tripdata_2021-04.parquet', 'dest': 'fhv_tripdata', 'date': '2021-04', 'ext': '.parquet'},
        'fhvhv_tripdata_2021-04.parquet': {'source': '/opt/airflow/data/fhvhv_tripdata/fhvhv_tripdata_2021-04.parquet', 'dest': 'fhvhv_tripdata', 'date': '2021-04', 'ext': '.parquet'}}

    Example Outputs (run in place, returns nothing):

    """

    from sqlalchemy import create_engine
    import pyarrow.dataset as ds

    path = ti.xcom_pull(key="source_dest_paths", task_ids="download_files")

    engine = create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

    for i in range(len(list(path.keys()))):
        filename = path[list(path.keys())[i]]["source"]
        dest = path[list(path.keys())[i]]["dest"]

        dataset = ds.dataset(filename, format="parquet")

        for batch in dataset.to_batches(batch_size=10_000):
            df = batch.to_pandas()
            df.to_sql(name=dest, con=engine, if_exists="append")
            print("Processed 10,000 rows successfully.")
            del df

    del dataset

    return None
