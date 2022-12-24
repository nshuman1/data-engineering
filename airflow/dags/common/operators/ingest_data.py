import argparse
import os
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine


def load_postgres(ti):
    # user = params.user
    # password = params.password
    # host = params.host
    # port = params.port
    # db = params.db
    # table_name = params.table_name
    # url = params.url

    filename = filename

    ti.xcom_pull(key="path", task_ids="download_files")

    engine = create_engine("postgresql://root:root@pgdatabase:5432/ny_taxi")

    for i in range(len(list(path.keys()))):
        filename = path[list(path.keys())[i]]["filename"]
        dest = path[list(path.keys())[i]]["dest"]

        parquet_table = pq.read_table(filename)
        df = parquet_table.to_pandas()

        df.to_sql(name=dest, con=engine, if_exists="append", chunksize=100000)

    return None
