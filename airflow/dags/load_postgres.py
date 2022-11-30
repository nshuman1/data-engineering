from google.cloud import storage
import argparse
import os
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.dataset as ds


def load_postgres(ti):
    # user = params.user
    # password = params.password
    # host = params.host 
    # port = params.port 
    # db = params.db
    # table_name = params.table_name
    # url = params.url
   # filename = filename
    
    
    path = ti.xcom_pull(key = 'path', task_ids='download_files')

    engine = create_engine('postgresql://root:root@database-pgdatabase-1:5432/ny_taxi')

    for i in range(len(list(path.keys()))):
        filename = (path[list(path.keys())[i]]['source'])
        dest = (path[list(path.keys())[i]]['dest'])
    
        dataset = ds.dataset(filename, format='parquet')
        
        for batch in dataset.to_batches(batch_size=10_000):
            df = batch.to_pandas()
            df.to_sql(name = dest, con = engine, if_exists = 'append')
            print("Processed 10,000 rows successfully.")
            del df

    del dataset

    return None