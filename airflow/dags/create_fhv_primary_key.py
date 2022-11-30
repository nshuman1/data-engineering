import calendar
import datetime as dt
import pandas as pd
import pyarrow.parquet as pq


def fhv_key_gen(ti, year: str, month: str) -> None:
    '''
    dedupe_parquet_col _summary_

    _extended_summary_

    Arguments:
    ti -- _description_
    '''
    
    path = ti.xcom_pull(key = 'path', task_ids='download_files')

#      CURRENT STRUCTURE OF PATH VARIABLE
# 
#     {'yellow_tripdata_2022-04.parquet': {'source': '/opt/airflow/data/yellow_tripdata/yellow_tripdata_2022-04.parquet',
#   'dest': 'yellow_tripdata',
#   'date': '2022-04',
#   'ext': '.parquet'},
#  'green_tripdata_2022-04.parquet': {'source': '/opt/airflow/data/green_tripdata/green_tripdata_2022-04.parquet',
#   'dest': 'green_tripdata',
#   'date': '2022-04',
#   'ext': '.parquet'},
#  'fhv_tripdata_2022-04.parquet': {'source': '/opt/airflow/data/fhv_tripdata/fhv_tripdata_2022-04.parquet',
#   'dest': 'fhv_tripdata',
#   'date': '2022-04',
#   'ext': '.parquet'},
#  'fhvhv_tripdata_2022-04.parquet': {'source': '/opt/airflow/data/fhvhv_tripdata/fhvhv_tripdata_2022-04.parquet',
#   'dest': 'fhvhv_tripdata',
#   'date': '2022-04',
#   'ext': '.parquet'}}
#

    fhv_path = {}
    for _, j in enumerate((list(path.values()))):
    
        if dict(j)['dest'] == 'fhv_tripdata':
            fhv_path = dict(j)
            break

    filename = fhv_path['source']
    dest = fhv_path['dest']

    df = pq.read_table(filename)
    df = df.to_pandas()

    int_year = int(year)
    int_month = int(month)

    date_str = year + month + str(calendar.monthrange(int_year, int_month)[1])




    df['load_date'] = pd.to_datetime(date_str)

    df['taxi_type'] = dest


    df['primary_key'] = df['dispatching_base_num'].map(str) + df['pickup_datetime'].map(str) + df['dropOff_datetime'].map(str) + df['PUlocationID'].map(str) + df['DOlocationID'].map(str) + df['Affiliated_base_number'].map(str)


    df['primary_key'] = df['primary_key'].apply(lambda x: x.replace(" ", ""))
    df['primary_key'] = df['primary_key'].apply(lambda x: x.replace(":", ""))
    df['primary_key'] = df['primary_key'].apply(lambda x: x.replace(".", ""))

    df.to_parquet(filename)

    del df