import pyarrow.parquet as pq


trips = pq.read_table('yellow_tripdata_2022-01.parquet')
trips = trips.to_pandas()


print(f'Finished succesfully, data shape is: {trips.shape}')