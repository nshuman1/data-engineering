Docker Run Commands

## In powershell the line must end with "\" (backtick)
docker run -i \
  -e POSTGRES_USER="admin" \
  -e POSTGRES_PASSWORD="admin" \
  -e POSTGRES_DB="ny_taxi" \
  -v c:/Users/User/github/nshuman/data_engineering/taxi_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
   postgres:13


URL="https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-01.parquet"
URL="http://172.25.192.1:8000/github/nshuman/data_engineering/yellow_tripdata_2022-01.parquet"

docker run -i \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4


  python ingest_data.py \
  --user=admin \
  --password=admin \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}