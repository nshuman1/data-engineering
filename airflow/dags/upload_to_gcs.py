from google.cloud import storage


def process_xcom_for_gcs_upload(ti, bucket_name):
    """Processes a dictionary containing nested dictionary with keys
    corresponding to a source file path, and a destination file path."""
     
    # TO-DO: convert to variable that can accept the xcom pull as an argument

    gcs_path = ti.xcom_pull(key = 'gcs_path', task_ids='download_files')
    
    for i in range(len(list(gcs_path.keys()))):
        source = (gcs_path[list(gcs_path.keys())[i]]['source'])
        dest = (gcs_path[list(gcs_path.keys())[i]]['dest'])

        upload_blob(bucket_name, source, dest)
        

    
    
def upload_blob(bucket_name: str, source_file_name: str, destination_blob_name: str) -> None:
    """Uploads a file to the cloud storage bucket."""

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

    return None

# upload_blob(bucket_name='levant-data-lake_nyc-taxi-dwh', source_file_name='./yellow_tripdata_2022-01-TEST.parquet', destination_blob_name='raw/yellow_tripdata/yellow_taxi_2022_01.parquet')