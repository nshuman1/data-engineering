import os
import urllib.request

def download_files(ti) -> None:
    '''
    Reads a dictionary containing a subject as a key, and a list stored as the value pair containing the following:
        list[0] = ******@@@@@*****  -- TO-DO: populate this description based on whether this function will be for NY taxi only
        list[1] = YYYY-MM
        list[2] = file extension
        list[3] = url to download file
    Utilizes unpacked list values to set a path and filename before downloading the files and saving them to the local machine.

    ti -- Task Instance (required for Airflow XCOM)
    '''

    
    download_info = ti.xcom_pull(key='download_info', task_ids='get_url')
    source_dest_gcs = defaultdict()    

    for k,v in download_info.items():
        taxi_type = v[0]
        year_month = v[1]
        file_ext = v[2]
        url = v[3]

        parent_dir = os.path.abspath(os.path.join(os.path.join(os.path.dirname(__file__), '..'), taxi_type))
        file_name = f'{taxi_type}_{year_month}{file_ext}'
        dest = os.path.join(parent_dir, file_name)
        os.makedirs(parent_dir, exist_ok=True)
            
        urllib.request.urlretrieve(url, dest)
        source_dest_gcs[file_name] = { 'source' : dest, 'dest' : f'{taxi_type}/{taxi_type}_{year_month}{file_ext}' }

    ti.xcom_push(key='gcs_path', value = source_dest_gcs)
    
    return None