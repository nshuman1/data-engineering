### Tech Stack

The scope of this project was limited due to the GCP free trial constraints. 

![image](https://raw.githubusercontent.com/nshuman1/data-engineering/main/docs/Infra.png?token=GHSAT0AAAAAABV6YMZ6AZQ3FHSP4ZYKR65AYXJVYGA)

### Pre-requisites
   1. Set environment variables for the following:
      1. WEATHER_API - [Visual Crossing API Key](https://www.visualcrossing.com/weather-api) 
      2. GCP_GCS_BUCKET - The name of your GCP GCS Bucket, can be found via GCP Console
      3. GCP_PROJECT_ID - GCP Project ID, can be found via GCP Console

#### Setup
  [Airflow Setup with Docker, through official guidelines](1_setup_official.md)

 #### Execution
 
  1. Build the image (only first-time, or when there's any change in the `Dockerfile`, takes ~15 mins for the first-time):
     ```shell
     docker-compose build
     ```
   
     then
   
     ```shell
     docker-compose up
     ```


 1. In another terminal, run `docker-compose ps` to see which containers are up & running (there should be 7, matching with the services in your docker-compose file).

 2. Login to Airflow web UI on `localhost:8080` with default creds: `airflow/airflow`

 3. Run your DAG on the Web Console.

 4. When finished or to shut down the container:
    ```shell
    docker-compose down
    ```

    To stop and delete containers, delete volumes with database data, and download images, run:
    ```
    docker-compose down --volumes --rmi all
    ```

    or
    ```
    docker-compose down --volumes --remove-orphans
    ```

### DAG Structure

![image](https://raw.githubusercontent.com/nshuman1/data-engineering/main/docs/DAG_Structure.png?token=GHSAT0AAAAAABV6YMZ6H5QYQWYMXHPO2FX6YXJVXEA)