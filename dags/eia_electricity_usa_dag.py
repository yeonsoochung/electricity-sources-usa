# Imports
from airflow import DAG
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import time
import requests
import pandas as pd
from google.cloud import storage
import os

# Define DAG
dag = DAG(
    'eia_electricity_usa',
    start_date=datetime(2025, 2, 1),
    end_date=datetime(2026, 12, 31),
    schedule_interval='0 17 1 * *', # 1st day of each month at 5pm UTC
    # default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    catchup=False
)

# Only execute DAG once if multiple scheduled runs are missed
latest_only = LatestOnlyOperator(
    task_id='latest_only',
    dag=dag,
)

# Define API input parameters
api_key = os.getenv("API_KEY")
url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
# states = ["IL", "IN", "IA"]
states = ["IL", "IN", "IA", "KS", "MI", "MN", "MO", "NE", "ND", "OH", "SD", "WI", "IL", "IN", "IA", 
          "CT", "ME", "MA", "NH", "NJ", "NY", "PA", "RI", "VT", "AL", "AR", "FL", "GA", "KY", "LA", 
          "MS", "NC", "OK", "SC", "TN", "TX", "VA", "DC", "MD", "DE", "WV", "AZ", "AK", "CO", "ID", 
          "HI", "MT", "NV", "OR", "NM", "UT", "WA", "WY", "CA"]
params_plants = {
    "frequency": "monthly",
    "data[]": ["consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation"],
    # "facets[state][]": states,
    "start": "2001-01",
    # "start": "2025-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000"
}
csv_name = f"elec_power_plants_usa.csv"

# Define local and GCS file paths
local_csv_path = f"/opt/airflow/data/{csv_name}"
gcs_path = f"csv_exports_test/{csv_name}"  # Path inside GCS bucket
gcs_bucket = "ysc-eia-project-bucket"  # Your GCS bucket name

# def get_json(url, params):
#     try:
#         # Make the request to the EIA API
#         response = requests.get(url, params=params)
#         response.raise_for_status()  # Raise an exception for HTTP errors
#         json_data = response.json()
#     except requests.exceptions.RequestException as e:
#         print("Error fetching data:", e)
#         return None
#     return json_data

def get_json(url, params, max_retries=3, backoff_factor=2): # add retries
    """
    Fetch JSON data from the given URL with retries on failure.
    
    Args:
        url (str): API endpoint
        params (dict): Query parameters
        max_retries (int): Number of retries before giving up
        backoff_factor (int/float): Multiplier for exponential backoff
    
    Returns:
        dict or None: JSON response if successful, otherwise None
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            wait_time = backoff_factor ** attempt
            print(f"Attempt {attempt+1} failed: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
    
    print(f"All {max_retries} attempts failed for params: {params}")
    return None

def create_csv(url, params):
    df_usa = pd.DataFrame()
    for state in states:
        counter = 0
        params["facets[state][]"] = [state]
        params["start"] = "2001-01"  # Reset start date for each state
        # params["start"] = "2025-01"  # Reset start date for each state
        params["offset"] = 0  # Reset pagination offset
        if counter == 0:
            json_data = requests.get(url, params=params).json()
            df_state = pd.DataFrame(json_data["response"]["data"])
            counter += 1
        while len(json_data["response"]["data"]) == 5000:
            params["offset"] += 5000
            json_data = get_json(url=url, params=params)
            df_new = pd.DataFrame(json_data["response"]["data"])
            df_state = pd.concat([df_state, df_new], axis=0, ignore_index=True)
        # Remove duplicate rows
        df_state.drop_duplicates(inplace=True)
        df_usa = pd.concat([df_usa, df_state], axis=0, ignore_index=True)
    # Upload CSV to Docker container
    df_usa.to_csv(local_csv_path, index=False)
    return

# def create_csv(url, params): # fetches smaller chunks of data by setting offset to 0 after each year
#     df_usa = pd.DataFrame()

#     for state in states:
#         df_state = pd.DataFrame()

#         # Loop year by year to keep offsets small
#         for year in range(2001, datetime.now().year + 1):
#             params["facets[state][]"] = [state]
#             params["start"] = f"{year}-01"
#             params["end"] = f"{year}-12"
#             params["offset"] = 0  # reset each year

#             # Fetch first batch for this year
#             json_data = get_json(url, params)
#             df_year = pd.DataFrame(json_data["response"]["data"])

#             # Paginate if needed
#             while len(json_data["response"]["data"]) == 5000:
#                 params["offset"] += 5000
#                 json_data = get_json(url, params)
#                 df_new = pd.DataFrame(json_data["response"]["data"])
#                 df_year = pd.concat([df_year, df_new], axis=0, ignore_index=True)

#             # Add yearly data into state df
#             df_state = pd.concat([df_state, df_year], axis=0, ignore_index=True)

#         # Deduplicate and append to USA df
#         df_state.drop_duplicates(inplace=True)
#         df_usa = pd.concat([df_usa, df_state], axis=0, ignore_index=True)

#     # Write final file
#     df_usa.to_csv(local_csv_path, index=False)
#     return


# Instantiate task to download data from API
download_task = PythonOperator(
    task_id = 'download_data',
    python_callable = create_csv,
    op_kwargs = {
        "url": url_plants,
        "params": params_plants
    },
    dag = dag
)

# Task to upload data to GCS bucket (THIS TASK TIMED OUT; USE TASK CODE BELOW)
# upload_to_gcs_task = LocalFilesystemToGCSOperator(
#     task_id="upload_to_gcs",
#     src=local_csv_path,  # Local file path
#     dst=gcs_path,  # Destination path in GCS bucket
#     bucket=gcs_bucket,
#     mime_type="text/csv",
#     dag=dag
# )

def upload_to_gcs():
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_csv_path, timeout=300)  # 5 min timeout

upload_to_gcs_task = PythonOperator(
    task_id = "upload_to_gcs",
    python_callable = upload_to_gcs,
    dag = dag
)


# Define project ID, BQ dataset name, and BQ table name
gcp_project_id = "eia-project-440601"
bq_dataset = "elec_power_plants"
bq_raw_table = "usa_raw"

# Task to load elec-power-plants data from GCS to BQ (BigQuery)
load_gcs_to_bq_task_1 = GCSToBigQueryOperator(
    task_id="load_gcs_to_bq_elec",
    bucket=gcs_bucket,
    source_objects=gcs_path,  # Path in GCS
    destination_project_dataset_table=f"{gcp_project_id}.{bq_dataset}.{bq_raw_table}",
    source_format="CSV",
    skip_leading_rows=1,
    write_disposition="WRITE_TRUNCATE",  # Overwrites existing data
    autodetect=True,
    dag=dag,
)

# Task to load power plants data from GCS to BQ (BigQuery)
load_gcs_to_bq_task_2 = GCSToBigQueryOperator(
    task_id="load_gcs_to_bq_pp",
    bucket=gcs_bucket,
    source_objects="csv_exports_test/power_plants.csv",  # Path in GCS
    destination_project_dataset_table=f"{gcp_project_id}.{bq_dataset}.power_plants",
    source_format="CSV",
    skip_leading_rows=1,
    write_disposition="WRITE_TRUNCATE",  # Overwrites existing data
    autodetect=True,
    dag=dag,
)

# Read sql files
sql_dates_path = "dags/dates.sql"
with open(sql_dates_path, "r") as dates_file:
    sql_dates_query = dates_file.read()

sql_transformation_1_path = "dags/process_df_usa.sql"
with open(sql_transformation_1_path, "r") as transformation_1_file:
    sql_transformation_1_query = transformation_1_file.read()

sql_transformation_2_path = "dags/update_power_plants.sql"
with open(sql_transformation_2_path, "r") as transformation_2_file:
    sql_transformation_2_query = transformation_2_file.read()

# Task to run SQL query in BQ to process data
run_bq_dates_query = BigQueryInsertJobOperator(
    task_id="run_bq_dates_query",
    configuration={
        "query": {
            "query": sql_dates_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)

run_bq_transformation_1_query = BigQueryInsertJobOperator(
    task_id="run_bq_transformation_1_query",
    configuration={
        "query": {
            "query": sql_transformation_1_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)

run_bq_transformation_2_query = BigQueryInsertJobOperator(
    task_id="run_bq_transformation_2_query",
    configuration={
        "query": {
            "query": sql_transformation_2_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)

# Define Task Dependencies
latest_only >> download_task >> \
    upload_to_gcs_task >> load_gcs_to_bq_task_1 >> load_gcs_to_bq_task_2 >> \
    run_bq_dates_query >> run_bq_transformation_1_query >> run_bq_transformation_2_query