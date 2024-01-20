import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from pandas_gbq import to_gbq
from google.oauth2 import service_account
from datetime import datetime

# Set the environment variable
credentials = service_account.Credentials.from_service_account_file("credentials.json") # Add a credentials.json file in the repository (past your service acount key)

def read_objects(data, context):
    bucket_name = data['bucket']
    object_name = data['name']
    console_project_id='principal-bird-410107' # Replace with your console project ID
    dataset_id = 'v_mart_dataset' # Replace with your actual bigquery dataset name
    table_id_1 = 'product_master' # Replace with your actual table name
    table_id_2 = 'store_master' # Replace with your actual table name
    staging_bucket = 'v-mart-stagging'  # Replace with your actual staging bucket name
    archive_bucket = 'v-mart-archive' # Replace with your actual archive bucket name

    def table_exists(client, dataset_id, table_id_1, table_id_2):
        try:
            client.get_table(f'{dataset_id}.{table_id_1}')
            client.get_table(f'{dataset_id}.{table_id_2}')
            return True
        except NotFound:
            return False

    bq_client = bigquery.Client()

    # Extracting the file name from the object path
    file_name = object_name.split("/")[-1]

    # Download the content of the blob as bytes
    content = storage.Client().get_bucket(bucket_name).get_blob(object_name).download_as_bytes()

    # Use pd.read_excel without encoding for binary Excel files
    df = pd.read_excel(pd.io.common.BytesIO(content))

    # Determine the destination table based on the file name
    if 'Product_Master' in file_name:       # Make sure your file name                                                       
        destination_table = table_id_1
    elif 'Store_Master' in file_name:       # Make sure your file name
        destination_table = table_id_2
    else:
        print(f"Unknown file name: {file_name}. Skipping...")
        return

    # Check if the table exists
    table_exists_flag = table_exists(bq_client, dataset_id, table_id_1, table_id_2)

    # Write the DataFrame to BigQuery
    to_gbq(df, destination_table=f'{dataset_id}.{destination_table}', project_id=console_project_id, if_exists='append' if table_exists_flag else 'replace')

    print(f"Data loaded into BigQuery table {dataset_id}.{destination_table}")

    # Delete the file from the staging bucket
    storage.Client().get_bucket(staging_bucket).get_blob(object_name).delete()

    print(f"File {object_name} deleted from staging bucket")
    
    # Archive the file to the archive bucket with timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    new_object_name = f"{destination_table.lower()}{timestamp}.xlsx"
    
    storage.Client().get_bucket(archive_bucket).blob(new_object_name).upload_from_string(content)

    print(f"File {object_name} archived to {archive_bucket} with timestamp: {timestamp}")
# Uncomment the line below to call the function locally for testing
# read_objects({'bucket': 'your-bucket-name', 'name': 'your-object-path/Store_Master.xlsx'}, None)
